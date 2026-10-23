"""Local validated-event application; no Spark, scheduler or external clients.

Computations are pure, then each axis commits snapshot+intent atomically in the
existing ledger. A two-axis call is resumable, not a cross-axis transaction.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from math import isfinite

from streaming.state.bindings import FRYER_REFERENCE, HOOD_PROFILE
from streaming.state.change_envelope import (
    CurrentProjection, OutputConflict, OutputContext, StateOutput, _sealed,
    canonical, digest, health_output, operational_output, stamp,
)
from streaming.state.contracts import FaultSignal, SyntheticSignal
from streaming.state.health import HealthObservation
from streaming.state.health_codec import (
    decode_health_snapshot, encode_health_snapshot,
)
from streaming.state.hood import HoodRpmEvent
from streaming.state.operational_codec import (
    decode_operational_snapshot, encode_operational_snapshot,
)
from streaming.state.publication_store import PreparedCommand, decode_output
from streaming.state.registry import RegistryError, RegistryErrorCode
from streaming.validation.domain_validator import REQUIRED_FIELDS
from streaming.validation.event_time_normalizer import normalize_event_time


class ApplicationContractError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class AxisResult:
    axis_key: tuple[str, str, str]
    decision: str
    output_id: str | None
    root_revision: int
    replayed: bool = False


def validated_observation(event):
    """Canonical numeric 10-field boundary, not reimplementation of Job 2."""
    if not isinstance(event, Mapping) or set(event) != set(REQUIRED_FIELDS):
        raise ApplicationContractError("VALIDATED_SCHEMA_MISMATCH")
    data = dict(event)
    try:
        if type(data["event_time"]) is str:
            data["event_time"] = normalize_event_time(data["event_time"])
        value = data["metric_value"]
        if type(value) not in (int, float) or not isfinite(value):
            raise ValueError("finite numeric metric required")
        data["metric_value"] = float(value)
        return HealthObservation(**data)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ApplicationContractError("INVALID_VALIDATED_EVENT") from exc


class LocalStateApplication:
    def __init__(self, store, operational_registry, health_registry, *,
                 hood_policies):
        self.store = store
        self.operational_registry = operational_registry
        self.health_registry = health_registry
        # Policies are explicitly supplied; no timeout/source/config discovery.
        self.hood_policies = dict(hood_policies)

    def _binding(self, equipment_id):
        binding = next((item for item in self.operational_registry.bindings
                        if item.equipment_id == equipment_id), None)
        if binding is None:
            raise RegistryError(RegistryErrorCode.UNKNOWN_EQUIPMENT)
        self.operational_registry.resolve(
            store_id=binding.store_id, equipment_id=equipment_id,
            equipment_type=binding.profile.equipment_type,
            profile=binding.profile,
        )
        return binding

    def _health_policy(self, equipment_id, profile=None):
        policy = next((item for item in self.health_registry.policies
                       if item.binding.equipment_id == equipment_id), None)
        if policy is None:
            raise ApplicationContractError("HEALTH_BINDING_NOT_FOUND")
        return self.health_registry.resolve(
            store_id=policy.binding.store_id, equipment_id=equipment_id,
            equipment_type=policy.profile.equipment_type,
            profile=profile if profile is not None else policy.profile,
        )

    def initialize_operational(self, snapshot, *, equipment_id, generation):
        binding = self._binding(equipment_id)
        payload = encode_operational_snapshot(snapshot, binding=binding)
        decode_operational_snapshot(
            payload, binding=binding,
            hood_policy=self.hood_policies.get(equipment_id),
        )
        self.store.initialize((binding.store_id, equipment_id, "OPERATIONAL"),
                              generation, payload)

    def initialize_health(self, snapshot, *, equipment_id, generation):
        policy = self._health_policy(equipment_id)
        self.health_registry.plan_timer(
            snapshot, profile=policy.profile,
            evaluation_time=snapshot.last_evaluated_at,
        )
        self.store.initialize(
            (policy.binding.store_id, equipment_id, "HEALTH"),
            generation, encode_health_snapshot(snapshot),
        )

    def load_operational(self, equipment_id):
        binding = self._binding(equipment_id)
        stored = self.store.state((binding.store_id, equipment_id,
                                   "OPERATIONAL"))
        return decode_operational_snapshot(
            stored.snapshot, binding=binding,
            hood_policy=self.hood_policies.get(equipment_id),
        )

    def load_health(self, equipment_id, *, profile=None):
        policy = self._health_policy(equipment_id, profile)
        stored = self.store.state((policy.binding.store_id, equipment_id,
                                   "HEALTH"))
        return decode_health_snapshot(stored.snapshot, policy=policy)

    def _cached(self, axis, command_ref, recorded):
        try:
            previous = self.store.command_payload(axis, command_ref)
        except KeyError:
            return None
        if canonical(previous["input"]) != recorded:
            raise OutputConflict("command reference/input conflict")
        output = (decode_output(canonical(previous["output"]))
                  if previous["output"] is not None else None)
        output_id = output.identity if output is not None else None
        if output is not None and self.store.output(output_id) != output:
            raise OutputConflict("cached command/output mismatch")
        return AxisResult(
            axis, previous["decision"], output_id,
            previous["expected_revision"] + 1, True,
        )

    def _prepare_health(self, previous, reduction, *, root, item, recorded,
                        command_ref, reference_time, decision=None):
        state = reduction.snapshot if reduction is not None else previous
        output = None
        if state != previous:
            context = OutputContext(
                command_ref, root.generation, state.health_revision,
                state.snapshot_revision, previous.last_health_change_id,
            )
            output = health_output(
                previous, reduction, item, context=context,
                reference_time=reference_time,
            )
        return PreparedCommand(
            root.axis_key, command_ref, root.generation, root.revision,
            digest(root.snapshot), encode_health_snapshot(state), recorded,
            decision if decision is not None else reduction.decision.value,
            output,
        )

    def _prepare_operational(self, previous, native, event, *, binding, root,
                             command_ref, recorded, reference_time):
        payload = encode_operational_snapshot(native.snapshot, binding=binding)
        result = native.fsm
        output = operational_output(
            previous.state, result, event, binding=binding,
            context=OutputContext(
                command_ref, root.generation,
                root.change_revision + int(bool(result.intents)),
                root.projection_revision + 1, root.last_change_id,
            ), reference_time=reference_time,
        )
        if output is not None:
            # Existing envelope builder uses the old opaque snapshot image.
            # Bind the Current image digest to versioned Codec bytes before
            # commit. Preserve the native change ID and payload.
            data = output.projection.data
            data.pop("content_sha256")
            data["current"]["snapshot_sha256"] = digest(payload)
            output = StateOutput(
                output.change, CurrentProjection(_sealed(data)),
            )
        return PreparedCommand(
            root.axis_key, command_ref, root.generation, root.revision,
            digest(root.snapshot), payload, recorded, result.decision.value,
            output,
        )

    def _commit(self, prepared, *, hook):
        if isinstance(prepared, AxisResult):
            return prepared
        axis = prepared.axis_key[2].lower()
        hook("before_" + axis + "_commit")
        output_id = self.store.commit(prepared)
        hook("after_" + axis + "_commit")
        return AxisResult(
            prepared.axis_key, prepared.decision, output_id,
            prepared.expected_revision + 1,
        )

    def process_validated(self, event, *, command_ref, reference_time,
                          operational_profile=None, health_profile=None,
                          hook=lambda stage: None):
        stamp(reference_time)
        sample = validated_observation(event)
        policy = self._health_policy(sample.equipment_id, health_profile)
        self.health_registry.resolve(
            store_id=sample.store_id, equipment_id=sample.equipment_id,
            equipment_type=sample.equipment_type, profile=policy.profile,
        )
        # Select Health first, validate admission before either axis is stored.
        health_axis = (sample.store_id, sample.equipment_id, "HEALTH")
        health_root = self.store.state(health_axis)
        previous_health = self.load_health(sample.equipment_id,
                                          profile=policy.profile)
        recorded = canonical({"input": sample,
                              "evaluation_time": reference_time})
        cached = self._cached(health_axis, command_ref, recorded)
        if cached is None:
            reduction = self.health_registry.evaluate(
                previous_health, sample, profile=policy.profile,
                reference_time=reference_time,
            )
            prepared_health = self._prepare_health(
                previous_health, reduction, root=health_root, item=sample,
                recorded=recorded, command_ref=command_ref,
                reference_time=reference_time,
            )
        else:
            prepared_health = cached
        prepared_op = None
        if sample.equipment_type == "hood" or operational_profile is not None:
            binding = self._binding(sample.equipment_id)
            self.operational_registry.resolve(
                store_id=sample.store_id, equipment_id=sample.equipment_id,
                equipment_type=sample.equipment_type,
                profile=(operational_profile if operational_profile is not None
                         else binding.profile),
            )
        # Fryer numeric input explicitly has no Operational invocation.
        if sample.equipment_type == "hood":
            axis = (sample.store_id, sample.equipment_id, "OPERATIONAL")
            root = self.store.state(axis)
            previous = self.load_operational(sample.equipment_id)
            cached_op = self._cached(axis, command_ref, recorded)
            if cached_op is None:
                rpm = HoodRpmEvent(**{
                    name: getattr(sample, name) for name in REQUIRED_FIELDS
                })
                native = self.operational_registry.evaluate(
                    previous, rpm, equipment_type="hood",
                    reference_time=reference_time,
                )
                prepared_op = self._prepare_operational(
                    previous, native, rpm, binding=binding, root=root,
                    command_ref=command_ref, recorded=recorded,
                    reference_time=reference_time,
                )
            else:
                prepared_op = cached_op
        hook("after_calculation")
        pending = (prepared_health,) if prepared_op is None else (
            prepared_op, prepared_health,
        )
        return tuple(self._commit(item, hook=hook) for item in pending)

    def process_signal(self, event, *, equipment_type, command_ref,
                       reference_time, profile=None, hook=lambda stage: None):
        stamp(reference_time)
        if not isinstance(event, SyntheticSignal) or isinstance(
            event.payload, FaultSignal,
        ):
            raise ApplicationContractError("UNSUPPORTED_SIGNAL")
        binding = self._binding(event.equipment_id)
        self.operational_registry.resolve(
            store_id=event.store_id, equipment_id=event.equipment_id,
            equipment_type=equipment_type,
            profile=profile if profile is not None else binding.profile,
        )
        if binding.profile != FRYER_REFERENCE:
            raise ApplicationContractError("UNSUPPORTED_SIGNAL_PROFILE")
        axis = (event.store_id, event.equipment_id, "OPERATIONAL")
        root = self.store.state(axis)
        previous = self.load_operational(event.equipment_id)
        recorded = canonical({"input": {
            "signal": event, "payload_type": type(event.payload).__name__,
        }, "evaluation_time": reference_time})
        cached = self._cached(axis, command_ref, recorded)
        if cached:
            return cached
        native = self.operational_registry.evaluate(
            previous, event, equipment_type=equipment_type,
            reference_time=reference_time,
        )
        prepared = self._prepare_operational(
            previous, native, event, binding=binding, root=root,
            command_ref=command_ref, recorded=recorded,
            reference_time=reference_time,
        )
        hook("after_calculation")
        return self._commit(prepared, hook=hook)

    def reconcile(self, coordinator):
        if coordinator.store is not self.store:
            raise ApplicationContractError("COORDINATOR_STORE_MISMATCH")
        coordinator.reconcile()
