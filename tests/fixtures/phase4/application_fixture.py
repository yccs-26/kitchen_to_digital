"""Explicit local fixture policies and resources, never operating defaults."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from streaming.state.application import LocalStateApplication
from streaming.state.bindings import (
    EquipmentBinding, FRYER_REFERENCE, HOOD_PROFILE, InputKind, ProfileKey,
)
from streaming.state.change_wire import decode_change, encode_change
from streaming.state.contracts import (
    HealthStatus, ModeSignal, OperationalState, ReportedMode, StateObservation,
    SyntheticSignal,
)
from streaming.state.fsm import TwinState
from streaming.state.health import HealthPolicy, initialize_health
from streaming.state.health_registry import EquipmentHealthRegistry
from streaming.state.hood import HoodPolicy, HoodSnapshot, HoodState
from streaming.state.local_sinks import LocalCurrent, LocalHistory, LocalKafka
from streaming.state.publication import PublicationCoordinator
from streaming.state.publication_store import PublicationStore
from streaming.state.registry import ProfileSnapshot, build_registry


BASE = datetime(2026, 10, 23, tzinfo=timezone.utc)
TIMEOUT = timedelta(seconds=10)
HOOD_POLICY = HoodPolicy(600, 200, timedelta(seconds=3), timedelta(seconds=2),
                         timedelta(seconds=2))
FRYER = EquipmentBinding("store-001", "fryer-001", FRYER_REFERENCE,
                         InputKind.SYNTHETIC_SIGNAL)
HOOD = EquipmentBinding("store-001", "hood-001", HOOD_PROFILE,
                        InputKind.NUMERIC_TELEMETRY)


def health_policy(binding):
    hood = binding == HOOD
    return HealthPolicy(
        EquipmentBinding(
            binding.store_id, binding.equipment_id, binding.profile,
            InputKind.NUMERIC_TELEMETRY,
        ),
        "fixture-binding-v1", "fixture-numeric-v1",
        ProfileKey(binding.profile.equipment_type, "fixture-freshness", "1"),
        TIMEOUT, "fan_rpm" if hood else "oil_temperature_celsius",
        "rpm" if hood else "celsius", ("1.0.0",), ("fixture",),
    )


class WireCheckingKafka(LocalKafka):
    """Local wire round-trip before the canonical Fake append, not a broker."""

    def publish(self, change):
        frame = encode_change(change, schema_id=17)
        if decode_change(frame.key, frame.value, schema_id=17) != change:
            raise ValueError("local Avro round-trip mismatch")
        return super().publish(change)


@dataclass
class Bundle:
    app: LocalStateApplication
    coordinator: PublicationCoordinator
    store: PublicationStore
    history: LocalHistory
    kafka: LocalKafka
    current: LocalCurrent

    def close(self):
        for resource in (self.store, self.history, self.kafka, self.current):
            resource.close()


def bundle(path, *, initialize=False, policies=None, hood_policy=HOOD_POLICY):
    policies = policies or (health_policy(FRYER), health_policy(HOOD))
    store = PublicationStore(path / "application.sqlite")
    app = LocalStateApplication(
        store, build_registry((FRYER, HOOD)),
        EquipmentHealthRegistry(policies),
        hood_policies={"hood-001": hood_policy},
    )
    history = LocalHistory(path / "history.sqlite")
    kafka = WireCheckingKafka(path / "kafka.sqlite")
    current = LocalCurrent(path / "current.sqlite")
    result = Bundle(
        app, PublicationCoordinator(store, history, kafka, current),
        store, history, kafka, current,
    )
    if initialize:
        app.initialize_operational(ProfileSnapshot(
            FRYER_REFERENCE, TwinState("store-001", "fryer-001",
                                      StateObservation(OperationalState.IDLE,
                                                       HealthStatus.STALE)),
        ), equipment_id="fryer-001", generation=1)
        app.initialize_operational(ProfileSnapshot(
            HOOD_PROFILE, HoodSnapshot("store-001", "hood-001", hood_policy,
                                       current=HoodState.STOPPED),
        ), equipment_id="hood-001", generation=1)
        for policy in policies:
            app.initialize_health(initialize_health(
                policy, monitoring_epoch="fixture-epoch",
                monitoring_started_at=BASE,
            ), equipment_id=policy.binding.equipment_id, generation=1)
    return result


def numeric(second=0, value=800, equipment_id="hood-001", event_id=None):
    hood = equipment_id == "hood-001"
    return {
        "event_id": event_id or f"{equipment_id}-{second}",
        "event_time": BASE + timedelta(seconds=second),
        "store_id": "store-001", "equipment_id": equipment_id,
        "equipment_type": "hood" if hood else "fryer",
        "metric_name": "fan_rpm" if hood else "oil_temperature_celsius",
        "metric_value": value, "unit": "rpm" if hood else "celsius",
        "schema_version": "1.0.0", "source": "fixture",
    }


def signal(second=0, payload=None):
    return SyntheticSignal(
        f"signal-{second}", BASE + timedelta(seconds=second), "store-001",
        "fryer-001", "fixture-controller",
        (payload if payload is not None
         else ModeSignal(ReportedMode.PREHEATING)),
        "synthetic_test_only",
    )
