"""Opt-in local command admission around existing application and timers."""

from streaming.state.application import validated_observation
from streaming.state.application_timer import LocalHealthTimers
from streaming.state.change_envelope import canonical
from streaming.state.writer_fencing import FencedPublicationStore, StaleWriter


class FencedStateApplication:
    """Preserve caller order; no sorting, scheduling, migration or new IDs.

    The whole explicit configuration is recorded in the admission request.
    Retrying a partial command under another policy/profile fails closed.
    Invalid admitted commands remain blocked for review; no catch-and-skip.
    """

    def __init__(self, application):
        if not isinstance(application.store, FencedPublicationStore):
            raise TypeError('FencedPublicationStore required')
        self.application = application
        self.store = application.store
        self.timers = LocalHealthTimers(application)

    def _run(self, token, equipment_id, command_ref, axes, request, operation):
        binding = self.application._binding(equipment_id)
        if token.device != (binding.store_id, equipment_id):
            raise StaleWriter('writer/device identity mismatch')
        recorded = canonical({
            'request': request,
            'binding': binding,
            'health_policies': tuple(
                policy for policy in self.application.health_registry.policies
                if policy.binding.equipment_id == equipment_id
            ),
            'hood_policy': self.application.hood_policies.get(equipment_id),
        })
        return self.store.run_command(token, command_ref, recorded, axes,
                                      operation)

    def process_validated(self, token, event, *, command_ref, reference_time,
                          operational_profile=None, health_profile=None,
                          hook=lambda stage: None):
        sample = validated_observation(event)
        axes = ('OPERATIONAL', 'HEALTH') if sample.equipment_type == (
            'hood'
        ) else ('HEALTH',)
        kwargs = dict(command_ref=command_ref, reference_time=reference_time,
                      operational_profile=operational_profile,
                      health_profile=health_profile)
        return self._run(
            token, sample.equipment_id, command_ref, axes,
            {'kind': 'numeric', 'input': sample, **kwargs},
            lambda: self.application.process_validated(event, hook=hook,
                                                       **kwargs),
        )

    def process_signal(self, token, event, *, equipment_type, command_ref,
                       reference_time, profile=None, hook=lambda stage: None):
        kwargs = dict(equipment_type=equipment_type, command_ref=command_ref,
                      reference_time=reference_time, profile=profile)
        return self._run(
            token, event.equipment_id, command_ref, ('OPERATIONAL',),
            {'kind': 'signal', 'input': event,
             'payload_type': type(event.payload).__name__, **kwargs},
            lambda: self.application.process_signal(event, hook=hook,
                                                    **kwargs),
        )

    def evaluate_timer(self, token, equipment_id, timer, *, command_ref,
                       reference_time, profile=None, hook=lambda stage: None):
        kwargs = dict(command_ref=command_ref, reference_time=reference_time,
                      profile=profile)
        return self._run(
            token, equipment_id, command_ref, ('HEALTH',),
            {'kind': 'tick', 'input': timer, **kwargs},
            lambda: self.timers.evaluate(equipment_id, timer, hook=hook,
                                        **kwargs),
        )
