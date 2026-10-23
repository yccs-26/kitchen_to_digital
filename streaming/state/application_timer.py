"""Explicit Health timer commands for LocalStateApplication; no scheduler."""

from streaming.state.change_envelope import canonical, stamp
from streaming.state.health import HealthTick


class LocalHealthTimers:
    def __init__(self, application):
        self.application = application

    def plan(self, equipment_id, *, evaluation_time, profile=None):
        app = self.application
        policy = app._health_policy(equipment_id, profile)
        previous = app.load_health(equipment_id, profile=policy.profile)
        return app.health_registry.plan_timer(
            previous, profile=policy.profile, evaluation_time=evaluation_time,
        )

    def evaluate(self, equipment_id, token, *, command_ref, reference_time,
                 profile=None, hook=lambda stage: None):
        stamp(reference_time)
        app = self.application
        policy = app._health_policy(equipment_id, profile)
        previous = app.load_health(equipment_id, profile=policy.profile)
        axis = (policy.binding.store_id, equipment_id, "HEALTH")
        root = app.store.state(axis)
        item = HealthTick(token)
        recorded = canonical({"input": item,
                              "evaluation_time": reference_time})
        cached = app._cached(axis, command_ref, recorded)
        if cached is not None:
            return cached
        coordinated = app.health_registry.coordinate_timer(
            previous, token, profile=policy.profile,
            evaluation_time=reference_time,
        )
        reduction = coordinated.reduction
        prepared = app._prepare_health(
            previous, reduction, root=root, item=item, recorded=recorded,
            command_ref=command_ref, reference_time=reference_time,
            decision=(reduction.decision.value if reduction is not None
                      else coordinated.status.value),
        )
        hook("after_calculation")
        return app._commit(prepared, hook=hook)
