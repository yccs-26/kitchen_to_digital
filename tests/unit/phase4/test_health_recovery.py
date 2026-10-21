"""실제 임시 파일의 codec bytes를 재적재하는 로컬 복원 검증."""

from dataclasses import asdict, replace

import pytest

from streaming.state.health import HealthDecision, HealthTick, timer_token
from streaming.state.health_codec import (
    HealthCodecError, decode_health_snapshot, encode_health_snapshot,
)
from test_health import BASE, EPS, POLICY, TIMEOUT, boot, evaluate, observation


def save_reload(snapshot, path):
    path.write_bytes(encode_health_snapshot(snapshot))
    restored = decode_health_snapshot(path.read_bytes(), policy=POLICY)
    assert restored is not snapshot
    assert asdict(restored) == asdict(snapshot)
    return restored


def test_continuous_and_file_restored_execution_match_every_step(tmp_path):
    first = evaluate(boot(), observation()).snapshot
    first_tick = HealthTick(timer_token(first))
    second_event = observation(BASE + EPS, "e2")
    second = evaluate(first, second_event, BASE + EPS).snapshot
    second_tick = HealthTick(timer_token(second))
    sequence = [
        (observation(), BASE + EPS, HealthDecision.DUPLICATE),
        (replace(observation(), metric_value=801), BASE + EPS,
         HealthDecision.ID_CONFLICT),
        (second_event, BASE + EPS, HealthDecision.NO_CHANGE),
        (first_tick, BASE + TIMEOUT, HealthDecision.OBSOLETE_TIMER),
        (second_tick, BASE + TIMEOUT + EPS, HealthDecision.TRANSITION),
        (second_tick, BASE + TIMEOUT + EPS, HealthDecision.NO_CHANGE),
        (observation(BASE, "old"), BASE + TIMEOUT + EPS,
         HealthDecision.OUT_OF_ORDER),
        (observation(BASE + TIMEOUT + 2 * EPS, "recovered"),
         BASE + TIMEOUT + 2 * EPS, HealthDecision.TRANSITION),
    ]
    continuous = first
    restored = save_reload(first, tmp_path / "health.json")
    changes = []
    for item, at, decision in sequence:
        direct = evaluate(continuous, item, at)
        recovered = evaluate(restored, item, at)
        assert recovered == direct
        assert recovered.decision is decision
        assert [c.health_change_id for c in recovered.changes] == [
            c.health_change_id for c in direct.changes
        ]
        changes.extend(recovered.changes)
        continuous = direct.snapshot
        restored = save_reload(recovered.snapshot, tmp_path / "health.json")
    assert len(changes) == 2
    assert changes[1].previous_health_change_id == changes[0].health_change_id
    assert len(restored.receipts) == 3


@pytest.mark.parametrize("offset", [-EPS, EPS * 0, EPS])
def test_restored_deadline_boundary_and_same_change_id(tmp_path, offset):
    state = evaluate(boot(), observation()).snapshot
    restored = save_reload(state, tmp_path / "boundary.json")
    at = BASE + TIMEOUT + offset
    tick = HealthTick(timer_token(state))
    assert evaluate(restored, tick, at) == evaluate(state, tick, at)


@pytest.mark.parametrize("observation_first", [True, False])
def test_file_restore_keeps_race_order(tmp_path, observation_first):
    state = evaluate(boot(), observation()).snapshot
    tick = HealthTick(timer_token(state))
    at = BASE + TIMEOUT
    event = observation(at, "new-generation")
    commands = (event, tick) if observation_first else (tick, event)
    direct, restored = state, save_reload(state, tmp_path / "race.json")
    changes = []
    for item in commands:
        left = evaluate(direct, item, at)
        right = evaluate(restored, item, at)
        assert left == right
        changes.extend(right.changes)
        direct = left.snapshot
        restored = save_reload(right.snapshot, tmp_path / "race.json")
    assert len(changes) == (0 if observation_first else 2)


@pytest.mark.parametrize(
    "damage", ["truncated", "changed-digit", "invalid-utf8"],
)
def test_damaged_local_file_refuses_restore(tmp_path, damage):
    path = tmp_path / "damaged.json"
    state = evaluate(boot(), observation()).snapshot
    payload = encode_health_snapshot(state)
    if damage == "truncated":
        payload = payload[:len(payload) // 2]
    elif damage == "changed-digit":
        assert b'"metric_value":800' in payload
        payload = payload.replace(b'"metric_value":800', b'"metric_value":801')
    else:
        payload += b"\xff"
    path.write_bytes(payload)
    with pytest.raises(HealthCodecError):
        decode_health_snapshot(path.read_bytes(), policy=POLICY)
    assert state.receipts[0].metric_value == 800
