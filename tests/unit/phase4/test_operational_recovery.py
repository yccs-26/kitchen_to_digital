"""Native transitions, dedup and IDs after versioned local snapshot restore."""

from dataclasses import replace

from streaming.state.bindings import FRYER_REFERENCE, HOOD_PROFILE
from streaming.state.contracts import (
    CleaningCompletedSignal, CleaningSignal, FaultSignal, ModeSignal,
    ReportedMode,
)
from streaming.state.fsm import transition
from streaming.state.operational_codec import (
    decode_operational_snapshot, encode_operational_snapshot,
)
from streaming.state.registry import ProfileSnapshot
from test_change_envelope import BINDING, fryer_pair
from test_operational_codec import HOOD_BINDING
from test_hood import BASE, POLICY, apply, event, initial


def test_fryer_all_signal_types_and_repeated_recovery_preserve_native_ids(
    tmp_path,
):
    before, first, sample, _, _ = fryer_pair()
    snapshot = ProfileSnapshot(FRYER_REFERENCE, first.state)
    path = tmp_path / "fryer.json"
    for index, payload in enumerate((
        ModeSignal(ReportedMode.COOKING), CleaningSignal(True),
        CleaningCompletedSignal(), FaultSignal("fixture-fault", True),
    ), 1):
        from datetime import timedelta
        signal = replace(sample, signal_id=f"s{index + 1}", payload=payload,
                         event_time=sample.event_time + timedelta(
                             seconds=index,
                         ))
        path.write_bytes(encode_operational_snapshot(
            snapshot, binding=BINDING,
        ))
        restored = decode_operational_snapshot(
            path.read_bytes(), binding=BINDING,
        )
        direct = transition(snapshot.state, signal,
                            reference_time=signal.event_time)
        recovered = transition(restored.state, signal,
                               reference_time=signal.event_time)
        assert recovered == direct
        assert recovered.intents == direct.intents
        snapshot = ProfileSnapshot(FRYER_REFERENCE, direct.state)
    final = decode_operational_snapshot(encode_operational_snapshot(
        snapshot, binding=BINDING,
    ), binding=BINDING)
    assert final == snapshot
    assert final.state.receipts[-1].payload == FaultSignal(
        "fixture-fault", True,
    )
    assert not transition(final.state, signal,
                          reference_time=signal.event_time).intents
    assert before.receipts == ()


def test_hood_pending_noise_and_dedup_survive_every_step_restore(tmp_path):
    state = initial()
    path = tmp_path / "hood.json"
    for second, rpm in ((0, 0), (1, 0), (2, 0), (3, 800), (4, 800), (6, 800),
                        (7, 300), (8, 0), (9, 0), (10, 0)):
        snapshot = ProfileSnapshot(HOOD_PROFILE, state)
        path.write_bytes(encode_operational_snapshot(snapshot,
                                                     binding=HOOD_BINDING))
        restored = decode_operational_snapshot(
            path.read_bytes(), binding=HOOD_BINDING, hood_policy=POLICY,
        )
        sample = event(second, rpm)
        direct = apply(state, sample)
        assert apply(restored.state, sample) == direct
        assert not apply(direct.state, sample).intents
        if direct.intents:
            assert direct.intents[0].state_change_id == (
                apply(restored.state, sample).intents[0].state_change_id
            )
        state = direct.state
    assert state.current.value == "STOPPED"
    assert state.candidate is None
    old = event(0, 0, event_id="older")
    assert apply(state, old).state == state
    conflict = event(10, 1)
    assert apply(state, conflict).decision.value == "ID_CONFLICT"
