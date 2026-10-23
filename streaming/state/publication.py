"""Synchronous retry/reconcile over durable intents and injected sinks."""

from dataclasses import asdict

from streaming.state.change_envelope import (
    CurrentProjection, OutputConflict, revision, text,
)
from streaming.state.publication_store import SINKS
from streaming.state.sink_protocols import (
    CurrentAck, CurrentSink, HistoryAck, HistorySink, KafkaAck, KafkaSink,
    RetryableSinkError,
)


class PublicationCoordinator:
    """No clock, hidden retry loop, scheduler or external service dependency.

    begin_attempt is durable before the call. Death after a side effect leaves
    UNKNOWN, so restart may repeat it. All completed sink stages are retained.
    """

    def __init__(self, store, history: HistorySink, kafka: KafkaSink,
                 current: CurrentSink):
        self.store = store
        self.history = history
        self.kafka = kafka
        self.current = current

    def _call(self, sink, output):
        change = output.change
        if sink == "history":
            ack = self.history.write(change)
            if not isinstance(ack, HistoryAck) or (
                ack.identity != change.identity
                or ack.payload_sha256 != change.sha256
            ):
                raise RetryableSinkError("unconfirmed History ACK")
            text(ack.reference)
            return "ACKED", asdict(ack)
        if sink == "kafka":
            ack = self.kafka.publish(change)
            if not isinstance(ack, KafkaAck) or (
                ack.callback_received is not True or ack.error is not None
                or ack.topic != "kitchen.state.changes"
                or ack.key != change.data["equipment"]["equipment_id"].encode()
                or ack.payload_sha256 != change.sha256
            ):
                raise RetryableSinkError("unconfirmed Kafka ACK")
            try:
                revision(ack.partition)
                revision(ack.offset)
            except ValueError as exc:
                raise RetryableSinkError("invalid Kafka ACK offset") from exc
            receipt = asdict(ack)
            receipt["key"] = ack.key.decode("utf-8")
            return "ACKED", receipt
        projection = output.projection
        ack = self.current.apply(projection)
        if not isinstance(ack, CurrentAck) or ack.status not in (
            "ACKED", "ALREADY_APPLIED", "SUPERSEDED",
        ):
            raise RetryableSinkError("unconfirmed Current ACK")
        data = projection.data
        revision(ack.generation, 1)
        revision(ack.revision, 1)
        if ack.generation != data["generation"] or (
            ack.status == "SUPERSEDED" and ack.revision <= (
                data["projection_revision"]
            )
        ) or (
            ack.status != "SUPERSEDED" and (
                ack.revision != data["projection_revision"]
                or ack.projection_id != projection.identity
                or ack.payload_sha256 != projection.sha256
            )
        ):
            raise RetryableSinkError("invalid Current revision receipt")
        accepted = CurrentProjection(ack.canonical_payload)
        evidence = accepted.data
        if accepted.identity != ack.projection_id or (
            accepted.sha256 != ack.payload_sha256
            or evidence["generation"] != ack.generation
            or evidence["projection_revision"] != ack.revision
            or any(evidence[name] != data[name] for name in (
                "equipment", "profile", "policy", "generation",
            ))
        ):
            raise RetryableSinkError("Current receipt context mismatch")
        if ack.status == "SUPERSEDED" and not (
            self.store.contains_projection(accepted)
        ):
            raise RetryableSinkError(
                "superseding intent is not in this ledger",
            )
        receipt = asdict(ack)
        receipt["canonical_payload"] = evidence
        return ack.status, receipt

    def dispatch(self, output_id, *, hook=lambda stage: None, order=SINKS):
        """Order override is only for explicit local fault simulation."""
        if type(order) is not tuple or set(order) != set(SINKS) or len(
            order
        ) != len(SINKS):
            raise ValueError("each sink must appear exactly once")
        output = self.store.output(output_id)
        for sink in order:
            attempt = self.store.begin_attempt(output_id, sink)
            if not attempt:
                continue
            try:
                hook("before_" + sink)
                status, receipt = self._call(sink, output)
                hook("after_" + sink)
            except OutputConflict as exc:
                self.store.finish_attempt(
                    output_id, sink, "CONFLICT",
                    error=f"{type(exc).__name__}: {exc}",
                    attempt=attempt,
                )
                raise
            except Exception as exc:
                self.store.finish_attempt(
                    output_id, sink, "FAILED", error=(
                        f"{type(exc).__name__}: {exc}" if isinstance(
                            exc, RetryableSinkError,
                        ) else type(exc).__name__
                    ),
                    attempt=attempt,
                )
                raise
            # BaseException/process death deliberately leaves durable UNKNOWN.
            self.store.finish_attempt(
                output_id, sink, status, receipt=receipt, attempt=attempt,
            )
        hook("before_complete")
        self.store.complete(output_id)

    def reconcile(self):
        """One bounded pass; errors propagate and stop this pass."""
        for output_id in self.store.pending():
            self.dispatch(output_id)
