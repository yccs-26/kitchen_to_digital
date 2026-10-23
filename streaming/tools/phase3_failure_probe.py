"""격리된 테스트 query의 MERGE 경계 실패와 bounded 실행 수명을 제어한다."""

from dataclasses import dataclass


@dataclass(frozen=True)
class FailureProbe:
    """재시작에서 probe를 제거하면 동일 batch의 final 값이 다시 대조된다."""

    job_tag: str
    checkpoint: str
    stage: str
    batch_id: int

    def wrap(self, consume, settings, approval):
        approval.require(settings, "failure_probe")
        if (self.job_tag != approval.job_tag
                or self.checkpoint != settings.checkpoint
                or self.stage not in ("before_merge", "after_merge")
                or type(self.batch_id) is not int or self.batch_id < 0):
            raise ValueError("Failure probe requires approved test query")
        stage, selected = self.stage, self.batch_id

        def injected(batch, batch_id):
            if batch_id == selected and stage == "before_merge":
                # 실패 batch를 성공 반환하지 않는다. state/checkpoint는 Spark 책임.
                raise RuntimeError("PHASE3_TEST_BEFORE_MERGE")
            consume(batch, batch_id)
            if batch_id == selected and stage == "after_merge":
                raise RuntimeError("PHASE3_TEST_AFTER_MERGE")

        return injected


def await_bounded_query(query, timeout_seconds: int) -> bool:
    """정상 timeout stop과 query 실패를 구분한다. compute 종료 보장은 아니다."""
    if type(timeout_seconds) is not int or timeout_seconds <= 0:
        raise ValueError("Positive query timeout required")
    try:
        completed = query.awaitTermination(timeout_seconds)
        if not completed:
            query.stop()
        error = query.exception()
        if error is not None:
            raise error
        return bool(completed)
    except BaseException:
        # 원래 예외를 보존한다. stop 실패로 query 실패를 바꾸지 않는다.
        try:
            query.stop()
        except Exception:
            pass
        raise
