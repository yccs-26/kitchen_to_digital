# ADR 초안 — Phase 3 Gold 물리 저장 형식

상태: **제안 / 사용자 승인 대기**. 작성일: 2026-10-23.
이 문서는 운영 table 생성·변환·이전·Catalog 변경 승인이 아니다.

## 확인한 현재 상태와 승인 경계

- Phase 1 `raw_ingestion.py`는 CREATE ... USING ICEBERG와 format("iceberg")를 사용한다.
  `docs/reports/phase-1-verification.md`에는 UC Managed Iceberg 실행 증거가 있다.
  현재 cloud의 provider는 이번 작업에서 조회하지 않았다.
- 현재 Phase 2는 Iceberg adapter가 아니라 `DeltaSilverStorage`를 사용한다.
  `sensor_validation_job.py`의 batch adapter도 이 구현을 주입한다.
  `docs/reports/phase-2-verification.md`의 「B 채택」에는 Silver를 UC Managed Delta로
  결정하고 Bronze Iceberg를 유지한다는 기록과 후속 runtime smoke가 있다.
  이 범위의 결정을 Gold 변경 승인으로 확대하지 않는다.
- validated 전달은 동일 Avro 10필드와 Confluent schema ID framing을 유지한다.
  Kafka는 저장 형식과 분리된 경계다. Silver 성공 뒤 재발행 가능성이 있고 payload는 불변이다.
- Gold 논리 문서와 기준 계획은 Iceberg를 기본 플랫폼으로 서술한다.
  최신 Phase 3 설계 §7은 Delta Gold를 **물리 형식 변경 제안/사용자 검토 대상**으로 명시한다.
- 사용자는 2026-10-22에 로컬 Delta Sink 구현·검증을 명시적으로 요청했다.
  이는 로컬 검증 승인이며 운영 UC Gold의 Delta 채택 승인으로 간주하지 않는다.
- 현재 `GoldDeltaSink`는 로컬 absolute path + forPath + 파일 log 검사 방식이다.
  UC managed table의 이름 기반 adapter나 운영 DDL이 구현됐다는 뜻이 아니다.

## 대안 비교

| 기준 | A: UC Managed Iceberg Gold 유지 | B: UC Managed Delta Gold 명시적 채택 |
|---|---|---|
| 계획 일관성 | 기존 Iceberg 목표 유지 | 승인된 Gold 예외/계획 연결 필요 |
| 논리 정확성 | 동일 자연키·불변 final 계약 유지 필요 | 동일 계약, 로컬 Delta 증거 재사용 가능 |
| MERGE/CONFLICT | 실제 managed Iceberg의 지원 API와 충돌 abort 보장부터 확인 | Delta MERGE는 문서상 지원, matched raise_error는 실제 DBR에서 별도 검증 |
| 복구 | 단일 commit·checkpoint retry의 실제 Iceberg 증거 필요 | 로컬 단일 MERGE·SIGKILL 복구를 검증, DBR/UC 복구는 별도 |
| 구현 비용 | 새 Iceberg adapter와 runtime 수용 테스트 필요 | UC 이름 기반 adapter/metadata 검증 및 배포 설정 필요 |
| 기존 코드 영향 | 로컬 Delta adapter는 비교용으로 보존, production sink 새로 필요 | Bronze와 Phase 2 변경 없이 Gold adapter만 UC에 맞춰 확장 가능 |
| 제약 | NOT NULL 등 constraint를 Delta와 동일하게 가정할 수 없음 | 자연키 UNIQUE 없음, 외부 writer 통제 필요 |

[Databricks MERGE reference](https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into)는
해당 명령을 Delta target 전용으로 명시한다. OSS Iceberg의 MERGE 기능이나
managed Iceberg의 batch INSERT 성공을 동일한 지원 증거로 사용하지 않는다.
[managed Iceberg 문서](https://docs.databricks.com/aws/en/iceberg)는
UC·DBR 16.4 LTS 이상·serverless metadata 유지 전제와 constraint 관련 제약을 설명한다.
실제 선택 runtime에서의 지원은 미검증이다.

UC는 두 형식의 managed table을 지원하므로 플랫폼 공존 자체와 개별 write API 호환성은 구별한다.
[UC 요구사항](https://docs.databricks.com/aws/en/data-governance/unity-catalog/requirements)

## 제안과 승인 전 차단 조건

구현·복구 증거 재사용 측면에서 B를 우선 검토할 수 있지만 **채택 결정은 내리지 않는다**.
A 선택 시에도 정확 비교·INSERT/NO-OP/CONFLICT·Append final 계약을 약화하지 않는다.
승인할 항목은 저장 형식, UC managed table 식별자, schema/nullability 정책,
실행 주체·권한, checkpoint 식별자, single writer 소유권, 테스트·비용 한도다.

승인 전에는 새 UC adapter 구현/배포나 기존 table의 provider 변경·데이터 이관을 하지 않는다.
Bronze/Phase 2 target·운영 checkpoint에는 이번 로컬 검증의 쓰기가 없다.
다른 형식 target을 같은 checkpoint에 연결해 성공시키는 방식도 사용하지 않는다.

## E2E 실행 준비 추천안 — 2026-10-23

**권장 후보는 UC Managed Delta Gold다. 상태는 여전히 사용자 승인 대기다.**
로컬 불변 final MERGE·동일 batch 복구·SIGKILL 증거를 재사용할 수 있고,
현재 Silver와 같은 Delta 운영 지식을 활용한다는 점에서 구현·수용 비용이 작다.
이 권고는 실제 UC target의 존재·권한·호환성 확인이나 형식 채택을 뜻하지 않는다.

현재 로컬 API와 필요한 UC API는 다음처럼 다르다.

| 항목 | 현재 로컬 구현 | UC Managed Delta에 필요한 경로 |
|---|---|---|
| 식별 | absolute Path, delta.`path` | catalog.schema.table |
| 접근 | DeltaTable.forPath / format(delta).load | DeltaTable.forName / spark.table |
| DDL | USING DELTA + 로컬 path | 승인된 3-part name, USING DELTA, managed라면 LOCATION 생략 |
| 스키마 검사 | `_delta_log/*.json` 직접 읽기 | 승인된 catalog metadata와 table schema/constraint 확인 |
| checkpoint | 테스트 임시 디렉터리 | 승인된 격리 영속 위치, target 데이터 경로와 분리 |
| 인증·권한 | local JVM/filesystem | UC·Kafka·Registry의 실제 사용자/서비스 권한 |

[공식 Delta tutorial](https://docs.databricks.com/aws/en/delta/tutorial)은 managed table에
fully-qualified name을 쓰도록 설명한다. 로컬 Path를 `/Volumes/...`로 바꾸거나
managed table의 storage location을 직접 읽는 방식으로 adapter를 대체하지 않는다.
[공식 managed table 문서](https://docs.databricks.com/aws/en/tables/managed)

[foreachBatch 문서](https://docs.databricks.com/aws/en/structured-streaming/foreach)는
at-least-once, stateful batch 전량 소비, Standard 환경의 직렬화/모듈 제약을 명시한다.
현재 callback의 primitive capture·같은 final 정확 비교를 유지하되 실제 DBR에서 재검증한다.
`DeltaTable.forName`과 Delta MERGE는 문서화된 API지만 새 UC writer 구현은 아직 차단 상태다.

Managed Iceberg 대안은 Bronze와 형식 일관성이 높지만 별도 writer와 target MERGE/constraint/
abort·checkpoint 수용 검증이 필요하다. Delta용 matched raise_error abort나 NOT NULL 실행
결과를 그대로 Iceberg 증거로 옮기지 않는다. Bronze/Silver의 변환이나 데이터 이관도 필요 없다.

UC에서 재검증할 기존 로컬 시나리오는 12컬럼/type/NOT NULL, 자연키, INSERT/NO-OP/CONFLICT,
source 중복 키 거부, 24행 확정 결과, late/idle, 동일 batch/같은 checkpoint 재시작,
MERGE 후 callback 실패, primitive callback serialization, 실제 state eviction이다.
외부 writer와 권한 거부, Registry/executor 접근, Kafka retention 손실도 추가 수용 대상이다.

사용자가 결정할 항목은 형식, UC target/owner/권한, runtime/build/access mode,
checkpoint·writer 소유권, 입력 범위·최대 실행 시간·비용 및 보존/정리 정책이다.
새 설정의 gold_format_approved=true만으로 구현·수용 게이트를 해제할 수 없다.


## UC 이름 기반 후보 구현 후 상태 — 2026-10-23

사용자가 이번 작업에서 UC 후보의 로컬 구현을 명시적으로 요청했다. 위의
'승인 전 새 UC adapter 구현 금지'는 이전 작업 경계이며 이번 요청으로 로컬 구현 범위만
확장됐다. **저장 형식 상태는 여전히 제안 / 사용자 승인 대기**다.

`GoldUCDeltaSink`는 기존 식별자 검증기를 재사용하고 `DeltaTable.forName`,
`Catalog.getTable`로 접근한다.
CREATE/DROP/GRANT/LOCATION/forPath/직접 log 접근은 없다. 입력 전량 검증 뒤
단일 MERGE로 기존 INSERT/NO-OP/CONFLICT를 적용한다. schema/type와
managed 속성과 Delta provider를 확인한다. NOT NULL DDL 수용은 아래 별도 검사다.
로컬 spark_catalog의 이름 기반 Delta 결과는 UC 권한·managed storage·DBR build의
수용 증거가 아니다. Gold Delta 추천과 실제 운영 채택 결정을 구분한다.

[DESCRIBE JSON](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-describe-table)은
DBR 16.2 이상이며 columns nullable, provider, type을 반환한다. 목표 DBR 17.3 LTS의
실제 schema 출력·권한·MERGE conflict abort·callback 직렬화를 live에서 재검증한다.
미확인 API를 path 기반으로 우회하지 않는다. 실제 테이블 속성이나 계정 설정은 조회하지 않았다.

추가 실험에서 OSS Spark 4 + Delta V2는 DESCRIBE AS JSON에
`NOT_SUPPORTED_COMMAND_FOR_V2_TABLE`을 반환했다. 따라서 이 API를 공통 Writer에
강제하거나 로컬 파일 log 접근으로 우회하지 않는다. DBR 문서상 JSON metadata 지원은
실제 UC에서 검증해야 한다. Writer의 read schema 타입 검사는 NOT NULL DDL 증거가 아니다.
로컬 fixture의 NOT NULL은 실제 Delta NULL append 거부로 별도 검증했다.
