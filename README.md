# Kitchen to Digital (KTD)

> 주방 환경의 센서 이벤트를 실시간으로 수집하고, 이상 상태를 감지·처리하는 데이터 플랫폼 프로젝트  
> **Status: In Progress**

Docker Compose와 Kafka 기반으로 주방 센서의 정상·온도 이상 이벤트를 실시간 수집하는 데이터 플랫폼을 구축하고 있으며, 현재 데이터 품질 검증·저장·오케스트레이션 계층으로 확장 중입니다.

## 1. 프로젝트 소개

Kitchen to Digital(KTD)은 주방의 온도·상태와 같은 현장 센서 데이터를 실시간으로 수집하고, 향후 이상 탐지·저장·모니터링까지 확장하기 위해 설계한 데이터 플랫폼 프로젝트입니다.

주방은 식재료 보관, 조리 환경, 장비 운영 과정에서 온도와 같은 물리적 상태값을 지속적으로 관리해야 하는 환경입니다. 센서 이벤트가 누락되거나 이상 상태가 제때 감지되지 않으면 운영 품질과 안전에 영향을 줄 수 있습니다.

본 프로젝트에서는 이러한 환경을 가정하여 Python 기반 센서 시뮬레이터가 정상 상태와 온도 이상 상태 이벤트를 생성하고, Apache Kafka가 이를 실시간 스트림으로 수집하는 기반을 구현했습니다.

현재는 **이벤트 생성 및 Kafka 수집 계층**까지 구현되어 있으며, 이후 Consumer, 데이터 품질 검증, 저장소 적재, 모니터링 및 오케스트레이션 계층을 단계적으로 확장할 예정입니다.

## 2. 문제 정의

현장 센서 기반 시스템에서는 다음과 같은 문제가 발생할 수 있습니다.

- 센서 데이터가 지속적으로 발생해 이벤트를 안정적으로 수집할 수 있어야 한다.
- 정상 데이터와 이상 데이터를 구분할 수 있는 기준이 필요하다.
- 이상 이벤트가 발생했을 때 후속 시스템에서 추적·대응할 수 있어야 한다.
- 개발 환경이 사람마다 달라지지 않도록 재현 가능한 로컬 환경이 필요하다.
- 기술 선택과 구조 변경의 이유를 기록해 프로젝트의 유지보수성을 높여야 한다.

KTD는 이러한 문제를 해결하기 위해 데이터 수집 단계부터 이벤트 기반 아키텍처와 문서화된 설계 원칙을 적용하는 것을 목표로 합니다.

## 3. 현재 구현 범위

### 구현 완료

- Docker Compose 기반 로컬 데이터 플랫폼 환경 구성
- Apache Kafka 단일 노드 KRaft 모드 구성
- Python 기반 주방 센서 시뮬레이터 구현
- 정상 상태 이벤트 생성
- 온도 이상 상태(`temperature_breach`) 이벤트 생성
- Kafka Producer를 통한 센서 이벤트 발행
- `kitchen.sensor.raw` Kafka 토픽으로 이벤트 수집
- Kafka Console Consumer를 통한 이벤트 발행 결과 검증
- `uv` Workspace 기반 Python 의존성 관리
- `.env.example` 기반 환경변수 분리
- ADR(Architecture Decision Record) 기반 설계 의사결정 문서화

### 진행 중 / 확장 예정

- Kafka Consumer 구현 및 이벤트 소비
- 센서 이벤트 스키마 검증 및 데이터 품질 규칙 적용
- 정상 이벤트와 이상 이벤트의 분기 처리
- Raw / Processed 데이터 저장 계층 구성
- PostgreSQL 또는 S3 호환 Object Storage 연동
- 데이터 품질 검증 실패 이벤트 격리(Quarantine) 구조 구현
- Airflow 기반 배치 작업 및 재처리 오케스트레이션
- 이벤트 처리 지연·실패·처리량 모니터링
- 이상 이벤트 알림 및 운영 대시보드 구성

> 아직 구현하지 않은 기능은 포트폴리오 및 이력서에서 “구현 완료”가 아닌 “설계 중” 또는 “확장 예정”으로 표기합니다.

## 4. 아키텍처

```text
┌───────────────────────────────┐
│  Kitchen Sensor Simulator     │
│  - Normal mode                │
│  - Temperature breach mode    │
│  - Python                     │
└───────────────┬───────────────┘
                │ Sensor event
                ▼
┌───────────────────────────────┐
│          Apache Kafka         │
│      KRaft / Docker Compose   │
│                               │
│   Topic: kitchen.sensor.raw   │
└───────────────┬───────────────┘
                │
                │ Currently implemented:
                │ event publishing and verification
                ▼
┌───────────────────────────────┐
│      Consumer / Validation    │
│       (Planned / In progress) │
└───────────────┬───────────────┘
                │
                ├───────────────┐
                ▼               ▼
┌───────────────────┐  ┌───────────────────┐
│ Normal Data Store │  │ Quarantine Store  │
│ (Planned)         │  │ (Planned)         │
└───────────────────┘  └───────────────────┘
```

## 5. 데이터 흐름

1. Python 센서 시뮬레이터가 주방 센서 이벤트를 생성합니다.
2. 시뮬레이터는 정상 모드 또는 온도 이상 모드로 실행됩니다.
3. Kafka Producer가 이벤트를 `kitchen.sensor.raw` 토픽으로 발행합니다.
4. Kafka Console Consumer로 토픽에 이벤트가 정상적으로 수집됐는지 검증합니다.
5. 다음 단계에서는 Consumer가 이벤트를 읽어 스키마와 값 범위를 검증합니다.
6. 유효한 이벤트는 저장·분석 계층으로 전달하고, 품질 기준을 충족하지 못한 이벤트는 Quarantine 영역으로 분리할 계획입니다.

## 6. 기술 스택

| 구분 | 기술 | 활용 목적 |
|---|---|---|
| Language | Python | 센서 이벤트 생성 및 Kafka Producer 구현 |
| Messaging | Apache Kafka | 센서 이벤트의 실시간 스트리밍 수집 |
| Kafka Mode | KRaft | ZooKeeper 없이 Kafka 메타데이터 관리 |
| Infrastructure | Docker Compose | 재현 가능한 로컬 개발 환경 구성 |
| Dependency Management | uv | Python Workspace 및 의존성 관리 |
| Configuration | Environment Variables, `.env` | 실행 환경별 설정·민감 정보 분리 |
| Documentation | ADR | 기술 선택과 아키텍처 의사결정 기록 |
| Planned | PostgreSQL / Object Storage | Raw·Processed 데이터 저장 계층 |
| Planned | Airflow | 배치 처리·재시도·백필 오케스트레이션 |

## 7. 실행 방법

### 사전 준비

- Docker 및 Docker Compose
- Python 3.x
- `uv`

### 1) Kafka 실행

```bash
cd infra/docker
docker compose up -d
```

Kafka 컨테이너가 정상 실행됐는지 확인합니다.

```bash
docker ps
```

### 2) 센서 시뮬레이터 환경 설정

```bash
cd apps/sensor-simulator

uv sync
cp .env.example .env
```

`.env` 파일에서 Kafka 접속 정보를 현재 환경에 맞게 확인합니다.

### 3) 정상 센서 이벤트 발행

```bash
uv run --package kitchen-sensor-simulator \
  python -m kitchen_simulator.main
```

### 4) 온도 이상 이벤트 발행

```bash
SIMULATION_MODE=temperature_breach \
uv run --package kitchen-sensor-simulator \
  python -m kitchen_simulator.main
```

### 5) Kafka 이벤트 검증

```bash
docker exec -it ktd-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic kitchen.sensor.raw \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" | "
```

정상 모드와 `temperature_breach` 모드의 이벤트가 `kitchen.sensor.raw` 토픽에 수집되는지 확인합니다.

## 8. 설계 의사결정

프로젝트에서 주요 기술 선택과 구조적 판단은 ADR로 관리합니다.

- [ADR-001: Local-first Architecture](docs/adr/01-local-first-architecture.md)
- [ADR-002: Adopt Kafka over Redpanda](docs/adr/02-adopt-kafka-over-redpanda.md)

### Local-first를 선택한 이유

초기 단계에서는 클라우드 비용과 외부 의존성을 줄이고, 로컬 환경에서 이벤트 생성·수집·검증 흐름을 반복적으로 실험할 수 있도록 Local-first 방식을 선택했습니다. 이후 데이터량, 운영 요구사항, 협업 환경이 확장되면 클라우드 인프라로 전환 가능한 구조를 검토합니다.

### Kafka를 선택한 이유

센서 데이터는 연속적으로 발생하며, 생산자와 소비자 계층을 분리해야 확장성과 장애 격리가 쉬워집니다. Kafka는 이벤트를 토픽 단위로 보존하고 Consumer가 독립적으로 읽을 수 있어, 향후 이상 탐지, 저장, 분석, 알림과 같은 다수의 후속 처리 계층을 연결하기에 적합하다고 판단했습니다.

## 9. 핵심 학습 포인트

- 실시간 데이터 플랫폼은 Producer를 구현하는 데서 끝나지 않으며, 이벤트 스키마·품질 검증·재처리·관측성까지 포함해야 한다.
- 정상 이벤트뿐 아니라 온도 이상과 같은 예외 상황을 먼저 재현해야 이후의 처리·알림 정책을 현실적으로 설계할 수 있다.
- Docker Compose 기반 환경은 개발·테스트 환경의 차이를 줄이고 재현 가능한 실험을 가능하게 한다.
- 기술 선택의 이유를 ADR로 기록하면 프로젝트가 확장될 때 구조적 판단을 추적하고 협업하기 쉬워진다.
- 실시간 시스템에서도 데이터의 정확성, 추적성, 장애 격리가 중요한 품질 기준이 된다.

## 10. 향후 로드맵

### Phase 1. 실시간 수집 기반 — 완료

- [x] Docker Compose 기반 Kafka 환경 구성
- [x] KRaft 기반 단일 노드 Kafka 구성
- [x] Python 센서 시뮬레이터 구현
- [x] 정상·온도 이상 이벤트 발행
- [x] Kafka 토픽 수집 검증

### Phase 2. 이벤트 소비 및 데이터 품질

- [ ] Kafka Consumer 구현
- [ ] 센서 이벤트 스키마 검증
- [ ] 필수값·자료형·허용 범위 검증
- [ ] 정상·이상 이벤트 분기
- [ ] 검증 실패 이벤트 Quarantine 처리

### Phase 3. 저장 및 배치 처리

- [ ] PostgreSQL 또는 S3 호환 Object Storage 연동
- [ ] Raw / Processed 데이터 계층화
- [ ] Airflow 기반 배치 적재
- [ ] 재시도·백필·멱등성 처리

### Phase 4. 관측성과 운영

- [ ] Consumer Lag 모니터링
- [ ] 처리 실패·지연 이벤트 로깅
- [ ] 이상 이벤트 알림
- [ ] 데이터 품질 지표 및 운영 대시보드 구성

## 11. 프로젝트를 통한 목표

KTD는 단순히 Kafka를 사용해 보는 실습이 아니라, 현장 센서 데이터가 생성되는 순간부터 수집·검증·저장·모니터링까지 이어지는 데이터 플랫폼의 핵심 원리를 학습하고 구현하는 프로젝트입니다.

궁극적으로는 데이터 지연, 누락, 이상값, 스키마 변경, 처리 실패와 같은 운영 이슈에도 신뢰할 수 있는 결과를 제공하는 파이프라인을 구축하는 것을 목표로 합니다.