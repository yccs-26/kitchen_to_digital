# 환경 변수 로드 및 자동 매핑
# 타입 검증 및 기본값 설정
# 하드코드 방지 및 중앙 집중화

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:29092"
    kafka_topic_raw: str = "kitchen.sensor.raw"
    kafka_client_id: str = "kitchen-sensor-simulator"

    store_id: str = "store-001"
    simulation_mode: str = "normal"
    default_interval_seconds: float = 1.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )