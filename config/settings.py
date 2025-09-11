from pydantic_settings import BaseSettings
from typing import Optional
from pathlib import Path

class Settings(BaseSettings):
    
    app_name: str = "VB to Python FastAPI"
    app_version: str = "1.0.0"
    app_description: str = "FastAPI application with hot reload in Docker"
    
    # Constants for file paths
    base_path: str = str(Path(__file__).parent.parent / "data")
    
    signal_file_name: str = "signal_hkex_energy"
    hk_energy_token_file_name: str = "hk_energy_token"
    hk_ta_token_file_name: str = "hk_ta_token"
    signals_hkex_ta1_file_name: str = "signals_hkex_ta1"
    signals_hkex_ta2_file_name: str = "signals_hkex_ta2"

    # test db tables
    test_db_table_ta1: str = "test_db_table_ta1"
    test_db_table_ta2: str = "test_db_table_ta2"
    test_db_table_energy: str = "test_db_table_energy"

    hk_ta_retry_schedule: str = "hk_ta_retry_schedule"

    # Daily retry task 
    daily_retry: int = 1 # 12 hours

    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = True
    
    log_level: str = "INFO"
    log_file: str = "fastapi_app"
    
    cors_origins: list = ["*"]
    cors_allow_credentials: bool = True
    cors_allow_methods: list = ["*"]
    cors_allow_headers: list = ["*"]
    
    # Database settings
    serhio_db_host: str = "localhost"
    serhio_db_port: int = 3306
    serhio_db: str = "derivates_crawler"
    serhio_db_user: str = "reader"
    serhio_db_pass: str = "password" 

    kl_db_host: str = "localhost"
    kl_db_port: int = 3306
    kl_db: str = "derivates_crawler"
    kl_db_user: str = "reader"
    kl_db_pass: str = "password" 

    api_key: str 

    # RabbitMQ settings
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "admin"
    rabbitmq_password: str = "password"
    rabbitmq_vhost: str = "/"
    
    # Celery settings
    celery_broker_url: str = "amqp://admin:password@localhost:5672//"
    celery_result_backend: str = "redis://redis:6379/0"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()

