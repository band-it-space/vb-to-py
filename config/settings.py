from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    
    app_name: str = "VB to Python FastAPI"
    app_version: str = "1.0.0"
    app_description: str = "FastAPI application with hot reload in Docker"
    
    
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

    api_key: str 

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()

