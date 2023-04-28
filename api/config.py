from pydantic import BaseSettings


class Settings(BaseSettings):
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    """Application configuration"""
    APP_TITLE: str = "storage interface implement"
    APP_DESCRIPTION: str = "Implement a simple RAID 5 storage web interface"
    APP_VERSION: str = "0.1.0"
    APP_OPENAPI_URL: str = "/openapi.json"
    APP_PREFIX: str = "/api"


settings = Settings()
