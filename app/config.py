from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    DB_HOST: str
    DB_USER: str
    DB_PASSWORD: str
    DB_DATABASE: str
    TRATUM_EMAIL: str
    TRATUM_PASSWORD: str
    TRATUM_HOLDER_ID: int
    TRATUM_ORGANIZATION_ID: int
    TRATUM_PLAN_ID: int
    OPENAI_API_KEY: str

    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()