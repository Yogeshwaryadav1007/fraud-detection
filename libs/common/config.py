"""Central configuration. Every service reads from env vars with sane local defaults."""
import os


def _b(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).lower() in ("1", "true", "yes")


class Settings:
    # --- identity ---
    SERVICE_NAME: str = os.getenv("SERVICE_NAME", "unknown-service")
    ENV: str = os.getenv("ENV", "local")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # --- kafka ---
    KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    KAFKA_GROUP_PREFIX: str = os.getenv("KAFKA_GROUP_PREFIX", "fraud")

    # --- stores ---
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    POSTGRES_DSN: str = os.getenv(
        "POSTGRES_DSN", "postgresql://fraud:fraud@localhost:5432/fraud"
    )
    NEO4J_URI: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "fraudgraph")
    ELASTIC_URL: str = os.getenv("ELASTIC_URL", "http://localhost:9200")

    # --- observability ---
    OTEL_ENDPOINT: str = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    METRICS_PORT: int = int(os.getenv("METRICS_PORT", "9100"))
    TRACING_ENABLED: bool = _b("TRACING_ENABLED", "false")

    # --- business tuning ---
    DECISION_TIMEOUT_MS: int = int(os.getenv("DECISION_TIMEOUT_MS", "800"))
    APPROVE_MAX: float = float(os.getenv("APPROVE_MAX", "30"))
    CHALLENGE_MAX: float = float(os.getenv("CHALLENGE_MAX", "60"))
    REVIEW_MAX: float = float(os.getenv("REVIEW_MAX", "85"))

    W_RULES: float = float(os.getenv("W_RULES", "0.40"))
    W_ML: float = float(os.getenv("W_ML", "0.35"))
    W_GRAPH: float = float(os.getenv("W_GRAPH", "0.25"))

    MODEL_PATH: str = os.getenv("MODEL_PATH", "/app/ml/model.joblib")
    API_KEYS: str = os.getenv("API_KEYS", "demo-key-1,demo-key-2")


settings = Settings()
