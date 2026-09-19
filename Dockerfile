# Single parameterised image for every Python service.
# Build:  docker build --build-arg SERVICE=rule-engine -t fraud/rule-engine .
FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

# --- runtime ---------------------------------------------------------------
FROM base AS runtime
ARG SERVICE
ENV SERVICE_NAME=${SERVICE} PYTHONPATH=/app:/app/service

COPY libs /app/libs
COPY ml /app/ml
COPY services/${SERVICE} /app/service

RUN useradd -u 10001 -m appuser && chown -R appuser /app
USER appuser

EXPOSE 8080 9100
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
  CMD curl -fsS http://localhost:9100/metrics || exit 1

WORKDIR /app/service
CMD ["python", "main.py"]

# --- http variant (gateway / ingestion / cases run uvicorn) ----------------
FROM runtime AS http
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "2"]
