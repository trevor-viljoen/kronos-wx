FROM python:3.13-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libhdf5-dev \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt
COPY kronos_web/backend/requirements.txt backend_requirements.txt
RUN pip install --user --no-cache-dir -r requirements.txt \
    && pip install --user --no-cache-dir -r backend_requirements.txt

FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libhdf5-dev \
    libxml2 \
    libxslt1.1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /root/.local /root/.local

ENV PATH=/root/.local/bin:$PATH \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    LOG_LEVEL=INFO \
    DATA_DIR=/app/data \
    LOG_DIR=/app/logs

COPY ok_weather_model/ ./ok_weather_model/
COPY kronos_web/backend/ ./kronos_web/backend/
COPY data/models/ ./bundled_models/
COPY data/analogues.json ./data/analogues.json
COPY scripts/entrypoint.sh ./entrypoint.sh

RUN mkdir -p data logs && chmod +x entrypoint.sh

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

EXPOSE 8000
ENTRYPOINT ["./entrypoint.sh"]
CMD ["uvicorn", "kronos_web.backend.api:app", "--host", "0.0.0.0", "--port", "8000"]
