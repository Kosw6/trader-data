FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# 런타임마다 pip install을 하지 않도록 의존성을 이미지 레이어에 미리 설치한다.
COPY requirements.txt .
RUN python -m pip install --upgrade pip \
    && python -m pip install -r requirements.txt

# 기본 이미지는 전체 소스를 포함한다.
# 로컬 compose에서는 개발 편의를 위해 bind mount로 이 내용을 덮어쓸 수 있다.
COPY . .

CMD ["python", "-m", "workers.kafka_pipeline_consumer"]
