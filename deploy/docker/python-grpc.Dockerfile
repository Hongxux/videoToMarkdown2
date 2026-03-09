# syntax=docker/dockerfile:1.7
FROM python:3.10-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONNOUSERSITE=1
ENV PIP_NO_CACHE_DIR=1
ENV PIP_ROOT_USER_ACTION=ignore
ENV PYTHONPATH=/app:/app/contracts/gen/python
ENV HF_HOME=/opt/huggingface
ENV HUGGINGFACE_HUB_CACHE=/opt/huggingface/hub
ENV WHISPER_MODEL_CACHE_DIR=/opt/huggingface/hub
ENV WHISPER_PRELOAD_MODEL=base

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    tesseract-ocr \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
COPY requirements.grpc_server.txt /app/requirements.grpc_server.txt
COPY requirements.docker.txt /app/requirements.docker.txt
COPY requirements /app/requirements
COPY contracts/proto /app/contracts/proto
COPY apps /app/apps
COPY services /app/services
COPY config /app/config

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip setuptools wheel \
    && pip install --index-url https://download.pytorch.org/whl/cpu --extra-index-url https://pypi.org/simple torch torchvision torchaudio \
    && pip install -r /app/requirements.docker.txt \
    && pip install absl-py attrs flatbuffers matplotlib \
    && pip install --no-deps mediapipe==0.10.14 \
    && mkdir -p /app/contracts/gen/python \
    && python -m grpc_tools.protoc \
      -I /app/contracts/proto \
      --python_out /app/contracts/gen/python \
      --grpc_python_out /app/contracts/gen/python \
      /app/contracts/proto/video_processing.proto \
    && mkdir -p /opt/huggingface/hub \
    && python -X utf8 -c "import os; from services.python_grpc.src.media_engine.knowledge_engine.core.model_downloader import download_whisper_model; model_size = os.environ.get('WHISPER_PRELOAD_MODEL', 'base'); model_dir = download_whisper_model(model_size=model_size, hf_endpoint='https://hf-mirror.com', use_mirror=False, enable_endpoint_fallback=True, skip_integrity_check_on_failure=False, skip_reverify_after_success=True); print(f'Whisper model ready: {model_size} -> {model_dir}', flush=True)" \
    && ffmpeg -version > /dev/null

EXPOSE 50051

CMD ["python", "apps/grpc-server/main.py"]