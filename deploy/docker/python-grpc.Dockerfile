FROM python:3.10-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONNOUSERSITE=1
ENV PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    libgl1 \
    libglib2.0-0 \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
COPY contracts/proto /app/contracts/proto
COPY apps /app/apps
COPY services /app/services
COPY config /app/config

RUN pip install --upgrade pip setuptools wheel \
    && grep -v '^mediapipe==' /app/requirements.txt > /app/requirements.docker.txt \
    && pip install -r /app/requirements.docker.txt \
    && pip install absl-py attrs flatbuffers matplotlib \
    && pip install --no-deps mediapipe==0.10.14 \
    && mkdir -p /app/contracts/gen/python \
    && python -m grpc_tools.protoc \
      -I /app/contracts/proto \
      --python_out /app/contracts/gen/python \
      --grpc_python_out /app/contracts/gen/python \
      /app/contracts/proto/video_processing.proto

EXPOSE 50051

CMD ["python", "apps/grpc-server/main.py"]
