import os

class Config:
    # Server Settings
    GRPC_PORT = int(os.getenv("GRPC_PORT", "50060"))
    MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
    
    # Dependencies
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    # Feature Extraction Settings
    DEFAULT_SAMPLE_RATE = 2
    ENABLE_CUDA = True

config = Config()
