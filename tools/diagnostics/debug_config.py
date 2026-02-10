import sys
import os
import yaml
from pathlib import Path

# Add repo root to sys.path
repo_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(repo_root))

from services.python_grpc.src.server.service import VideoProcessingServicer, GlobalResourceManager
from services.python_grpc.src.config_paths import load_yaml_dict, resolve_video_config_path

def test_config_loading():
    print("Loading config...")
    config_path = resolve_video_config_path(anchor_file=__file__)
    config = load_yaml_dict(config_path) if config_path else {}
    print(f"Config loaded: {list(config.keys())}")
    print(f"Whisper config: {config.get('whisper', {})}")
    
    print("\nInitializing Servicer with config...")
    servicer = VideoProcessingServicer(config)
    
    print(f"\nServicer Config keys: {list(servicer.config.keys())}")
    print(f"GlobalResourceManager Config keys: {list(servicer.resources.config.keys())}")
    
    print("\nAccessing transcriber property...")
    transcriber = servicer.resources.transcriber
    if transcriber:
        print(f"Transcriber initialized: {transcriber}")
        print(f"Transcriber Parallel: {transcriber.parallel}")
        print(f"Transcriber Num Workers: {transcriber.num_workers}")
        print(f"Transcriber Config keys: {list(transcriber.config.keys()) if transcriber.config else 'None'}")
    else:
        print("Transcriber NOT initialized")

if __name__ == "__main__":
    test_config_loading()
