import sys
import os
import yaml
from pathlib import Path

# Add project root to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, "MVP_Module2_HEANCING"))

from python_grpc_server import VideoProcessingServicer, GlobalResourceManager, _load_yaml_file

def test_config_loading():
    print("Loading config...")
    root_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(root_dir, "videoToMarkdown", "config.yaml")
    config = _load_yaml_file(Path(config_path))
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
