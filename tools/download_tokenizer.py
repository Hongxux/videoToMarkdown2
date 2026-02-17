from huggingface_hub import hf_hub_download
import os

repo_id = "microsoft/Florence-2-base-ft"
local_dir = "var/models/omniparser-v2/icon_caption"
files = [
    "tokenizer.json",
    "tokenizer_config.json",
    "special_tokens_map.json",
    "vocab.json",
    "merges.txt"
]

for file in files:
    try:
        print(f"Downloading {file}...")
        hf_hub_download(repo_id=repo_id, filename=file, local_dir=local_dir)
        print(f"✓ Downloaded {file}")
    except Exception as e:
        print(f"✗ Failed to download {file}: {e}")
