from pathlib import Path

from services.python_grpc.src.content_pipeline.phase2a.segmentation.concrete_knowledge_validator import (
    ConcreteKnowledgeValidator,
)


def _write(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_load_vision_config_supports_dashscope_api_key_env(monkeypatch, tmp_path: Path):
    config_path = tmp_path / "video_config.yaml"
    _write(
        config_path,
        """
vision_ai:
  enabled: true
  provider: "dashscope"
  api_key: ""
  api_key_env: "DASHSCOPE_API_KEY"
  bearer_token: ""
  base_url: "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
  model: "qwen-vl-plus-2025-07-10"
""".strip(),
    )
    monkeypatch.setenv("DASHSCOPE_API_KEY", "dashscope-env-token")

    validator = ConcreteKnowledgeValidator.__new__(ConcreteKnowledgeValidator)
    vision_cfg = validator._load_vision_config(str(config_path))

    assert vision_cfg is not None
    assert vision_cfg.api_key == "dashscope-env-token"
    assert vision_cfg.bearer_token == "dashscope-env-token"
    assert vision_cfg.api_key_env == "DASHSCOPE_API_KEY"
    assert vision_cfg.model == "qwen-vl-plus-2025-07-10"
    assert vision_cfg.base_url == "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
