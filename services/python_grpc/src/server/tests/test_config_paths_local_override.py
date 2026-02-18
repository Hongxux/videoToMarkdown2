import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[5]))


from services.python_grpc.src.config_paths import load_yaml_dict


def _write(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_load_yaml_dict_applies_local_override(tmp_path: Path) -> None:
    base = tmp_path / "video_config.yaml"
    local = tmp_path / "video_config.local.yaml"
    _write(
        base,
        """
ai:
  api_key: ""
  base_url: "https://api.example.com"
vision_ai:
  bearer_token: ""
  enabled: false
""".strip(),
    )
    _write(
        local,
        """
ai:
  api_key: "local-key"
vision_ai:
  bearer_token: "local-token"
""".strip(),
    )

    data = load_yaml_dict(base)

    assert data["ai"]["api_key"] == "local-key"
    assert data["ai"]["base_url"] == "https://api.example.com"
    assert data["vision_ai"]["bearer_token"] == "local-token"
    assert data["vision_ai"]["enabled"] is False


def test_load_yaml_dict_without_local_file_keeps_base(tmp_path: Path) -> None:
    base = tmp_path / "module2_config.yaml"
    _write(
        base,
        """
vl_material_generation:
  api:
    api_key: ""
    model: "qwen"
""".strip(),
    )

    data = load_yaml_dict(base)

    assert data["vl_material_generation"]["api"]["api_key"] == ""
    assert data["vl_material_generation"]["api"]["model"] == "qwen"
