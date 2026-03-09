import os

import pytest

from services.python_grpc.src.media_engine.knowledge_engine.core import model_downloader as md


def test_download_whisper_model_retries_retryable_error(monkeypatch):
    calls = {"config.json": 0}

    def _fake_download(**kwargs):
        filename = kwargs["filename"]
        if filename == "config.json" and calls["config.json"] == 0:
            calls["config.json"] += 1
            raise RuntimeError("Server disconnected without sending a response.")
        calls[filename] = calls.get(filename, 0) + 1
        return f"/tmp/mock_model/{filename}"

    monkeypatch.setattr(md, "hf_hub_download", _fake_download)
    monkeypatch.setattr(md, "_verify_file_integrity", lambda *_: True)
    monkeypatch.setattr(md.time, "sleep", lambda *_: None)

    model_dir = md.download_whisper_model(
        model_size="small",
        use_mirror=False,
        max_retries=2,
        retry_base_delay_sec=0,
        enable_endpoint_fallback=False,
        skip_reverify_after_success=False,
    )

    assert model_dir == "/tmp/mock_model"
    assert calls["config.json"] == 2


def test_download_whisper_model_fallback_to_mirror(monkeypatch):
    endpoint_history = []

    def _fake_download(**kwargs):
        endpoint_history.append(os.environ.get("HF_ENDPOINT"))
        if os.environ.get("HF_ENDPOINT"):
            return f"/tmp/mock_model/{kwargs['filename']}"
        raise RuntimeError("Server disconnected without sending a response.")

    monkeypatch.setattr(md, "hf_hub_download", _fake_download)
    monkeypatch.setattr(md, "_verify_file_integrity", lambda *_: True)
    monkeypatch.setattr(md.time, "sleep", lambda *_: None)

    model_dir = md.download_whisper_model(
        model_size="small",
        use_mirror=False,
        hf_endpoint="https://hf-mirror.com",
        max_retries=1,
        enable_endpoint_fallback=True,
        skip_reverify_after_success=False,
    )

    assert model_dir == "/tmp/mock_model"
    assert endpoint_history[0] is None
    assert any(endpoint == "https://hf-mirror.com" for endpoint in endpoint_history[1:])


def test_download_whisper_model_non_retryable_error_no_fallback(monkeypatch):
    endpoint_history = []

    def _fake_download(**kwargs):
        endpoint_history.append(os.environ.get("HF_ENDPOINT"))
        raise RuntimeError("401 Unauthorized")

    monkeypatch.setattr(md, "hf_hub_download", _fake_download)
    monkeypatch.setattr(md.time, "sleep", lambda *_: None)

    with pytest.raises(RuntimeError, match="401 Unauthorized"):
        md.download_whisper_model(
            model_size="small",
            use_mirror=False,
            max_retries=1,
            enable_endpoint_fallback=True,
            skip_reverify_after_success=False,
        )

    assert endpoint_history == [None]


def test_download_whisper_model_skip_integrity_check_on_failure(monkeypatch):
    force_flags = []

    def _fake_download(**kwargs):
        force_flags.append(bool(kwargs.get("force_download", False)))
        return f"/tmp/mock_model/{kwargs['filename']}"

    monkeypatch.setattr(md, "hf_hub_download", _fake_download)
    monkeypatch.setattr(md, "_verify_file_integrity", lambda *_: False)
    monkeypatch.setattr(md.time, "sleep", lambda *_: None)

    model_dir = md.download_whisper_model(
        model_size="small",
        use_mirror=False,
        max_retries=1,
        enable_endpoint_fallback=False,
        skip_integrity_check_on_failure=True,
        skip_reverify_after_success=False,
    )

    assert model_dir == "/tmp/mock_model"
    assert force_flags
    assert not any(force_flags)


def test_download_whisper_model_skip_reverify_after_success(monkeypatch):
    workspace_tmp = os.path.join("var", "tmp_model_downloader_preverified")
    cache_dir = os.path.join(workspace_tmp, "hf_cache")
    repo_id = "Systran/faster-whisper-small"
    model_dir = os.path.join(workspace_tmp, "models", "snapshot")
    os.makedirs(model_dir, exist_ok=True)

    with open(os.path.join(model_dir, "config.json"), "w", encoding="utf-8") as f:
        f.write("{}")
    with open(os.path.join(model_dir, "model.bin"), "wb") as f:
        f.write(b"binary")
    with open(os.path.join(model_dir, "tokenizer.json"), "w", encoding="utf-8") as f:
        f.write("{}")

    md._write_verify_state(
        cache_dir,
        repo_id,
        {
            "repo_id": repo_id,
            "model_size": "small",
            "verified_at_epoch": 1770000000,
            "integrity_result": "passed",
            "essential_files": ["config.json", "model.bin", "tokenizer.json"],
        },
    )

    calls = []

    def _fake_download(**kwargs):
        calls.append(kwargs)
        if kwargs.get("local_files_only"):
            filename = kwargs["filename"]
            if filename == "vocabulary.txt":
                raise RuntimeError("not found")
            return os.path.join(model_dir, filename)
        raise AssertionError("should not trigger remote download when preverified")

    monkeypatch.setattr(md, "hf_hub_download", _fake_download)
    monkeypatch.setattr(md.os.path, "expanduser", lambda _: cache_dir)
    monkeypatch.setattr(
        md,
        "_verify_file_integrity",
        lambda *_: (_ for _ in ()).throw(AssertionError("should skip integrity checks after restart")),
    )

    resolved = md.download_whisper_model(
        model_size="small",
        use_mirror=False,
        max_retries=1,
        enable_endpoint_fallback=False,
        skip_reverify_after_success=True,
    )

    assert resolved == model_dir
    assert calls
    assert all(call.get("local_files_only") for call in calls)



def test_resolve_whisper_cache_dir_prefers_explicit_env(monkeypatch):
    monkeypatch.setenv("WHISPER_MODEL_CACHE_DIR", "/tmp/whisper-cache")
    monkeypatch.setenv("HUGGINGFACE_HUB_CACHE", "/tmp/hf-cache")
    monkeypatch.setenv("HF_HOME", "/tmp/hf-home")

    assert md._resolve_whisper_cache_dir() == "/tmp/whisper-cache"



def test_resolve_whisper_cache_dir_falls_back_to_hf_home(monkeypatch):
    monkeypatch.delenv("WHISPER_MODEL_CACHE_DIR", raising=False)
    monkeypatch.delenv("HUGGINGFACE_HUB_CACHE", raising=False)
    monkeypatch.setenv("HF_HOME", "/tmp/hf-home")

    assert md._resolve_whisper_cache_dir() == os.path.join("/tmp/hf-home", "hub")
