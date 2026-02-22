import asyncio
import shutil
import uuid
from pathlib import Path

from services.python_grpc.src.content_pipeline.infra.llm.vision_ai_client import (
    VisionAIClient,
    VisionAIConfig,
    get_vision_ai_client,
)
import services.python_grpc.src.content_pipeline.infra.llm.vision_ai_client as vision_ai_client_module


def test_vision_client_prefers_explicit_api_key_over_env(monkeypatch):
    monkeypatch.setenv("DASHSCOPE_API_KEY", "env-key")
    config = VisionAIConfig(
        enabled=True,
        api_key="config-key",
        bearer_token="legacy-bearer",
        base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
    )
    client = VisionAIClient(config)

    assert client._api_key == "config-key"
    assert client._api_key_env == "DASHSCOPE_API_KEY"


def test_vision_client_loads_dashscope_api_key_from_env(monkeypatch):
    monkeypatch.setenv("DASHSCOPE_API_KEY", "dashscope-env-key")
    config = VisionAIConfig(
        enabled=True,
        api_key="",
        bearer_token="",
        base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
    )
    client = VisionAIClient(config)

    assert client._api_key == "dashscope-env-key"
    assert client._api_key_env == "DASHSCOPE_API_KEY"


def test_get_vision_ai_client_loads_runtime_vl_api_config(monkeypatch):
    monkeypatch.setenv("DASHSCOPE_API_KEY", "runtime-vl-key")
    monkeypatch.setattr(vision_ai_client_module, "_global_vision_client", None)

    def _fake_load_module2_config():
        return {
            "vl_material_generation": {
                "enabled": True,
                "api": {
                    "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
                    "model": "qwen-vl-max-2025-08-13",
                    "api_key": "",
                    "api_key_env": "DASHSCOPE_API_KEY",
                    "bearer_token": "",
                    "bearer_token_env": "",
                    "temperature": 0.2,
                },
            }
        }

    import services.python_grpc.src.content_pipeline.infra.runtime.config_loader as config_loader_module

    monkeypatch.setattr(config_loader_module, "load_module2_config", _fake_load_module2_config)
    client = get_vision_ai_client(None)
    try:
        assert client.config.enabled is True
        assert client.config.base_url.endswith("/chat/completions")
        assert client.config.model == "qwen-vl-max-2025-08-13"
        assert client._api_key == "runtime-vl-key"
    finally:
        asyncio.run(client.close())
        monkeypatch.setattr(vision_ai_client_module, "_global_vision_client", None)


def test_validate_image_person_subject_prefilter_skips_vision(monkeypatch):
    tmp_dir = Path("var") / f"tmp_test_vision_prefilter_{uuid.uuid4().hex}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    try:
        image_path = tmp_dir / "person.jpg"
        image_path.write_bytes(b"fake")

        config = VisionAIConfig(
            enabled=True,
            bearer_token="token",
            duplicate_detection_enabled=False,
            person_subject_filter_enabled=True,
            person_mask_area_threshold=0.3,
        )
        client = VisionAIClient(config)

        def _fake_prefilter(path: str):
            assert path == str(image_path)
            return {
                "has_concrete_knowledge": False,
                "should_include": False,
                "reason": "person_subject_prefilter",
                "person_mask_ratio": 0.66,
            }

        async def _fake_call_vision_api(*args, **kwargs):
            raise AssertionError("Vision API should not be called when prefilter matched")

        monkeypatch.setattr(client, "_run_person_subject_prefilter", _fake_prefilter)
        monkeypatch.setattr(client, "_call_vision_api", _fake_call_vision_api)

        result = asyncio.run(client.validate_image(str(image_path)))

        assert result["has_concrete_knowledge"] is False
        assert result["should_include"] is False
        assert result["reason"] == "person_subject_prefilter"
        assert client.get_stats().get("person_subject_skips", 0) == 1
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_validate_images_batch_person_subject_prefilter_keeps_order(monkeypatch):
    tmp_root = Path("var") / f"tmp_test_vision_prefilter_{uuid.uuid4().hex}"
    tmp_root.mkdir(parents=True, exist_ok=True)
    try:
        image_person = tmp_root / "person.jpg"
        image_non_person = tmp_root / "slide.jpg"
        image_person.write_bytes(b"p")
        image_non_person.write_bytes(b"s")

        config = VisionAIConfig(
            enabled=True,
            bearer_token="token",
            duplicate_detection_enabled=False,
            batch_enabled=True,
            batch_max_size=4,
            person_subject_filter_enabled=True,
            person_mask_area_threshold=0.3,
        )
        client = VisionAIClient(config)
        called = {"paths": []}

        def _fake_prefilter(path: str):
            if path == str(image_person):
                return {
                    "has_concrete_knowledge": False,
                    "should_include": False,
                    "reason": "person_subject_prefilter",
                    "person_mask_ratio": 0.72,
                }
            return None

        async def _fake_call_vision_api_batch(image_paths, prompt="", system_prompt=None):
            called["paths"] = list(image_paths)
            return [
                {
                    "has_concrete_knowledge": True,
                    "should_include": True,
                    "reason": "vision_ok",
                }
                for _ in image_paths
            ]

        monkeypatch.setattr(client, "_run_person_subject_prefilter", _fake_prefilter)
        monkeypatch.setattr(client, "_call_vision_api_batch", _fake_call_vision_api_batch)

        results = asyncio.run(
            client.validate_images_batch([str(image_person), str(image_non_person)], prompt="")
        )

        assert called["paths"] == [str(image_non_person)]
        assert results[0]["should_include"] is False
        assert results[1]["should_include"] is True
        assert results[0]["reason"] == "person_subject_prefilter"
        assert results[1]["reason"] == "vision_ok"
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


def test_validate_image_person_subject_prefilter_deletes_file(monkeypatch):
    tmp_root = Path("var") / f"tmp_test_vision_prefilter_delete_{uuid.uuid4().hex}"
    tmp_root.mkdir(parents=True, exist_ok=True)
    try:
        image_path = tmp_root / "person.jpg"
        image_path.write_bytes(b"p")

        config = VisionAIConfig(
            enabled=True,
            bearer_token="token",
            duplicate_detection_enabled=False,
            person_subject_filter_enabled=True,
            person_mask_area_threshold=0.3,
        )
        client = VisionAIClient(config)

        def _fake_prefilter(path: str):
            assert path == str(image_path)
            return {
                "has_concrete_knowledge": False,
                "should_include": False,
                "reason": "person_subject_prefilter",
                "person_mask_ratio": 0.91,
            }

        async def _fake_call_vision_api(*args, **kwargs):
            raise AssertionError("Vision API should not be called when prefilter matched")

        monkeypatch.setattr(client, "_run_person_subject_prefilter", _fake_prefilter)
        monkeypatch.setattr(client, "_call_vision_api", _fake_call_vision_api)

        result = asyncio.run(client.validate_image(str(image_path)))

        assert result["should_include"] is False
        assert result["reason"] == "person_subject_prefilter"
        assert not image_path.exists()
        stats = client.get_stats()
        assert stats.get("person_subject_skips", 0) == 1
        assert stats.get("person_subject_deleted_files", 0) == 1
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


def test_validate_images_batch_person_subject_prefilter_deletes_file(monkeypatch):
    tmp_root = Path("var") / f"tmp_test_vision_prefilter_batch_delete_{uuid.uuid4().hex}"
    tmp_root.mkdir(parents=True, exist_ok=True)
    try:
        image_person = tmp_root / "person.jpg"
        image_non_person = tmp_root / "slide.jpg"
        image_person.write_bytes(b"p")
        image_non_person.write_bytes(b"s")

        config = VisionAIConfig(
            enabled=True,
            bearer_token="token",
            duplicate_detection_enabled=False,
            batch_enabled=True,
            batch_max_size=4,
            person_subject_filter_enabled=True,
            person_mask_area_threshold=0.3,
        )
        client = VisionAIClient(config)

        def _fake_prefilter(path: str):
            if path == str(image_person):
                return {
                    "has_concrete_knowledge": False,
                    "should_include": False,
                    "reason": "person_subject_prefilter",
                    "person_mask_ratio": 0.88,
                }
            return None

        async def _fake_call_vision_api_batch(image_paths, prompt="", system_prompt=None):
            return [
                {
                    "has_concrete_knowledge": True,
                    "should_include": True,
                    "reason": "vision_ok",
                }
                for _ in image_paths
            ]

        monkeypatch.setattr(client, "_run_person_subject_prefilter", _fake_prefilter)
        monkeypatch.setattr(client, "_call_vision_api_batch", _fake_call_vision_api_batch)

        results = asyncio.run(
            client.validate_images_batch([str(image_person), str(image_non_person)], prompt="")
        )

        assert results[0]["should_include"] is False
        assert results[1]["should_include"] is True
        assert not image_person.exists()
        assert image_non_person.exists()
        stats = client.get_stats()
        assert stats.get("person_subject_skips", 0) == 1
        assert stats.get("person_subject_deleted_files", 0) == 1
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


def test_person_subject_prefilter_threshold_is_strict_greater(monkeypatch):
    config = VisionAIConfig(
        person_subject_filter_enabled=True,
        person_mask_area_threshold=0.3,
        person_mask_high_conf_min_area=0.08,
    )
    client = VisionAIClient(config)

    monkeypatch.setattr(
        client,
        "_estimate_person_mask_signals",
        lambda _: {"mask_ratio": 0.3, "high_conf_ratio": 0.5},
    )
    assert client._run_person_subject_prefilter("dummy.jpg") is None

    monkeypatch.setattr(
        client,
        "_estimate_person_mask_signals",
        lambda _: {"mask_ratio": 0.3001, "high_conf_ratio": 0.2},
    )
    result = client._run_person_subject_prefilter("dummy.jpg")
    assert isinstance(result, dict)
    assert result.get("should_include") is False


def test_person_subject_prefilter_guard_avoids_false_positive(monkeypatch):
    config = VisionAIConfig(
        person_subject_filter_enabled=True,
        person_mask_area_threshold=0.3,
        person_mask_high_conf_min_area=0.08,
    )
    client = VisionAIClient(config)

    monkeypatch.setattr(
        client,
        "_estimate_person_mask_signals",
        lambda _: {"mask_ratio": 0.7452, "high_conf_ratio": 0.01},
    )
    assert client._run_person_subject_prefilter("dummy.jpg") is None


def test_person_subject_prefilter_force_include_pattern(monkeypatch):
    config = VisionAIConfig(
        person_subject_filter_enabled=True,
        person_mask_area_threshold=0.3,
        person_mask_high_conf_min_area=0.08,
        person_prefilter_force_include_patterns=["SU012_ss_island_006.jpg"],
    )
    client = VisionAIClient(config)

    monkeypatch.setattr(
        client,
        "_estimate_person_mask_signals",
        lambda _: {"mask_ratio": 0.95, "high_conf_ratio": 0.95},
    )
    assert client._run_person_subject_prefilter("tmp/SU012_ss_island_006.jpg") is None


def test_validate_images_batch_prefilter_runs_reverse_order(monkeypatch):
    config = VisionAIConfig(
        enabled=True,
        bearer_token="token",
        duplicate_detection_enabled=False,
        batch_enabled=True,
        batch_max_size=8,
        person_subject_filter_enabled=True,
    )
    client = VisionAIClient(config)
    checked_paths = []

    def _fake_prefilter(path: str):
        checked_paths.append(path)
        return None

    async def _fake_call_vision_api_batch(image_paths, prompt="", system_prompt=None):
        return [
            {
                "has_concrete_knowledge": True,
                "should_include": True,
                "reason": Path(path).name,
            }
            for path in image_paths
        ]

    monkeypatch.setattr(client, "_run_person_subject_prefilter", _fake_prefilter)
    monkeypatch.setattr(client, "_call_vision_api_batch", _fake_call_vision_api_batch)

    image_paths = ["front.jpg", "middle.jpg", "back.jpg"]
    results = asyncio.run(client.validate_images_batch(image_paths, prompt=""))

    assert checked_paths == ["back.jpg", "middle.jpg", "front.jpg"]
    assert [item.get("reason") for item in results] == ["front.jpg", "middle.jpg", "back.jpg"]


def test_validate_images_batch_duplicate_check_runs_reverse_order(monkeypatch):
    config = VisionAIConfig(
        enabled=True,
        bearer_token="token",
        duplicate_detection_enabled=True,
        batch_enabled=True,
        batch_max_size=8,
        person_subject_filter_enabled=False,
    )
    client = VisionAIClient(config)

    class _FakeHashCache:
        def __init__(self):
            self.checked = []

        def check_duplicate(self, image_path: str):
            self.checked.append(image_path)
            if image_path == "front.jpg":
                return True, {
                    "has_concrete_knowledge": False,
                    "should_include": False,
                    "reason": "dup_front",
                }
            return False, None

        def store_result(self, image_path: str, result):
            return None

    fake_cache = _FakeHashCache()
    called = {"paths": []}

    async def _fake_call_vision_api_batch(image_paths, prompt="", system_prompt=None):
        called["paths"] = list(image_paths)
        return [
            {
                "has_concrete_knowledge": True,
                "should_include": True,
                "reason": f"vision_{Path(path).name}",
            }
            for path in image_paths
        ]

    client._hash_cache = fake_cache
    monkeypatch.setattr(client, "_call_vision_api_batch", _fake_call_vision_api_batch)

    results = asyncio.run(client.validate_images_batch(["front.jpg", "back.jpg"], prompt=""))

    assert fake_cache.checked == ["back.jpg", "front.jpg"]
    assert called["paths"] == ["back.jpg"]
    assert results[0].get("reason") == "dup_front"
    assert results[1].get("reason") == "vision_back.jpg"


def test_validate_image_duplicate_hit_deletes_file(monkeypatch):
    tmp_root = Path("var") / f"tmp_test_vision_dup_delete_{uuid.uuid4().hex}"
    tmp_root.mkdir(parents=True, exist_ok=True)
    try:
        image_path = tmp_root / "dup.jpg"
        image_path.write_bytes(b"dup")

        config = VisionAIConfig(
            enabled=True,
            bearer_token="token",
            duplicate_detection_enabled=True,
            person_subject_filter_enabled=False,
        )
        client = VisionAIClient(config)

        class _FakeHashCache:
            def check_duplicate(self, image_path: str):
                return True, {
                    "has_concrete_knowledge": False,
                    "should_include": False,
                    "reason": "dup_hit",
                }

            def check_duplicate_with_mode(self, image_path: str):
                return True, {
                    "has_concrete_knowledge": False,
                    "should_include": False,
                    "reason": "dup_hit",
                }, True

            def store_result(self, image_path: str, result):
                return None

            def get_stats(self):
                return {}

        async def _fake_call_vision_api(*args, **kwargs):
            raise AssertionError("Vision API should not be called on duplicate hit")

        client._hash_cache = _FakeHashCache()
        monkeypatch.setattr(client, "_call_vision_api", _fake_call_vision_api)

        result = asyncio.run(client.validate_image(str(image_path)))

        assert result.get("reason") == "dup_hit"
        assert not image_path.exists()
        stats = client.get_stats()
        assert stats.get("duplicate_skips", 0) == 1
        assert stats.get("duplicate_deleted_files", 0) == 1
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


def test_validate_images_batch_duplicate_hit_deletes_file(monkeypatch):
    tmp_root = Path("var") / f"tmp_test_vision_dup_delete_{uuid.uuid4().hex}"
    tmp_root.mkdir(parents=True, exist_ok=True)
    try:
        image_front = tmp_root / "front.jpg"
        image_back = tmp_root / "back.jpg"
        image_front.write_bytes(b"front")
        image_back.write_bytes(b"back")

        config = VisionAIConfig(
            enabled=True,
            bearer_token="token",
            duplicate_detection_enabled=True,
            batch_enabled=True,
            batch_max_size=8,
            person_subject_filter_enabled=False,
        )
        client = VisionAIClient(config)

        class _FakeHashCache:
            def __init__(self):
                self.checked = []

            def check_duplicate(self, image_path: str):
                self.checked.append(image_path)
                if image_path == str(image_front):
                    return True, {
                        "has_concrete_knowledge": False,
                        "should_include": False,
                        "reason": "dup_front",
                    }
                return False, None

            def check_duplicate_with_mode(self, image_path: str):
                self.checked.append(image_path)
                if image_path == str(image_front):
                    return True, {
                        "has_concrete_knowledge": False,
                        "should_include": False,
                        "reason": "dup_front",
                    }, True
                return False, None, False

            def store_result(self, image_path: str, result):
                return None

            def get_stats(self):
                return {}

        fake_cache = _FakeHashCache()
        called = {"paths": []}

        async def _fake_call_vision_api_batch(image_paths, prompt="", system_prompt=None):
            called["paths"] = list(image_paths)
            return [
                {
                    "has_concrete_knowledge": True,
                    "should_include": True,
                    "reason": f"vision_{Path(path).name}",
                }
                for path in image_paths
            ]

        client._hash_cache = fake_cache
        monkeypatch.setattr(client, "_call_vision_api_batch", _fake_call_vision_api_batch)

        results = asyncio.run(
            client.validate_images_batch([str(image_front), str(image_back)], prompt="")
        )

        assert fake_cache.checked == [str(image_back), str(image_front)]
        assert called["paths"] == [str(image_back)]
        assert results[0].get("reason") == "dup_front"
        assert results[1].get("reason") == "vision_back.jpg"
        assert not image_front.exists()
        assert image_back.exists()
        stats = client.get_stats()
        assert stats.get("duplicate_skips", 0) == 1
        assert stats.get("duplicate_deleted_files", 0) == 1
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


def test_validate_image_similar_duplicate_hit_does_not_delete_file(monkeypatch):
    tmp_root = Path("var") / f"tmp_test_vision_dup_keep_{uuid.uuid4().hex}"
    tmp_root.mkdir(parents=True, exist_ok=True)
    try:
        image_path = tmp_root / "near_dup.jpg"
        image_path.write_bytes(b"dup")

        config = VisionAIConfig(
            enabled=True,
            bearer_token="token",
            duplicate_detection_enabled=True,
            person_subject_filter_enabled=False,
        )
        client = VisionAIClient(config)

        class _FakeHashCache:
            def check_duplicate_with_mode(self, image_path: str):
                return True, {
                    "has_concrete_knowledge": False,
                    "should_include": False,
                    "reason": "dup_similar",
                }, False

            def check_duplicate(self, image_path: str):
                return True, {
                    "has_concrete_knowledge": False,
                    "should_include": False,
                    "reason": "dup_similar",
                }

            def store_result(self, image_path: str, result):
                return None

            def get_stats(self):
                return {}

        async def _fake_call_vision_api(*args, **kwargs):
            raise AssertionError("Vision API should not be called on duplicate hit")

        client._hash_cache = _FakeHashCache()
        monkeypatch.setattr(client, "_call_vision_api", _fake_call_vision_api)

        result = asyncio.run(client.validate_image(str(image_path)))

        assert result.get("reason") == "dup_similar"
        assert image_path.exists()
        stats = client.get_stats()
        assert stats.get("duplicate_skips", 0) == 1
        assert stats.get("duplicate_deleted_files", 0) == 0
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


def test_validate_images_batch_single_mode_runs_reverse_order(monkeypatch):
    config = VisionAIConfig(
        enabled=True,
        bearer_token="token",
        duplicate_detection_enabled=False,
        batch_enabled=False,
    )
    client = VisionAIClient(config)
    called = []

    async def _fake_validate_image(image_path: str, prompt="", system_prompt=None, skip_duplicate_check=False):
        called.append(image_path)
        return {"reason": image_path, "should_include": True}

    monkeypatch.setattr(client, "validate_image", _fake_validate_image)
    image_paths = ["front.jpg", "middle.jpg", "back.jpg"]
    results = asyncio.run(client.validate_images_batch(image_paths, prompt=""))

    assert called == ["back.jpg", "middle.jpg", "front.jpg"]
    assert [item.get("reason") for item in results] == ["front.jpg", "middle.jpg", "back.jpg"]
