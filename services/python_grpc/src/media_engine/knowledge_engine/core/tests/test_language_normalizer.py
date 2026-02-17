from services.python_grpc.src.media_engine.knowledge_engine.core.language_normalizer import (
    normalize_whisper_language,
    language_for_fingerprint,
)


def test_normalize_whisper_language_returns_none_for_auto_like_values():
    assert normalize_whisper_language(None) is None
    assert normalize_whisper_language("") is None
    assert normalize_whisper_language("auto") is None
    assert normalize_whisper_language("AUTO-DETECT") is None
    assert normalize_whisper_language("detect") is None


def test_normalize_whisper_language_maps_common_aliases():
    assert normalize_whisper_language("zh-cn") == "zh"
    assert normalize_whisper_language("zh_hans") == "zh"
    assert normalize_whisper_language("en-us") == "en"
    assert normalize_whisper_language("English") == "en"


def test_language_for_fingerprint_is_stable():
    assert language_for_fingerprint(None) == "auto"
    assert language_for_fingerprint("auto") == "auto"
    assert language_for_fingerprint("en-us") == "en"
