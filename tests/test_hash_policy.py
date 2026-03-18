import hashlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.python_grpc.src.common.utils import hash_policy


def test_fast_digest_text_prefers_xxh3_when_available():
    xxhash = pytest.importorskip("xxhash")

    expected = xxhash.xxh3_128_hexdigest("local-dedupe".encode("utf-8"))
    assert hash_policy.fast_hash_name() == "xxh3_128"
    assert hash_policy.fast_digest_text("local-dedupe") == expected


def test_fast_hasher_matches_one_shot_digest():
    hasher = hash_policy.fast_hasher()
    hasher.update(b"local-")
    hasher.update(b"verify")

    assert hasher.hexdigest() == hash_policy.fast_digest_bytes(b"local-verify")


def test_compat_and_external_hashes_keep_legacy_algorithms():
    payload = "stable-contract"

    assert hash_policy.md5_text_compat(payload) == hashlib.md5(payload.encode("utf-8")).hexdigest()
    assert hash_policy.sha256_text(payload) == hashlib.sha256(payload.encode("utf-8")).hexdigest()
