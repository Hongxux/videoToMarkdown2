import os
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[5]))


from services.python_grpc.src.server import grpc_service_impl as impl


def test_primary_storage_root_defaults_to_var_storage(monkeypatch):
    monkeypatch.delenv("V2M_STORAGE_ROOT", raising=False)
    root = impl._get_primary_storage_root()
    expected_suffix = os.path.normpath(os.path.join("var", "storage", "storage"))
    assert os.path.normpath(root).endswith(expected_suffix)


def test_primary_storage_root_can_be_overridden(monkeypatch):
    custom_root = os.path.abspath("tmp/custom_storage_root")
    monkeypatch.setenv("V2M_STORAGE_ROOT", custom_root)
    root = impl._get_primary_storage_root()
    assert os.path.normpath(root) == os.path.normpath(custom_root)
