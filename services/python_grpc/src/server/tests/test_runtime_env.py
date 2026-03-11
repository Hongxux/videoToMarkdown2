from pathlib import Path
import sys
import types

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.server import runtime_env as runtime_env_mod


def test_patch_protobuf_message_factory_compat_adds_getprototype(monkeypatch):
    fake_message_factory = types.ModuleType("google.protobuf.message_factory")

    class _FakeMessageFactory:
        pass

    captured = {}

    def _fake_get_message_class(descriptor):
        captured["descriptor"] = descriptor
        return ("message-class", descriptor)

    fake_message_factory.MessageFactory = _FakeMessageFactory
    fake_message_factory.GetMessageClass = _fake_get_message_class

    fake_protobuf = types.ModuleType("google.protobuf")
    fake_protobuf.message_factory = fake_message_factory

    fake_google = types.ModuleType("google")
    fake_google.protobuf = fake_protobuf

    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setitem(sys.modules, "google.protobuf", fake_protobuf)
    monkeypatch.setitem(sys.modules, "google.protobuf.message_factory", fake_message_factory)

    patched = runtime_env_mod.patch_protobuf_message_factory_compat()

    assert patched is True
    factory = _FakeMessageFactory()
    assert factory.GetPrototype("demo-descriptor") == ("message-class", "demo-descriptor")
    assert captured["descriptor"] == "demo-descriptor"