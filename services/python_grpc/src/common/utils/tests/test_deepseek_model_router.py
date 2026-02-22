from services.python_grpc.src.common.utils.deepseek_model_router import resolve_deepseek_model


def test_resolve_deepseek_model_reasoner_aliases():
    assert resolve_deepseek_model("deepseek-r1") == "deepseek-reasoner"
    assert resolve_deepseek_model("v3 reasoner") == "deepseek-reasoner"
    assert resolve_deepseek_model("v3.2 reasoner") == "deepseek-reasoner"
    assert resolve_deepseek_model("deepseek-v3.2-reasoner") == "deepseek-reasoner"
    assert resolve_deepseek_model("deepseek-resoner") == "deepseek-reasoner"


def test_resolve_deepseek_model_chat_aliases():
    assert resolve_deepseek_model("deepseek-v3") == "deepseek-chat"
    assert resolve_deepseek_model("v3") == "deepseek-chat"
    assert resolve_deepseek_model("deepseek-v3.2") == "deepseek-chat"
    assert resolve_deepseek_model("v3.2") == "deepseek-chat"


def test_resolve_deepseek_model_unknown_kept():
    assert resolve_deepseek_model("deepseek-chat") == "deepseek-chat"
    assert resolve_deepseek_model("custom-model-x") == "custom-model-x"
    assert resolve_deepseek_model("", default_model="deepseek-reasoner") == "deepseek-reasoner"
