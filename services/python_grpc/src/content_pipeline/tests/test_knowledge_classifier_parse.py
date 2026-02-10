"""
模块说明：KnowledgeClassifier 解析函数的测试用例集合。
执行逻辑：
1) 构造不同“非严格 JSON”输出样例（代码块/尾随文本/尾逗号/单引号/中文标点等）。
2) 调用 _parse_batch_content 做容错解析，并断言能稳定回收 items。
实现方式：pytest 单元测试 + 直接调用解析函数（绕过 __init__）。
核心价值：为 multi-unit batch / 单元 batch 的 JSON 解析提供回归保护，避免解析失败导致 knowledge_type 断链。
输入：
- 各测试用例内构造的字符串 content。
输出：
- 断言通过/失败（pytest 结果）。"""

import importlib


def _load_knowledge_classifier_module():
    # 做什么：按包路径导入模块，确保相对导入（from . import ...）可解析。
    # 为什么：知识分类器新增仓储依赖后，文件路径直载会丢失包上下文导致导入失败。
    # 权衡：保留轻量导入方式，避免执行与本测试无关的重型链路。
    return importlib.import_module("services.python_grpc.src.content_pipeline.phase2a.segmentation.knowledge_classifier")


KnowledgeClassifier = _load_knowledge_classifier_module().KnowledgeClassifier


def _parser() -> KnowledgeClassifier:
    # 不触发 __init__：做什么是避免依赖 API Key/网络；为什么是聚焦解析逻辑；权衡是跳过初始化校验
    return KnowledgeClassifier.__new__(KnowledgeClassifier)


def test_parse_strict_json_array():
    kc = _parser()
    content = (
        '[{"id":"0","knowledge_type":"讲解型","confidence":0.9,'
        '"reasoning":"ok","key_evidence":"e"}]'
    )
    res = kc._parse_batch_content(content)
    assert isinstance(res, list)
    assert res and res[0]["id"] == "0"


def test_parse_code_fence_json():
    kc = _parser()
    content = """```json
[
  {"id":"0","knowledge_type":"讲解型","confidence":0.9,"reasoning":"ok","key_evidence":"e"}
]
```"""
    res = kc._parse_batch_content(content)
    assert len(res) == 1
    assert res[0]["knowledge_type"] == "讲解型"


def test_parse_trailing_text():
    kc = _parser()
    content = """结果如下：
[
  {"id":"0","knowledge_type":"讲解型","confidence":0.9,"reasoning":"ok","key_evidence":"e"}
]
谢谢。"""
    res = kc._parse_batch_content(content)
    assert len(res) == 1
    assert res[0]["id"] == "0"


def test_parse_trailing_commas():
    kc = _parser()
    content = """[
  {"id":"0","knowledge_type":"讲解型","confidence":0.9,"reasoning":"ok","key_evidence":"e",},
]"""
    res = kc._parse_batch_content(content)
    assert len(res) == 1
    assert res[0]["confidence"] == 0.9


def test_parse_single_quotes_pythonish():
    kc = _parser()
    content = """[
  {'id': '0', 'knowledge_type': '讲解型', 'confidence': 0.9, 'reasoning': 'ok', 'key_evidence': 'e'},
]"""
    res = kc._parse_batch_content(content)
    assert len(res) == 1
    assert res[0]["id"] == "0"


def test_parse_chinese_punctuation_outside_strings():
    kc = _parser()
    content = """[
  {"id":"0"，"knowledge_type":"讲解型"，"confidence":0.9，"reasoning":"ok"，"key_evidence":"e"}
]"""
    res = kc._parse_batch_content(content)
    assert len(res) == 1
    assert res[0]["knowledge_type"] == "讲解型"


def test_parse_unescaped_newline_in_string():
    kc = _parser()
    content = """[
  {"id":"0","knowledge_type":"讲解型","confidence":0.9,"reasoning":"line1
line2","key_evidence":"e"}
]"""
    res = kc._parse_batch_content(content)
    assert len(res) == 1
    assert "line1" in res[0]["reasoning"]


def test_parse_truncated_array_salvage_objects():
    kc = _parser()
    content = """[
  {"id":"0","knowledge_type":"讲解型","confidence":0.9,"reasoning":"a","key_evidence":"b"},
  {"id":"1","knowledge_type":"实操","confidence":0.8,"reasoning":"c","key_evidence":"d"}
"""
    res = kc._parse_batch_content(content)
    assert len(res) == 2
    assert {r["id"] for r in res} == {"0", "1"}


def test_parse_items_wrapper():
    kc = _parser()
    content = """{
  "items": [
    {"id":"0","knowledge_type":"讲解型","confidence":0.9,"reasoning":"ok","key_evidence":"e"}
  ]
}"""
    res = kc._parse_batch_content(content)
    assert len(res) == 1
    assert res[0]["id"] == "0"

