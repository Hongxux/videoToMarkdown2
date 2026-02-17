"""
小模型替换大模型效果对比测试
===============================
对 4 张测试截图同时运行三种方案，对比「具象性知识判定」和「图像描述」的质量差异。

方案 A — 云端 Vision AI（baseline）
方案 B — RapidOCR + YOLOv8n（决策文档方案）
方案 C — MiniCPM-V4.5 via HTTP（本地 VLM）

用法：
    python tools/diagnostics/test_small_model_replacement.py
"""

import asyncio
import base64
import json
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from services.python_grpc.src.content_pipeline.infra.llm.llm_client import LLMClient
except ImportError:
    # Fallback or error if not found (though sys.path should handle it)
    print("Warning: Could not import LLMClient. DeepSeek judgment may fail.")
    LLMClient = None

async def assess_concrete_knowledge_with_llm(description: str) -> Tuple[bool, str]:
    """使用 DeepSeek 判定是否包含具象性知识"""
    if not LLMClient:
        return False, "LLMClient not available"

    client = LLMClient(model="deepseek-chat", temperature=0.0)
    
    prompt = f"""
    Based on the image description provided below, determine whether the image contains **Concrete Knowledge**.
    
    Image Description:
    {description}
    
    Judgment Rules:
    
    **POSITIVE (Contains Concrete Knowledge)** - Satisfies ANY of these:
    - Educational content: photos of real objects, specimens, lab equipment, anatomical diagrams, structural diagrams, maps.
    - Mathematical formulas: equations, derivation steps, symbolic expressions.
    - Functional diagrams: abstract block diagrams, logic flowcharts, mind maps used for explaining concepts (not decorative).
    - Specific interfaces: software screenshots, IDEs, terminals, command lines that show actual operations or code.
    - Data visualizations: charts, graphs, plots showing data or trends.
    - Visual forms that help beginners intuitively understand a concept.
    
    **NEGATIVE (No Concrete Knowledge)**:
    - Pure text without functional graphics.
    - Decorative images only (clip art, spacers) with no educational purpose.
    - Images of the speaker/teacher only.
    - Simple UI frames without specific content or operations.
    - General software interfaces without specific code, data, or complex workflow (e.g., empty login screen, simple menu).
    
    Return a JSON object with:
    - "has_concrete_knowledge": boolean
    - "reason": string (brief explanation)
    """
    
    try:
        # LLMClient.complete_json returns (parsed_json, metadata, logprobs)
        data, _, _ = await client.complete_json(prompt)
        return data.get("has_concrete_knowledge", False), data.get("reason", "")
    except Exception as e:
        print(f"[DeepSeek] Error: {e}")
        return False, f"LLM Error: {e}"

# ---------------------------------------------------------------------------
# 测试图片列表
# ---------------------------------------------------------------------------
TEST_IMAGES: List[Path] = [
    PROJECT_ROOT / "var/storage/storage/5dd689b51667d593eb2e36d8b2f8d204/assets/SU005/SU005_ss_route_008.jpg",
    PROJECT_ROOT / "var/storage/storage/5dd689b51667d593eb2e36d8b2f8d204/assets/SU005/SU005_ss_route_009.jpg",
    PROJECT_ROOT / "var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU018/SU018_ss_island_004.jpg",
    PROJECT_ROOT / "var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU023/SU023_ss_island_005.jpg",
]

# 人工标注的预期结果（ground truth）
EXPECTED_RESULTS: Dict[str, Dict[str, Any]] = {
    "SU005_ss_route_008.jpg": {
        "has_concrete_knowledge": True,
        "expected_type": "表格/数据列表",
        "description": "AI发展里程碑表格截图，包含产品名称、日期、公司等结构化数据",
    },
    "SU005_ss_route_009.jpg": {
        "has_concrete_knowledge": True,
        "expected_type": "时间线/流程图",
        "description": "AI发展历史时间线图，含多个关键节点和图标",
    },
    "SU018_ss_island_004.jpg": {
        "has_concrete_knowledge": True,
        "expected_type": "示意图/代码",
        "description": "Agent工具调用流程示意图，含JSON代码示例和功能模块",
    },
    "SU023_ss_island_005.jpg": {
        "has_concrete_knowledge": False,
        "expected_type": "UI/终端",
        "description": "Claude Code终端界面截图，纯操作界面无具象知识内容",
    },
}

# ---------------------------------------------------------------------------
# 生产级 Vision AI prompt（与 user.md 一致）
# ---------------------------------------------------------------------------
VISION_PROMPT = """# 角色
你是专业的教育多媒体分析专家，精通教学知识分类，严格按照「具象性知识」的学术定义判定。

# 核心判定规则
判定当前截图是否包含**具象性知识**：

**阳性（存在具象性知识）** - 满足任一条件：
- 截图中存在和教学知识点强相关的：实物照片、标本图、实验装置、解剖图、结构图、地图、实操界面、具体事物示意图，抽象框图、逻辑流程图、思维导图等
- 截图中存在**数学公式**（包括方程式、推导过程、符号表达式等）
- 该图形是用于讲解知识点的功能性元素，非装饰、水印、花边、无关插画
- 图形能明确对应现实中的具体事物、现象、操作步骤、数学关系
- 截图是否存在图表显示数据或者变化
- 截图是否能帮助初学者直观认知某个事物的视觉形式
- 截图是否能作为记忆点，用于学习者后续回顾和复习
**阴性（不存在具象性知识）**：
- 纯文字、无功能性图形
- 仅有装饰图片，无教学用具象图形或数学公式
- 仅仅是讲解者的人物图片

**混合型**：同时包含具象知识和抽象知识，仍标记为「存在具象性知识」

# 图像描述要求 (Image Description)
1. **可见文字**：如果文字很多（超过100字）则不需要全部列出，只需要输出概括性几句话的文字描述。
 - 必须输出正在执行的命令
 - 必须输出所有可见的代码和代码对应的注释和解释
 - 输出光标对应的段落，以及可见的标题、粗体、斜体、高亮。
 - 必须输出所有数学公式。
 - 输出配置的项、值和选项。
 - 必须输出该截图中字幕对应的文字。
2. **视觉元素**：显著的UI组件（按钮、图标、图表）、布局结构、所在是什么软件、命令行、终端、IDE、浏览器还是什么。
3. **动作/状态**：正在发生的操作（如"鼠标悬停在XX"，"终端显示报错"）。
4. **核心焦点**：输出该截图的操作中目前正在进行的操作和其关注的焦点。

# 输出要求（严格JSON格式）
{
    "has_concrete_knowledge": "是/否",
    "confidence": 0.0-1.0,
    "img_description": "详细的中文图像描述，包含可见文字、视觉元素与动作状态"
}

img_description禁止输出该图片中无关焦点的背景信息。（如背景为蓝天和海边的风景图。）
img_description禁止输出判定has_concrete_knowledge的理由
img_description禁止输出非核心焦点相关的可见文字
请只输出JSON，不要有其他内容。"""


# =====================================================================
# 数据结构
# =====================================================================
@dataclass
class ImageTestResult:
    """单张图片单个方案的结果"""
    image: str = ""
    method: str = ""
    has_concrete_knowledge: Optional[bool] = None
    confidence: float = 0.0
    img_description: str = ""
    elapsed_ms: float = 0.0
    ok: bool = False
    error: str = ""
    raw_output: str = ""


# =====================================================================
# 方案 A：云端 Vision AI (baseline)
# =====================================================================
async def run_vision_ai_test(image_paths: List[Path]) -> List[ImageTestResult]:
    """使用生产 VisionAIClient 调用云端大模型"""
    results: List[ImageTestResult] = []
    try:
        from services.python_grpc.src.content_pipeline.infra.llm.vision_ai_client import (
            VisionAIClient,
            VisionAIConfig,
        )
        from services.python_grpc.src.config_paths import load_yaml_dict, resolve_video_config_path

        config_path = resolve_video_config_path(anchor_file=__file__)
        if not config_path or not config_path.exists():
            return [ImageTestResult(image=str(p), method="vision_ai", error="config not found") for p in image_paths]

        raw = load_yaml_dict(config_path).get("vision_ai", {})
        if not raw.get("enabled"):
            return [ImageTestResult(image=str(p), method="vision_ai", error="vision_ai disabled") for p in image_paths]

        batch_cfg = raw.get("batch", {}) if isinstance(raw.get("batch"), dict) else {}
        cfg = VisionAIConfig(
            enabled=True,
            bearer_token=str(raw.get("bearer_token", "")),
            base_url=str(raw.get("base_url", "")),
            model=str(raw.get("model", "ernie-4.5-turbo-vl-32k")),
            temperature=float(raw.get("temperature", 0.1)),
            timeout=float(raw.get("timeout", raw.get("timeout_seconds", 60.0))),
            rate_limit_per_minute=int(raw.get("rate_limit_per_minute", 60)),
            duplicate_detection_enabled=False,
            batch_enabled=bool(batch_cfg.get("enabled", False)),
            batch_max_size=int(batch_cfg.get("max_size", 4)),
            batch_flush_ms=int(batch_cfg.get("flush_ms", 20)),
            batch_max_inflight_batches=int(batch_cfg.get("max_inflight_batches", 2)),
        )

        client = VisionAIClient(cfg)
        try:
            for path in image_paths:
                t0 = time.perf_counter()
                resp = await client.validate_image(
                    image_path=str(path),
                    prompt=VISION_PROMPT,
                    skip_duplicate_check=True,
                )
                elapsed_ms = (time.perf_counter() - t0) * 1000.0

                r = ImageTestResult(image=path.name, method="vision_ai", elapsed_ms=round(elapsed_ms, 2))
                if "error" in resp:
                    r.error = str(resp["error"])
                else:
                    r.ok = True
                    # 处理可能的嵌套格式：{"raw_response": "{...json...}", "should_include": true}
                    parsed = resp
                    if "raw_response" in resp and "has_concrete_knowledge" not in resp:
                        inner = _try_parse_json(str(resp["raw_response"]))
                        if inner:
                            parsed = inner
                    hck = parsed.get("has_concrete_knowledge", "")
                    r.has_concrete_knowledge = hck in ("是", True, "true", "True")
                    r.confidence = float(parsed.get("confidence", 0.0))
                    r.img_description = str(parsed.get("img_description", ""))
                    r.raw_output = json.dumps(resp, ensure_ascii=False)[:800]
                results.append(r)
        finally:
            await client.close()
    except Exception as exc:
        for p in image_paths:
            if not any(r.image == p.name for r in results):
                results.append(ImageTestResult(image=p.name, method="vision_ai", error=f"{type(exc).__name__}: {exc}"))
    return results


# =====================================================================
# 方案 B：RapidOCR + YOLOv8n（决策文档方案）
# =====================================================================
def _check_small_model_deps() -> Tuple[bool, str]:
    """检查 RapidOCR 和 ultralytics 是否可用"""
    missing = []
    try:
        from rapidocr_onnxruntime import RapidOCR  # noqa: F401
    except ImportError:
        missing.append("rapidocr_onnxruntime")
    try:
        from ultralytics import YOLO  # noqa: F401
    except ImportError:
        missing.append("ultralytics")
    if missing:
        return False, f"缺少依赖: {', '.join(missing)}。请运行: pip install {' '.join(missing)}"
    return True, ""


# ---------------------------------------------------------------------------
# 文本分类器 — 将 OCR 文字分为四类
# ---------------------------------------------------------------------------
import re as _re

# 数学公式特征（排除日期中的 - /）
_FORMULA_PATTERNS = [
    _re.compile(r"[=+×÷^√π∫∑∏≥≤]"),           # 数学运算符
    _re.compile(r"\b(lim|sin|cos|tan|log|ln)\b", _re.I),  # 数学函数
    _re.compile(r"d[xy]/d[xy]"),                # 微积分
    _re.compile(r"\b\d+\s*[\+\-\*\/\^]\s*\d+"),  # 数字运算表达式 (如 2+3, x^2)
]

# 代码特征
_CODE_PATTERNS = [
    _re.compile(r'^\s*(def |class |import |from .+ import|function |const |var |let |return )'),
    _re.compile(r'^\s*(if\s*\(|for\s*\(|while\s*\(|switch\s*\()'),
    _re.compile(r'["\']?\w+["\']?\s*[:=]\s*["\'\[\{]'),   # key: "val" / key = [...]
    _re.compile(r'\{\s*["\']?\w+["\']?\s*:'),              # JSON对象 {"name":
    _re.compile(r'^\s*[\[\{]'),                             # 行首 [ 或 {
    _re.compile(r'print\s*\(|console\.\w+\(|System\.out'),  # 打印语句
    _re.compile(r'#include|//|/\*|\*/'),                     # C系注释
]

# 命令/终端特征
_CMD_PATTERNS = [
    _re.compile(r'^\s*[\$#>]\s'),                            # $ / # / > 开头
    _re.compile(r'\b(sudo|pip|npm|yarn|docker|git|conda|wget|curl|apt|brew|yum)\b', _re.I),
    _re.compile(r'\b(cd|ls|mkdir|rm|cp|mv|cat|echo|chmod|chown)\b'),
    _re.compile(r'^\s*/\w+'),                                # /path 或 /command
]


def _classify_text(text: str) -> str:
    """将单行文字分类为: formula / code / command / text"""
    t = text.strip()
    if not t:
        return "text"
    # 公式判定：含2个以上数学特征
    formula_hits = sum(1 for p in _FORMULA_PATTERNS if p.search(t))
    if formula_hits >= 2:
        return "formula"
    # 代码判定
    if any(p.search(t) for p in _CODE_PATTERNS):
        return "code"
    # 命令判定
    if any(p.search(t) for p in _CMD_PATTERNS):
        return "command"
    return "text"


def _judge_concrete_knowledge(ocr_texts: List[str], visual_elements: List[str]) -> Tuple[bool, float]:
    """
    改进版规则引擎：基于 OCR 分类结果 + YOLO 视觉元素判定。
    返回 (has_concrete, confidence, hit_rules: List[str])
    """
    # — 先对 OCR 文字分类 —
    categories = {"formula": [], "code": [], "command": [], "text": []}
    for t in ocr_texts:
        cat = _classify_text(t)
        categories[cat].append(t)

    ocr_text_lower = " ".join(ocr_texts).lower()
    confidence = 0.0
    has_concrete = False
    hit_rules: List[str] = []

    # 规则1：存在数学公式
    if len(categories["formula"]) >= 1:
        has_concrete = True
        confidence += 0.35
        hit_rules.append(f"R1:数学公式×{len(categories['formula'])}")

    # 规则2：YOLO 功能性元素（去掉 tv 避免终端误判）
    functional_keywords = {"chart", "graph", "diagram", "flowchart", "map",
                           "laptop", "monitor", "cell phone", "keyboard", "mouse"}
    functional_hits = [e for e in visual_elements if e.lower() in functional_keywords]
    if functional_hits:
        has_concrete = True
        confidence += 0.3
        hit_rules.append(f"R2:功能图形={','.join(functional_hits)}")

    # 规则3：存在代码
    if len(categories["code"]) >= 1:
        has_concrete = True
        confidence += 0.25
        hit_rules.append(f"R3:代码×{len(categories['code'])}")

    # 规则4：存在命令/终端
    if len(categories["command"]) >= 1:
        has_concrete = True
        confidence += 0.2
        hit_rules.append(f"R4:命令×{len(categories['command'])}")

    # 规则5：结构化数据（表格分隔符）
    pipe_count = ocr_text_lower.count("|")
    tab_count = ocr_text_lower.count("\t")
    if pipe_count >= 3 or tab_count >= 3:
        has_concrete = True
        confidence += 0.15
        hit_rules.append(f"R5:结构化(|={pipe_count},tab={tab_count})")

    # 规则6：大量文字段落 → 可能是密集知识截图
    total_chars = len(ocr_text_lower)
    if total_chars > 300 and len(ocr_texts) > 20:
        if not has_concrete:
            has_concrete = True
            hit_rules.append(f"R6:密集文字({len(ocr_texts)}段,{total_chars}字符)")
        confidence += 0.15

    # 规则7：排除纯人物（但如果存在大量文字则不排除）
    person_count = visual_elements.count("person")
    total_objects = len(visual_elements)
    if person_count > 0 and person_count == total_objects:
        if not has_concrete and total_chars < 100:
            has_concrete = False
            confidence = 0.1
            hit_rules.append("R7:纯人物排除")
        elif has_concrete:
            hit_rules.append("R7:有person但文字丰富,保留判定")

    # 归一化
    confidence = min(confidence, 1.0)
    return has_concrete, round(confidence, 2)


def _generate_description(ocr_texts: List[str], visual_elements: List[str]) -> str:
    """基于 OCR 文字分类和 YOLO 检测结果生成结构化描述"""
    # 将文字分类
    categories: Dict[str, List[str]] = {"formula": [], "code": [], "command": [], "text": []}
    for t in ocr_texts:
        cat = _classify_text(t)
        categories[cat].append(t)

    sections = []

    # 数学公式
    if categories["formula"]:
        sections.append(f"【数学公式】共{len(categories['formula'])}项：\n" + "\n".join(categories["formula"]))

    # 代码
    if categories["code"]:
        sections.append(f"【代码】共{len(categories['code'])}行：\n" + "\n".join(categories["code"]))

    # 命令
    if categories["command"]:
        sections.append(f"【命令】共{len(categories['command'])}条：\n" + "\n".join(categories["command"]))

    # 普通文字
    if categories["text"]:
        sections.append(f"【文字】共{len(categories['text'])}段：\n" + "\n".join(categories["text"]))

    # 视觉元素
    if visual_elements:
        from collections import Counter
        counts = Counter(visual_elements)
        elem_desc = "、".join(f"{name}×{cnt}" if cnt > 1 else name for name, cnt in counts.most_common())
        sections.append(f"【视觉元素】{elem_desc}")

    if not sections:
        sections.append("未检测到显著文字或视觉元素")

    return "\n\n".join(sections)


def run_small_model_test(image_paths: List[Path]) -> List[ImageTestResult]:
    """方案 B：RapidOCR + YOLOv8n 本地小模型"""
    results: List[ImageTestResult] = []

    avail, msg = _check_small_model_deps()
    if not avail:
        return [ImageTestResult(image=p.name, method="small_model", error=msg) for p in image_paths]

    from rapidocr_onnxruntime import RapidOCR
    from ultralytics import YOLO

    print("[方案B] 正在加载 RapidOCR...")
    t_load = time.perf_counter()
    ocr_engine = RapidOCR()
    load_ocr_ms = round((time.perf_counter() - t_load) * 1000.0, 2)
    print(f"[方案B] RapidOCR 加载完成: {load_ocr_ms:.0f}ms")

    print("[方案B] 正在加载 YOLOv8n 模型...")
    t_load = time.perf_counter()
    yolo_model = YOLO("yolov8n.pt")
    load_yolo_ms = round((time.perf_counter() - t_load) * 1000.0, 2)
    print(f"[方案B] YOLOv8n 加载完成: {load_yolo_ms:.0f}ms")

    import cv2
    import traceback

    for path in image_paths:
        r = ImageTestResult(image=path.name, method="small_model")
        t0 = time.perf_counter()
        try:
            # 读取图片
            img = cv2.imread(str(path))

            # RapidOCR 文字提取 — 返回 ([[bbox, text, conf], ...], elapse)
            ocr_result, _ = ocr_engine(img)
            ocr_texts = [item[1] for item in ocr_result] if ocr_result else []

            # YOLO 目标检测
            yolo_results = yolo_model(img, verbose=False)
            visual_elements = []
            for box in yolo_results[0].boxes:
                cls_id = int(box.cls[0]) if len(box.cls) > 0 else 0
                visual_elements.append(yolo_model.names[cls_id])

            # 规则引擎判定
            has_concrete, confidence = _judge_concrete_knowledge(ocr_texts, visual_elements)
            description = _generate_description(ocr_texts, visual_elements)

            r.has_concrete_knowledge = has_concrete
            r.confidence = confidence
            r.img_description = description
            r.ok = True
            r.raw_output = json.dumps({
                "ocr_text_count": len(ocr_texts),
                "visual_elements": visual_elements,
                "ocr_sample": ocr_texts[:10],
            }, ensure_ascii=False)

        except Exception as exc:
            r.error = f"{type(exc).__name__}: {exc}"
            traceback.print_exc()

        r.elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        results.append(r)

    return results


# =====================================================================
# 方案 C：MiniCPM-V4.5 via HTTP
# =====================================================================
def run_minicpm_test(image_paths: List[Path]) -> List[ImageTestResult]:
    """方案 C：MiniCPM-V4.5 本地模型 (Gradio Server API)"""
    import httpx

    server_url = os.getenv("MINICPM_SERVER_URL", "http://127.0.0.1:9999/api").strip()
    health_url = server_url[:-4] if server_url.endswith("/api") else server_url
    results: List[ImageTestResult] = []

    try:
        with httpx.Client(timeout=httpx.Timeout(60.0, connect=5.0)) as cli:
            # 健康检查
            try:
                hr = cli.get(health_url)
                if hr.status_code >= 500:
                    return [ImageTestResult(image=p.name, method="minicpm",
                                           error=f"MiniCPM HTTP {hr.status_code}") for p in image_paths]
            except Exception as exc:
                return [ImageTestResult(image=p.name, method="minicpm",
                                       error=f"MiniCPM 服务不可达: {type(exc).__name__}: {exc}") for p in image_paths]

            for path in image_paths:
                r = ImageTestResult(image=path.name, method="minicpm")
                t0 = time.perf_counter()
                try:
                    with path.open("rb") as f:
                        image_b64 = base64.b64encode(f.read()).decode("utf-8")

                    question = json.dumps([{
                        "role": "user",
                        "content": [
                            {"type": "text", "pairs": VISION_PROMPT},
                            {"type": "image", "pairs": image_b64},
                        ],
                    }], ensure_ascii=False)

                    params = json.dumps({
                        "max_new_tokens": 512,
                        "temperature": 0.1,
                        "enable_thinking": False,
                        "stream": False,
                    }, ensure_ascii=False)

                    payload = {"image": "", "question": question, "params": params, "temporal_ids": None}
                    resp = cli.post(server_url, json=payload)

                    if resp.status_code != 200:
                        r.error = f"HTTP {resp.status_code}: {resp.text[:200]}"
                    else:
                        data = resp.json()
                        body = data.get("data", {}) if isinstance(data, dict) else {}
                        result_text = str(body.get("result", ""))
                        r.raw_output = result_text[:800]

                        # 尝试解析 JSON 输出
                        parsed = _try_parse_json(result_text)
                        if parsed:
                            hck = parsed.get("has_concrete_knowledge", "")
                            r.has_concrete_knowledge = hck in ("是", True, "true", "True")
                            r.confidence = float(parsed.get("confidence", 0.0))
                            r.img_description = str(parsed.get("img_description", ""))
                            r.ok = True
                        else:
                            r.img_description = result_text[:300]
                            r.ok = True  # 返回了内容但 JSON 解析失败
                            r.error = "JSON parse failed, raw text stored in img_description"

                except Exception as exc:
                    r.error = f"{type(exc).__name__}: {exc}"

                r.elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
                results.append(r)
    except Exception as exc:
        for p in image_paths:
            if not any(r.image == p.name for r in results):
                results.append(ImageTestResult(image=p.name, method="minicpm", error=f"{type(exc).__name__}: {exc}"))

    return results


# =====================================================================
# 方案 D：DocLayout-YOLO + RapidOCR（区域检测 + 分区域 OCR）
# =====================================================================
# DocLayout-YOLO 10 个类别
DOCLAYOUT_CLASSES = {
    0: "title", 1: "text", 2: "abandon", 3: "figure",
    4: "figure_caption", 5: "table", 6: "table_caption",
    7: "header", 8: "footer", 9: "equation",
}
# 具象知识对应的布局类别
CONCRETE_CATEGORIES = {"table", "equation", "figure", "code"}


def _check_doclayout_deps() -> Tuple[bool, str]:
    """检查 DocLayout-YOLO 和 RapidOCR 是否可用"""
    missing = []
    try:
        from doclayout_yolo import YOLOv10  # noqa: F401
    except ImportError:
        missing.append("doclayout-yolo")
    try:
        from rapidocr_onnxruntime import RapidOCR  # noqa: F401
    except ImportError:
        missing.append("rapidocr_onnxruntime")
    if missing:
        return False, f"缺少依赖: {', '.join(missing)}。请运行: pip install {' '.join(missing)}"
    return True, ""


def run_doclayout_test(image_paths: List[Path]) -> List[ImageTestResult]:
    """方案 D：DocLayout-YOLO 区域检测 + RapidOCR 分区域 OCR"""
    results: List[ImageTestResult] = []

    avail, msg = _check_doclayout_deps()
    if not avail:
        return [ImageTestResult(image=p.name, method="doclayout", error=msg) for p in image_paths]

    from doclayout_yolo import YOLOv10
    from rapidocr_onnxruntime import RapidOCR
    import cv2
    import traceback

    # 加载模型
    print("[方案D] 正在加载 DocLayout-YOLO 模型（首次会从 HuggingFace 下载）...")
    t_load = time.perf_counter()
    try:
        from huggingface_hub import hf_hub_download
        model_path = hf_hub_download(
            repo_id="juliozhao/DocLayout-YOLO-DocStructBench",
            filename="doclayout_yolo_docstructbench_imgsz1024.pt"
        )
        layout_model = YOLOv10(model_path)
    except Exception as exc:
        import traceback
        traceback.print_exc()
        return [ImageTestResult(image=p.name, method="doclayout",
                               error=f"模型加载失败: {exc}") for p in image_paths]
    load_layout_ms = round((time.perf_counter() - t_load) * 1000.0, 2)
    print(f"[方案D] DocLayout-YOLO 加载完成: {load_layout_ms:.0f}ms")

    print("[方案D] 正在加载 RapidOCR...")
    t_load = time.perf_counter()
    ocr_engine = RapidOCR()
    load_ocr_ms = round((time.perf_counter() - t_load) * 1000.0, 2)
    print(f"[方案D] RapidOCR 加载完成: {load_ocr_ms:.0f}ms")

    for path in image_paths:
        r = ImageTestResult(image=path.name, method="doclayout")
        t0 = time.perf_counter()
        try:
            img = cv2.imread(str(path))
            h, w = img.shape[:2]

            # DocLayout-YOLO 区域检测
            det_res = layout_model.predict(
                str(path), imgsz=1024, conf=0.2, device="cpu"
            )

            # 解析检测结果
            regions: Dict[str, List[Dict[str, Any]]] = {}  # category -> [{"bbox": ..., "conf": ...}]
            boxes = det_res[0].boxes
            names = det_res[0].names if hasattr(det_res[0], 'names') else DOCLAYOUT_CLASSES

            for box in boxes:
                cls_id = int(box.cls.item()) if hasattr(box.cls, 'item') else int(box.cls[0])
                cat_name = names.get(cls_id, f"unknown_{cls_id}") if isinstance(names, dict) else str(names[cls_id])
                conf = float(box.conf.item()) if hasattr(box.conf, 'item') else float(box.conf[0])
                xyxy = box.xyxy[0].tolist()  # [x1, y1, x2, y2]
                if cat_name not in regions:
                    regions[cat_name] = []
                regions[cat_name].append({"bbox": xyxy, "conf": conf})

            # 对每个区域裁切并 OCR
            categorized_text: Dict[str, List[str]] = {}
            for cat_name, boxes_list in regions.items():
                if cat_name in ("abandon", "header", "footer"):
                    continue  # 跳过无用区域
                texts = []
                for box_info in boxes_list:
                    x1, y1, x2, y2 = [int(v) for v in box_info["bbox"]]
                    # 裁切区域
                    x1, y1 = max(0, x1), max(0, y1)
                    x2, y2 = min(w, x2), min(h, y2)
                    if x2 - x1 < 10 or y2 - y1 < 10:
                        continue
                    crop = img[y1:y2, x1:x2]
                    ocr_result, _ = ocr_engine(crop)
                    if ocr_result:
                        texts.extend([item[1] for item in ocr_result])
                if texts:
                    categorized_text[cat_name] = texts

            # 判定具象知识
            has_concrete = False
            confidence = 0.0
            hit_categories = []

            for cat in regions:
                if cat in ("table", "equation", "figure"):
                    has_concrete = True
                    cat_conf = max(b["conf"] for b in regions[cat])
                    confidence = max(confidence, cat_conf)
                    hit_categories.append(f"{cat}×{len(regions[cat])}(conf={cat_conf:.2f})")
                # 检查 text/title 中是否有代码特征
                elif cat in ("text", "title") and cat in categorized_text:
                    code_lines = [t for t in categorized_text[cat] if _classify_text(t) in ("code", "formula")]
                    if len(code_lines) >= 2:
                        has_concrete = True
                        confidence = max(confidence, 0.6)
                        hit_categories.append(f"code_in_{cat}×{len(code_lines)}")

            if not has_concrete:
                confidence = 0.1

            # 构建描述
            desc_parts = []
            label_map = {
                "title": "标题", "text": "正文", "figure": "图表",
                "figure_caption": "图注", "table": "表格",
                "table_caption": "表注", "equation": "公式",
            }
            for cat_name, texts in categorized_text.items():
                label = label_map.get(cat_name, cat_name)
                desc_parts.append(f"【{label}】共{len(texts)}段：\n" + "\n".join(texts))

            # 区域汇总
            region_summary = ", ".join(
                f"{cat}×{len(boxes_list)}" for cat, boxes_list in regions.items()
            )
            desc_parts.append(f"【检测区域】{region_summary}")

            if hit_categories:
                desc_parts.append(f"【命中规则】{', '.join(hit_categories)}")

            r.ok = True
            r.has_concrete_knowledge = has_concrete
            r.confidence = round(confidence, 2)
            r.img_description = "\n\n".join(desc_parts)
            r.raw_output = json.dumps({
                "regions": {k: len(v) for k, v in regions.items()},
                "categorized_text_counts": {k: len(v) for k, v in categorized_text.items()},
                "hit_categories": hit_categories,
            }, ensure_ascii=False)

        except Exception as exc:
            r.error = f"{type(exc).__name__}: {exc}"
            traceback.print_exc()

        r.elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        results.append(r)

    return results


    return results


# =====================================================================
# 方案 F：Florence-2 (Lightweight VLM)
# =====================================================================
async def run_florence2_test(image_paths: List[Path]) -> List[ImageTestResult]:
    """方案 F：Florence-2-Large + DeepSeek"""
    results: List[ImageTestResult] = []

    # Check deps
    try:
        import torch
        from transformers import AutoProcessor, AutoModelForCausalLM
        from PIL import Image
    except ImportError as e:
        return [ImageTestResult(image=p.name, method="florence2",
                               error=f"缺少依赖: {e} (transformers, torch, timm, einops)") for p in image_paths]

    print("[方案F] 正在加载 Florence-2-Large 模型 (首次需下载 ~1.5GB)...")
    t_load = time.perf_counter()
    try:
        device = "cuda" if torch.cuda.is_available() else "cpu"
        dtype = torch.float16 if torch.cuda.is_available() else torch.float32

        model = AutoModelForCausalLM.from_pretrained(
            "microsoft/Florence-2-large",
            torch_dtype=dtype,
            trust_remote_code=True
        ).to(device)
        processor = AutoProcessor.from_pretrained("microsoft/Florence-2-large", trust_remote_code=True)
    except Exception as exc:
        import traceback
        traceback.print_exc()
        return [ImageTestResult(image=p.name, method="florence2",
                               error=f"模型加载失败: {exc}") for p in image_paths]

    load_ms = round((time.perf_counter() - t_load) * 1000.0, 2)
    print(f"[方案F] 模型加载完成: {load_ms:.0f}ms (device={device})")

    for path in image_paths:
        r = ImageTestResult(image=path.name, method="florence2")
        t0 = time.perf_counter()
        try:
            image = Image.open(path).convert("RGB")
            
            # Task 1: Detailed Caption
            task_prompt = "<MORE_DETAILED_CAPTION>"
            inputs = processor(text=task_prompt, images=image, return_tensors="pt").to(device, dtype)
            
            generated_ids = model.generate(
                input_ids=inputs["input_ids"],
                pixel_values=inputs["pixel_values"],
                max_new_tokens=1024,
                do_sample=False,
                num_beams=3,
            )
            generated_text = processor.batch_decode(generated_ids, skip_special_tokens=False)[0]
            parsed = processor.post_process_generation(
                generated_text, task=task_prompt, image_size=(image.width, image.height)
            )
            description = parsed.get(task_prompt, "")
            
            r.img_description = description
            r.raw_output = str(parsed)

            # DeepSeek Judgment
            print(f"[DeepSeek] Analyzing: {path.name}...")
            is_concrete, reason = await assess_concrete_knowledge_with_llm(description)
            
            r.has_concrete_knowledge = is_concrete
            # Synthetic confidence based on positive/negative
            r.confidence = 0.9 if is_concrete else 0.8
            
            if is_concrete:
                r.img_description += f"\n\n[DeepSeek] Judgment: {is_concrete}\nReason: {reason}"
            else:
                r.img_description += f"\n\n[DeepSeek] Judgment: {is_concrete}\nReason: {reason}"
            
            r.ok = True

        except Exception as exc:
            r.error = f"{type(exc).__name__}: {exc}"
            import traceback
            traceback.print_exc()

        r.elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        results.append(r)

    return results




# =====================================================================
# 工具函数
# =====================================================================
def _try_parse_json(text: str) -> Optional[Dict[str, Any]]:
    """尝试从可能包含 markdown 代码块的文本中提取 JSON"""
    import re
    # 去掉 markdown 代码块
    text = text.strip()
    m = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", text, re.DOTALL)
    if m:
        text = m.group(1).strip()
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None


def _now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


# =====================================================================
# 结果汇总 & 输出
# =====================================================================
def print_comparison_table(
    vision_results: List[ImageTestResult],
    small_results: List[ImageTestResult],
    minicpm_results: List[ImageTestResult],
    doclayout_results: List[ImageTestResult] = None,
    florence_results: List[ImageTestResult] = None,
) -> None:
    """终端简洁表格（详细内容见 JSON 报告）"""
    print("\n" + "=" * 90)
    print("小模型替换大模型效果对比测试（详细描述见 JSON 报告）")
    print("=" * 90)

    all_lists = [vision_results, small_results, minicpm_results]
    if doclayout_results:
        all_lists.append(doclayout_results)
    if florence_results:
        all_lists.append(florence_results)
    all_images = sorted(set(r.image for rl in all_lists for r in rl))

    def _find(results: List[ImageTestResult], image: str) -> Optional[ImageTestResult]:
        for r in results:
            if r.image == image:
                return r
        return None

    for img in all_images:
        gt = EXPECTED_RESULTS.get(img, {})
        exp_hck = "是" if gt.get("has_concrete_knowledge") else "否"
        print(f"\n📸 {img}  预期={exp_hck}, 类型={gt.get('expected_type', '?')}")
        methods = [("A.VisionAI", vision_results),
                   ("B.OCR+YOLO", small_results),
                   ("C.MiniCPM ", minicpm_results)]
        if doclayout_results:
            methods.append(("D.DocLayout", doclayout_results))
        if florence_results:
            methods.append(("F.Florence2 ", florence_results))
        for label, res_list in methods:
            r = _find(res_list, img)
            if r is None:
                print(f"  {label} | 未运行")
                continue
            if r.error and not r.ok:
                print(f"  {label} | ERR: {r.error[:60]}")
                continue
            hck = "是" if r.has_concrete_knowledge else "否"
            match = "✅" if r.has_concrete_knowledge == gt.get("has_concrete_knowledge") else "❌"
            print(f"  {label} | {match} 具象={hck} 置信={r.confidence:.2f} 耗时={r.elapsed_ms:.0f}ms")

    print("\n" + "=" * 90)


def build_report(
    vision_results: List[ImageTestResult],
    small_results: List[ImageTestResult],
    minicpm_results: List[ImageTestResult],
    doclayout_results: List[ImageTestResult] = None,
    florence_results: List[ImageTestResult] = None,
) -> Dict[str, Any]:
    """构建完整 JSON 报告"""
    def _to_dicts(results: List[ImageTestResult]) -> List[Dict[str, Any]]:
        return [asdict(r) for r in results]

    # 构建详细对比表
    all_lists = [vision_results, small_results, minicpm_results]
    if doclayout_results:
        all_lists.append(doclayout_results)
    if florence_results:
        all_lists.append(florence_results)
    all_images = sorted(set(r.image for rl in all_lists for r in rl))

    comparison_table: List[Dict[str, Any]] = []
    for img in all_images:
        row: Dict[str, Any] = {
            "image": img,
            "expected": EXPECTED_RESULTS.get(img, {}),
        }
        methods = [("vision_ai", vision_results),
                   ("small_model", small_results),
                   ("minicpm", minicpm_results)]
        if doclayout_results:
            methods.append(("doclayout", doclayout_results))
        if florence_results:
            methods.append(("florence2", florence_results))
        for method_key, res_list in methods:
            r = next((x for x in res_list if x.image == img), None)
            if r:
                row[method_key] = {
                    "ok": r.ok,
                    "has_concrete_knowledge": r.has_concrete_knowledge,
                    "confidence": r.confidence,
                    "img_description": r.img_description,
                    "elapsed_ms": r.elapsed_ms,
                    "error": r.error,
                }
        comparison_table.append(row)

    # 统计准确率
    def _accuracy(results: List[ImageTestResult]) -> Dict[str, Any]:
        total = 0
        correct = 0
        for r in results:
            gt = EXPECTED_RESULTS.get(r.image, {})
            if r.ok and gt.get("has_concrete_knowledge") is not None and r.has_concrete_knowledge is not None:
                total += 1
                if r.has_concrete_knowledge == gt["has_concrete_knowledge"]:
                    correct += 1
        return {"total": total, "correct": correct, "accuracy": f"{correct}/{total}" if total > 0 else "N/A"}

    # 平均耗时
    def _avg_ms(results: List[ImageTestResult]) -> float:
        valid = [r.elapsed_ms for r in results if r.ok]
        return round(sum(valid) / len(valid), 2) if valid else 0.0

    report = {
        "test_time": _now(),
        "test_images": [str(p) for p in TEST_IMAGES],
        "expected_results": EXPECTED_RESULTS,
        "comparison_table": comparison_table,
        "results": {
            "vision_ai": _to_dicts(vision_results),
            "small_model": _to_dicts(small_results),
            "minicpm": _to_dicts(minicpm_results),
        },
        "summary": {
            "vision_ai": {"accuracy": _accuracy(vision_results), "avg_ms": _avg_ms(vision_results)},
            "small_model": {"accuracy": _accuracy(small_results), "avg_ms": _avg_ms(small_results)},
            "minicpm": {"accuracy": _accuracy(minicpm_results), "avg_ms": _avg_ms(minicpm_results)},
        },
    }
    if doclayout_results:
        report["results"]["doclayout"] = _to_dicts(doclayout_results)
        report["summary"]["doclayout"] = {"accuracy": _accuracy(doclayout_results), "avg_ms": _avg_ms(doclayout_results)}
    if florence_results:
        report["results"]["florence2"] = _to_dicts(florence_results)
        report["summary"]["florence2"] = {"accuracy": _accuracy(florence_results), "avg_ms": _avg_ms(florence_results)}
    return report


# =====================================================================
# Vision AI 缓存
# =====================================================================
CACHE_DIR = PROJECT_ROOT / "var" / "artifacts" / "benchmarks" / "small_model_replacement"
VISION_CACHE_FILE = CACHE_DIR / "_vision_ai_cache.json"


def _save_vision_cache(results: List[ImageTestResult]) -> None:
    """保存 Vision AI 结果到缓存文件"""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    data = [asdict(r) for r in results]
    VISION_CACHE_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_vision_cache() -> Optional[List[ImageTestResult]]:
    """从缓存加载 Vision AI 结果，并修复 raw_response 嵌套解析"""
    if not VISION_CACHE_FILE.exists():
        return None
    try:
        data = json.loads(VISION_CACHE_FILE.read_text(encoding="utf-8"))
        results = []
        for d in data:
            r = ImageTestResult()
            for k, v in d.items():
                if hasattr(r, k):
                    setattr(r, k, v)
            # 修复 raw_response 嵌套解析
            if r.ok and r.has_concrete_knowledge is not None:
                raw = r.raw_output
                if raw and "raw_response" in raw and r.img_description == "":
                    parsed_raw = _try_parse_json(raw)
                    if parsed_raw and "raw_response" in parsed_raw:
                        inner = _try_parse_json(str(parsed_raw["raw_response"]))
                        if inner:
                            hck = inner.get("has_concrete_knowledge", "")
                            r.has_concrete_knowledge = hck in ("是", True, "true", "True")
                            r.confidence = float(inner.get("confidence", r.confidence))
                            r.img_description = str(inner.get("img_description", ""))
            results.append(r)
        return results
    except Exception:
        return None


# =====================================================================
# main
# =====================================================================
async def main() -> None:
    use_cache = "--no-cache" not in sys.argv

    # 检查测试图片
    missing = [p for p in TEST_IMAGES if not p.exists()]
    if missing:
        print(f"[错误] 以下测试图片不存在:")
        for m in missing:
            print(f"  - {m}")
        sys.exit(1)

    print(f"[{_now()}] 开始测试，共 {len(TEST_IMAGES)} 张图片\n")

    # 方案 A：云端 Vision AI (Baseline)
    # print(f"[{_now()}] ═══ 方案 A：云端 Vision AI ═══")
    # vision_results = await run_vision_ai_test(TEST_IMAGES)
    vision_results = []

    # 方案 B：RapidOCR + YOLOv8n (Small Model)
    # print(f"[{_now()}] ═══ 方案 B：RapidOCR + YOLOv8n ═══")
    # small_results = run_small_model_test(TEST_IMAGES)
    small_results = []

    # 方案 C：MiniCPM-V4.5 (Medium Model)
    # print(f"[{_now()}] ═══ 方案 C：MiniCPM-V4.5 ═══")
    # minicpm_results = run_minicpm_test(TEST_IMAGES)
    minicpm_results = []

    # 方案 D：DocLayout-YOLO (Layout Analysis)
    # print(f"[{_now()}] ═══ 方案 D：DocLayout-YOLO + RapidOCR ═══")
    # doclayout_results = run_doclayout_test(TEST_IMAGES)
    doclayout_results = []

    # 方案 F：Florence-2 (Lightweight VLM)
    print(f"[{_now()}] ═══ 方案 F：Florence-2-Large + DeepSeek ═══")
    florence_results = await run_florence2_test(TEST_IMAGES)
    print(f"[{_now()}] 方案 F 完成: {sum(1 for r in florence_results if r.ok)}/{len(florence_results)} 成功\n")

    # ── 对比表格 ──
    print_comparison_table(vision_results, small_results, minicpm_results, doclayout_results, florence_results)

    # ── 保存报告 ──
    report = build_report(vision_results, small_results, minicpm_results, doclayout_results, florence_results)
    out_dir = PROJECT_ROOT / "var" / "artifacts" / "benchmarks" / "small_model_replacement"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    out_path = out_dir / f"comparison_{ts}.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[{_now()}] 报告已保存: {out_path}")

    # ── 简要总结 ──
    print("\n📊 总结:")
    methods = [("vision_ai", "A.云端VisionAI"), ("small_model", "B.RapidOCR+YOLO"),
               ("minicpm", "C.MiniCPM"), ("doclayout", "D.DocLayout"),
               ("florence2", "F.Florence-2")]
    for method, label in methods:
        if method in report["summary"]:
            s = report["summary"][method]
            print(f"  {label:16s} | 准确率={s['accuracy']['accuracy']}  平均耗时={s['avg_ms']}ms")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
