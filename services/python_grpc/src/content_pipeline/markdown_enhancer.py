"""
模块说明：Module2 内容增强中的 markdown_enhancer 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import os
import json
import logging
import re
import yaml
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
# 🚀 使用集中式 LLMClient (连接池+HTTP/2+自适应并发)
import asyncio

from services.python_grpc.src.common.utils.text_patch import (
    extract_first_json_dict,
    find_add_insert_positions,
    find_contextual_match_positions,
)
from services.python_grpc.src.common.utils.patch_protocol import (
    collect_patch_ops,
    normalize_replace_add_patch_item,
    pick_full_text_fallback,
)
from services.python_grpc.src.common.utils.deepseek_model_router import resolve_deepseek_model
from services.python_grpc.src.config_paths import resolve_video_config_path
from services.python_grpc.src.content_pipeline.infra.llm.prompt_loader import get_prompt
from services.python_grpc.src.content_pipeline.infra.llm.prompt_registry import PromptKeys


logger = logging.getLogger(__name__)


# ==============================================================================
# Prompt Templates
# ==============================================================================

HIERARCHY_PROMPT = """你是{subject}学科的资深教研员，擅长将零散的知识点构建为结构严谨的逻辑树（Logic Tree）。

## 任务
请根据以下语义单元标题，构建一个三级逻辑导航树。重点不仅仅是分类，而是体现“核心论点”与“支撑论据”的逻辑关系。

{titles}

## 层级定义（语义承载）
- **一级层级（核心论题 / 核心方法论）**: 
  - 承载：整个章节探讨的宏观主题、核心理论框架、或作为基石的关键定义。
  - 作用：作为父节点，统领其下的所有具体论点与步骤。
- **二级层级（关键论点 / 操作步骤 / 核心主张）**: 
  - 承载：具体的观点陈述、方法论的执行步骤、或对核心论题的关键拆解。这是最主要的层级。
  - 作用：构成逻辑树的主干。
- **三级层级（支撑细节 / 延伸讨论 / 补充说明）**: 
  - 承载：具体的解释、复杂案例的深层剖析、易错点提示、或非核心的延伸信息。
  - 作用：作为叶节点，为关键论点提供血肉。

## 逻辑构建要求
1. **聚合性**: 确保所有围绕同一个核心论题展开的关键论点（Level 2）都归属到同一个父节点（Level 1）下。
2. **支撑性**: 确保补充细节（Level 3）紧密依附于其服务的关键论点（Level 2）。
3. **连贯性**: 同级节点之间应具有逻辑上的并列或递进关系。

## 输出格式 (JSON)
{{
  "hierarchy": [
    {{"unit_id": "SU001", "level": 1, "parent_id": null}},
    {{"unit_id": "SU002", "level": 2, "parent_id": "SU001"}},
    {{"unit_id": "SU003", "level": 3, "parent_id": "SU002"}}
  ]
}}

请只输出 JSON。"""


TEXT_ENHANCE_PROMPT = """你是教育内容编辑专家，擅长将口语化的教学内容转化为清晰的书面表达。

## 任务
根据以下信息，补全和完善正文中的指代不明或语义不明问题:

### 原始正文 (口语转录)
{body_text}

### 截图 OCR 结果 (包含公式)
{ocr_text}

### 动作单元信息
{action_info}

## 要求
1. 保留原文核心内容，不添加原文没有的信息
2. 将"这个""它""那个"等指代词替换为具体概念
3. 将口语表达转为书面表达
4. 保留数学公式，用 LaTeX 格式表示
5. 严禁输出标题，严禁以 # 开头
6. 严禁保留原文中的广告口播、赞助商鸣谢、订阅请求以及无实质信息的寒暄/过渡语句/元评论。

## 输出格式
直接输出增强后的正文，不要添加任何解释。"""


LOGIC_EXTRACT_PROMPT = """你是教育逻辑分析专家，擅长挖掘教学口语中的显性/隐性逻辑关系。

## 任务
对以下层级划分后的教学文本，提取逻辑关系并用结构化格式表达。

### 标题
{title}

### 正文内容
{body_text}

### 层级信息
{level_info}

### 动作单元信息 (非讲解型)
{action_info}

## 要求
1. 分析逻辑关系类型：总分、并列、递进、因果、对比、验证
2. 使用 **语义标签+冒号** 体现逻辑关系
3. 通过 **4个空格缩进** 展现逻辑层次关系
4. 用 **-** 等 markdown 语法展现序列
5. 还原隐性逻辑
6. 将动作单元的 key_evidence 作为具体示例插入对应位置
7. 自动忽略所有广告、赞助、过渡性废话（Meta-talk）和非知识性的闲聊节点。只保留具备信息密度的教育/业务内容。

## 语义拆分与标签化要求（核心）
1. **强制拆解长段落**：所有超过 3 行的长段落，如果包含多个逻辑层，**必须**将其拆分为多个独立的子列表项。
2. **语义标签驱动**：每个拆分后的子项，必须以 **加粗语义标签+冒号** 开头。
3. **颗粒度控制**：每个子项只表述一个核心意思。

## ⚠️ 重要：不要直接写出逻辑关系描述
❌ 错误示例: `- **顺序查找的定义与特点**：总分关系（先总述定义与核心思想...）`
✅ 正确示例: `- **顺序查找的定义与特点**：`

通过缩进和语义标签隐式表达逻辑，不要在内容中直接写"总分关系""因果关系"等描述词。

## 输出格式示例
- **核心知识点**：
	- 定义：...的含义
	- 实现方式：
		- **方式A**：描述
			- 适用场景：...
		- **方式B**：描述
	- 效率分析：
		- 成功情况：...
		- 失败情况：...

请直接输出结构化的 Markdown 内容，不要包含逻辑关系的显式描述。"""


# ==============================================================================
# 🚀 LLM 调用合并：一次请求完成「正文增强 + 逻辑结构化」（参考：LLM调用优化.md「批量请求合并」）
# ==============================================================================

COMBINED_SYSTEM_PROMPT = """你是教育内容编辑专家 + 教育逻辑分析专家。

你的任务是对给定的教学文本执行两步：
1) 正文增强：补全指代不明、修正口语化表达，但**不得**添加原文不存在的信息；保留数学公式，使用 LaTeX。
2) 逻辑结构化：基于增强后的正文提取逻辑层次，用结构化 Markdown 表达。

## 逻辑结构化要求
- 使用 **语义标签+冒号** 体现逻辑关系
- 通过 **4个空格缩进** 展现逻辑层次
- 使用 **-** 等 Markdown 语法展现序列
- 还原隐性逻辑，使结构线性化
- 不要在内容中直接写“总分关系/因果关系”等显式描述词
- 将动作单元的 key_evidence 作为具体示例插入对应位置
- 严禁输出标题，严禁以 # 开头，直接输出正文

## 内容过滤要求 (CRITICAL)
必须无情地剔除以下内容，不要将其包含在 output 中：
1. **商业推广**：广告、赞助商信息（如 DX, Vercel, DataDog, Eppo 等）、订阅号召、产品植入。
2. **元评论 (Meta-commentary)**：对对话流程的描述、过渡语、预告（例如“主持人转向下一个话题”、“在介绍完背景后...”、“为了平滑过渡...”）。我们只需要核心知识内容。
3. **无效寒暄**：开场白、客套话、互相吹捧。
4. **互动指引**：点击链接、点赞关注等。

## 语义拆分与标签化要求（核心）
1. **强制拆解长段落**：所有超过 3 行的长段落，如果包含多个逻辑层（尤其是包含“例如、但是、因此”等连接词），**必须**将其拆分为多个独立的子列表项，禁止堆砌在一起。
2. **语义标签驱动**：每个拆分后的子项，必须以 **加粗语义标签+冒号** 开头（例如：**- **核心定义**：...**）。
3. **颗粒度控制**：每个子项只表述一个核心意思。如果一个意思包含多个步骤或方面，请进一步使用子列表展示。

## 输出格式（JSON）
{
  "enhanced_body": "...",
  "structured_content": "..."
}

只输出 JSON，不要输出解释、不要输出代码块标记。"""

COMBINED_USER_PROMPT = """### 标题
{title}

### 层级信息
{level_info}

### 原始正文 (口语转录)
{body_text}

### 截图 OCR 结果 (包含公式)
{ocr_text}

### 动作单元信息
{action_info}
"""


STRUCTURED_TEXT_SYSTEM_PROMPT = """你是教学内容结构化助手。
请将给定语义单元整理为清晰的 Markdown 文本，适配 Obsidian 知识笔记。

要求：
1) 仅基于给定文本改写，不补充外部事实。
2) 直接输出 Markdown，不输出 JSON 或代码块。
3) 如果给出图片候选，请在对应句子的句末插入占位符，格式必须为【imgneeded_{{img_id}}】。
 - 匹配的方式是根据提供的图片描述与句子描述的匹配程度。
 - 如果图片描述与句子描述不匹配，则不使用。
 - 如果多张图片匹配一句话可以在句子的句末插入多张图片的占位符。
 - 如果图片候选为空（例如 `(none)`），严禁输出任何 `imgneeded` 占位符。
4) 禁止使用其他占位符格式（例如 [IMG:img_id]、{IMG=img_id} 等）。
5) 叙事衔接：多个列表项如果来自同一段连续话语，必须保留叙事连贯感，使用简短衔接句承接上一项，禁止百科词条式罗列。
6) 论证嵌套：对同一论点的原因、后果、程度、举例、隐喻，禁止与论点并列展开，必须缩进嵌套在父观点之下。如果列表项只有一句核心描述，应将描述直接写在标签冒号之后。
"""

STRUCTURED_TEXT_USER_PROMPT = """## 语义单元
- 标题: {title}
- 知识类型: {knowledge_type}

## 话题上下文（用于生成过渡）
{adjacent_context}

## 原始文本
{body_text}

## 图片候选（可为空）
{image_context}

请输出结构化 Markdown；若有图片候选，请根据图片描述把对应图片插入到匹配句子的末尾，
占位符必须使用【imgneeded_{{img_id}}】。
若图片候选为空（例如 `(none)`），不要输出任何 `imgneeded` 占位符。"""


IMG_DESC_AUGMENT_SYSTEM_PROMPT = """你是教学文本补全助手。
请根据图片描述中可见的代码、命令、配置项、参数名、按钮文案等信息，对原始语义单元文本做“增量补全”。

要求：
1) 仅补全原文已提及但表达不完整的信息，不得添加图片和原文都没有的新事实。
2) 不删除原文关键步骤与语义，仅在必要处补充细节。
3) 优先补全实操关键信息（代码、命令、配置路径、参数、关键术语）。
4) 若图片信息与文本无明显关联，保持原文不变。
5) 输出必须是 JSON，且字段名使用短键（压缩输出）。
6) 增量补全仅支持两种操作模式：
   - replace：替换原文中某个已有片段
   - add：在定位点附近补充新增片段
7) 定位必须稳定，优先使用最短 left/right 上下文；若无法唯一定位则不要输出该操作（宁缺毋滥）。"""


IMG_DESC_AUGMENT_USER_PROMPT = """## 原始语义单元文本
{body_text}

## 图片证据（按时间/句子对齐）
{image_evidence}

## 输出格式（仅 JSON，字段名必须短键）
{{
  "p": [
    {{
      "m": "r",
      "o": "被替换片段",
      "n": "替换后片段",
      "l": "左上下文",
      "r": "右上下文"
    }},
    {{
      "m": "a",
      "n": "新增补充片段",
      "l": "左锚点上下文",
      "r": "右锚点上下文",
      "p": "after"
    }}
  ]
}}

说明：
- m: 模式，r=replace，a=add
- p（操作内）: 插入方向，仅在 add 时使用，取值 before/after（默认 after）
- 若无可补全内容，输出 {{"p": []}}
- 不要输出解释文本，不要输出 Markdown 代码块。"""
# ==============================================================================
# Data Classes
# ==============================================================================

@dataclass
class EnhancedSection:
    """
    类说明：封装 EnhancedSection 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    unit_id: str
    title: str
    knowledge_type: str = ""
    level: int = 2                          # 1/2/3
    parent_id: Optional[str] = None
    original_body: str = ""
    enhanced_body: str = ""
    structured_content: str = ""            # 逻辑结构化后的内容
    screenshots: List[str] = field(default_factory=list)
    screenshot_items: List[Dict[str, Any]] = field(default_factory=list)
    augment_screenshot_items: List[Dict[str, Any]] = field(default_factory=list)
    validated_screenshots: List[str] = field(default_factory=list)  # V2: 验证后的截图
    video_clip: str = ""                    # V2: 视频片段路径
    video_clips: List[str] = field(default_factory=list)
    action_classifications: List[Dict] = field(default_factory=list)
    mult_steps: bool = False
    tutorial_steps: List[Dict[str, Any]] = field(default_factory=list)
    group_id: int = 0
    group_name: str = ""
    group_reason: str = ""
    vl_concrete_segments: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class EnhancedGroup:
    """封装同一核心论点下的语义单元集合。"""
    group_id: int
    group_name: str
    reason: str = ""
    units: List[EnhancedSection] = field(default_factory=list)


# ==============================================================================
# Main Class
# ==============================================================================

class MarkdownEnhancer:
    """
    类说明：封装 MarkdownEnhancer 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    
    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        决策逻辑：
        - 条件：not self.api_key
        依据来源（证据链）：
        - 对象内部状态：self.api_key。
        输入参数：
        - api_key: 函数入参（类型：Optional[str]）。
        - base_url: 函数入参（类型：Optional[str]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        self.base_url = base_url or "https://api.deepseek.com"
        
        if not self.api_key:
            logger.warning("DEEPSEEK_API_KEY not set, enhancement will be disabled")
            self._enabled = False
            self._llm_client = None
        else:
            self._enabled = True
            # 🚀 使用集中式 LLMClient
            from services.python_grpc.src.content_pipeline.infra.llm.llm_client import LLMClient
            self._llm_client = LLMClient(
                api_key=self.api_key,
                base_url=self.base_url + "/v1"  # LLMClient 需要 /v1 后缀
            )

        # 统一走 V3.2 模型路由：默认沿用客户端模型，支持配置文件与环境变量覆盖。
        default_model = "deepseek-chat"
        if self._llm_client is not None:
            default_model = str(getattr(self._llm_client, "model", "") or "deepseek-chat")
        self._structured_text_model = self._load_deepseek_model(default_model=default_model)
        
        
        # V2: assets 目录 (用于 Obsidian 相对路径)
        self._assets_dir = "assets"
        # Obsidian 嵌入路径基准目录（默认使用输出 Markdown 所在目录）
        self._markdown_dir = None
        self._result_dir = None
        # 🚀 调用合并开关：默认开启，失败时自动回退到两次调用
        raw = (os.getenv("MODULE2_MARKDOWN_ENHANCER_COMBINE_CALLS", "1") or "").strip().lower()
        self._combine_llm_calls = raw in ("1", "true", "yes", "y", "on")
        raw_max_inflight = str(
            os.getenv("MODULE2_MARKDOWN_SECTION_MAX_INFLIGHT", "56") or "56"
        ).strip()
        try:
            self._section_max_inflight = max(1, int(raw_max_inflight))
        except Exception:
            self._section_max_inflight = 56

        # 实验开关：在结构化前，基于图片描述对正文做一次增量补全。
        # 优先读取 config.yaml；若环境变量显式设置则覆盖配置。
        self._enable_img_desc_text_augment = self._load_img_desc_augment_switch(default_value=True)

        # 可观测性：LLM 调用明细追踪（可配置 full/summary）
        trace_cfg = self._load_llm_trace_config(default_enabled=False)
        self._llm_trace_enabled = bool(trace_cfg.get("enabled", False))
        self._llm_trace_level = str(trace_cfg.get("level", "summary") or "summary").strip().lower()
        if self._llm_trace_level not in ("full", "summary"):
            self._llm_trace_level = "summary"
        self._llm_trace_output_path_cfg = str(trace_cfg.get("output_path", "") or "").strip()
        self._llm_trace_file_path = ""
        self._llm_trace_lock = asyncio.Lock()

        # 兼容保留：旧测试/调试路径可能直接调用 _classify_hierarchy，
        # 但增强主链路已切换为 knowledge_groups 输入，不再依赖该调用。
        self._hierarchy_prompt_template = HIERARCHY_PROMPT

        # 统一从 prompt_loader 读取 markdown_enhancer 提示词；缺失时回退到代码内默认值。
        self._text_enhance_prompt_template = get_prompt(
            PromptKeys.DEEPSEEK_MD_TEXT_ENHANCE,
            fallback=TEXT_ENHANCE_PROMPT,
        )
        self._logic_extract_prompt_template = get_prompt(
            PromptKeys.DEEPSEEK_MD_LOGIC_EXTRACT,
            fallback=LOGIC_EXTRACT_PROMPT,
        )
        self._combined_system_prompt = get_prompt(
            PromptKeys.DEEPSEEK_MD_COMBINED_SYSTEM,
            fallback=COMBINED_SYSTEM_PROMPT,
        )
        self._combined_user_prompt_template = get_prompt(
            PromptKeys.DEEPSEEK_MD_COMBINED_USER,
            fallback=COMBINED_USER_PROMPT,
        )
        self._structured_system_prompt = get_prompt(
            PromptKeys.DEEPSEEK_MD_STRUCTURED_SYSTEM,
            fallback=STRUCTURED_TEXT_SYSTEM_PROMPT,
        )
        self._structured_user_prompt_template = get_prompt(
            PromptKeys.DEEPSEEK_MD_STRUCTURED_USER,
            fallback=STRUCTURED_TEXT_USER_PROMPT,
        )
        self._structured_system_preserve_img_prompt = get_prompt(
            PromptKeys.DEEPSEEK_MD_STRUCTURED_SYSTEM_PRESERVE_IMG,
            fallback=self._structured_system_prompt,
        )
        self._structured_user_preserve_img_prompt_template = get_prompt(
            PromptKeys.DEEPSEEK_MD_STRUCTURED_USER_PRESERVE_IMG,
            fallback=self._structured_user_prompt_template,
        )
        self._img_desc_augment_system_prompt = get_prompt(
            PromptKeys.DEEPSEEK_MD_IMG_DESC_AUG_SYSTEM,
            fallback=IMG_DESC_AUGMENT_SYSTEM_PROMPT,
        )
        self._img_desc_augment_user_prompt_template = get_prompt(
            PromptKeys.DEEPSEEK_MD_IMG_DESC_AUG_USER,
            fallback=IMG_DESC_AUGMENT_USER_PROMPT,
        )

    @staticmethod
    def _parse_bool(value: Any, default: bool) -> bool:
        """统一解析布尔开关，兼容 bool/int/str。"""
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            raw = value.strip().lower()
            if raw in ("1", "true", "yes", "y", "on"):
                return True
            if raw in ("0", "false", "no", "n", "off"):
                return False
        return bool(default)

    @staticmethod
    def _normalize_path_key(path_text: str) -> str:
        raw_path = str(path_text or "").strip()
        if not raw_path:
            return ""
        try:
            return str(Path(raw_path).resolve())
        except Exception:
            return os.path.normcase(os.path.normpath(raw_path))

    def _is_screenshot_item_includable(self, item: Dict[str, Any]) -> bool:
        if not isinstance(item, dict):
            return False
        if "should_include" in item:
            return self._parse_bool(item.get("should_include"), True)
        if "should_included" in item:
            return self._parse_bool(item.get("should_included"), True)
        if "has_concrete_knowledge" in item:
            return self._parse_bool(item.get("has_concrete_knowledge"), True)
        if "has_concrete" in item:
            return self._parse_bool(item.get("has_concrete"), True)
        return True

    def _filter_screenshot_items(self, raw_items: Any) -> List[Dict[str, Any]]:
        if not isinstance(raw_items, list):
            return []
        filtered: List[Dict[str, Any]] = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            if not self._is_screenshot_item_includable(item):
                continue
            filtered.append(item)
        return filtered

    def _has_explicit_screenshot_exclusion(self, raw_items: Any) -> bool:
        if not isinstance(raw_items, list):
            return False
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            has_gate_field = any(
                field_name in item
                for field_name in ("should_include", "should_included", "has_concrete_knowledge", "has_concrete")
            )
            if has_gate_field and not self._is_screenshot_item_includable(item):
                return True
        return False

    def _filter_screenshot_paths(
        self,
        raw_paths: Any,
        screenshot_items: List[Dict[str, Any]],
        drop_when_items_empty: bool = False,
    ) -> List[str]:
        source_paths = raw_paths if isinstance(raw_paths, list) else []
        cleaned_source_paths = [str(path_item or "").strip() for path_item in source_paths if str(path_item or "").strip()]

        item_paths = [
            str(item.get("img_path") or item.get("path") or item.get("file_path") or "").strip()
            for item in screenshot_items
            if isinstance(item, dict)
        ]
        item_paths = [path_item for path_item in item_paths if path_item]

        if not item_paths:
            if drop_when_items_empty:
                return []
            return cleaned_source_paths

        allowed_keys = {self._normalize_path_key(path_item) for path_item in item_paths if self._normalize_path_key(path_item)}
        kept: List[str] = []
        seen_keys: set[str] = set()

        for path_item in cleaned_source_paths:
            path_key = self._normalize_path_key(path_item)
            if path_key and path_key in allowed_keys and path_key not in seen_keys:
                kept.append(path_item)
                seen_keys.add(path_key)

        for path_item in item_paths:
            path_key = self._normalize_path_key(path_item)
            if path_key and path_key not in seen_keys:
                kept.append(path_item)
                seen_keys.add(path_key)

        return kept

    def _resolve_config_path(self) -> Optional[Path]:
        """解析 config.yaml 路径：环境变量优先，其次项目默认路径。"""
        env_path = str(os.getenv("MODULE2_CONFIG_PATH", "") or "").strip()
        if env_path:
            candidate = Path(env_path)
            if candidate.exists():
                return candidate
            logger.warning(f"MODULE2_CONFIG_PATH not found: {candidate}")
        return resolve_video_config_path(anchor_file=__file__)

    def _load_img_desc_augment_switch(self, default_value: bool = True) -> bool:
        """加载“图片描述增量补全”开关：config.yaml 默认开启，环境变量可覆盖。"""
        enabled = bool(default_value)

        config_path = self._resolve_config_path()
        if config_path is not None:
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f) or {}
                content_pipeline_cfg = config.get("content_pipeline", {}) if isinstance(config, dict) else {}
                enhancer_cfg = (
                    content_pipeline_cfg.get("markdown_enhancer", {})
                    if isinstance(content_pipeline_cfg, dict)
                    else {}
                )
                cfg_value = enhancer_cfg.get("enable_img_desc_text_augment", enabled)
                enabled = self._parse_bool(cfg_value, enabled)
            except Exception as exc:
                logger.warning(f"Failed to load img-desc augment switch from config: {exc}")

        env_raw = os.getenv("MODULE2_ENABLE_IMG_DESC_TEXT_AUGMENT")
        if env_raw is not None and str(env_raw).strip() != "":
            enabled = self._parse_bool(env_raw, enabled)

        return enabled

    def _load_deepseek_model(self, default_model: str) -> str:
        """加载 DeepSeek 模型配置，并统一映射到 V3.2 chat/reasoner 官方模型名。"""
        model_name = resolve_deepseek_model(default_model, default_model="deepseek-chat")
        config_path = self._resolve_config_path()
        if config_path is not None:
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f) or {}
                if isinstance(config, dict):
                    analysis_cfg = config.get("ai", {}).get("analysis", {})
                    configured = str(analysis_cfg.get("model", "") or "").strip()
                    if configured:
                        model_name = resolve_deepseek_model(configured, default_model=model_name)
            except Exception as exc:
                logger.warning(f"Failed to load deepseek model from config: {exc}")

        env_override = str(os.getenv("MODULE2_MARKDOWN_ENHANCER_MODEL", "") or "").strip()
        if env_override:
            model_name = resolve_deepseek_model(env_override, default_model=model_name)
        return model_name

    def _load_llm_trace_config(self, default_enabled: bool = False) -> Dict[str, Any]:
        """加载 LLM trace 配置：config.yaml 为主，环境变量覆盖。"""
        config_value: Dict[str, Any] = {
            "enabled": bool(default_enabled),
            "level": "summary",
            "output_path": "",
        }

        config_path = self._resolve_config_path()
        if config_path is not None:
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f) or {}
                content_pipeline_cfg = config.get("content_pipeline", {}) if isinstance(config, dict) else {}
                observability_cfg = (
                    content_pipeline_cfg.get("observability", {})
                    if isinstance(content_pipeline_cfg, dict)
                    else {}
                )
                llm_cfg = observability_cfg.get("llm_trace", {}) if isinstance(observability_cfg, dict) else {}
                config_value["enabled"] = self._parse_bool(llm_cfg.get("enabled", config_value["enabled"]), config_value["enabled"])
                config_value["level"] = str(llm_cfg.get("level", config_value["level"]) or config_value["level"]).strip().lower()
                config_value["output_path"] = str(llm_cfg.get("output_path", config_value["output_path"]) or "").strip()
            except Exception as exc:
                logger.warning(f"Failed to load llm-trace config: {exc}")

        env_enabled = os.getenv("MODULE2_LLM_TRACE_ENABLED")
        if env_enabled is not None and str(env_enabled).strip() != "":
            config_value["enabled"] = self._parse_bool(env_enabled, bool(config_value["enabled"]))

        env_level = str(os.getenv("MODULE2_LLM_TRACE_LEVEL", "") or "").strip().lower()
        if env_level:
            config_value["level"] = env_level

        env_output = str(os.getenv("MODULE2_LLM_TRACE_OUTPUT_PATH", "") or "").strip()
        if env_output:
            config_value["output_path"] = env_output

        return config_value

    def _prepare_llm_trace_output(self) -> None:
        """初始化 LLM trace 输出文件。"""
        if not self._llm_trace_enabled:
            self._llm_trace_file_path = ""
            return

        result_dir = Path(self._result_dir or "").resolve() if self._result_dir else Path.cwd()
        if self._llm_trace_output_path_cfg:
            configured = Path(self._llm_trace_output_path_cfg)
            if configured.is_absolute():
                trace_path = configured
            else:
                trace_path = result_dir / configured
        else:
            trace_path = result_dir / "intermediates" / "phase2b_llm_trace.jsonl"

        trace_path.parent.mkdir(parents=True, exist_ok=True)
        with open(trace_path, "w", encoding="utf-8") as file_obj:
            file_obj.write("")
        self._llm_trace_file_path = str(trace_path)
        logger.info(f"LLM trace enabled: {self._llm_trace_file_path} (level={self._llm_trace_level})")

    @staticmethod
    def _build_text_preview(text: str, max_chars: int = 500) -> str:
        value = str(text or "")
        if len(value) <= max_chars:
            return value
        return value[:max_chars] + "...<truncated>"

    async def _write_llm_trace_record(
        self,
        *,
        step_name: str,
        unit_id: str,
        system_prompt: str,
        user_prompt: str,
        response_text: str,
        duration_ms: float,
        success: bool,
        error_msg: str = "",
        metadata: Optional[Any] = None,
    ) -> None:
        """落盘单条 LLM 调用记录。"""
        if not self._llm_trace_enabled or not self._llm_trace_file_path:
            return

        model_name = ""
        prompt_tokens = None
        completion_tokens = None
        total_tokens = None
        cache_hit = False
        if metadata is not None:
            model_name = str(getattr(metadata, "model", "") or "")
            prompt_tokens = getattr(metadata, "prompt_tokens", None)
            completion_tokens = getattr(metadata, "completion_tokens", None)
            total_tokens = getattr(metadata, "total_tokens", None)
            cache_hit = bool(getattr(metadata, "cache_hit", False))

        if not model_name and self._llm_client is not None:
            model_name = str(getattr(self._llm_client, "model", "") or "")

        prompt_for_dump = str(user_prompt or "")
        system_for_dump = str(system_prompt or "")
        response_for_dump = str(response_text or "")

        if self._llm_trace_level != "full":
            prompt_for_dump = self._build_text_preview(prompt_for_dump)
            system_for_dump = self._build_text_preview(system_for_dump)
            response_for_dump = self._build_text_preview(response_for_dump)

        record = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "step_name": step_name,
            "unit_id": unit_id,
            "model": model_name,
            "duration_ms": float(duration_ms),
            "success": bool(success),
            "error": str(error_msg or ""),
            "system_prompt": system_for_dump,
            "user_prompt": prompt_for_dump,
            "response_text": response_for_dump,
            "prompt_chars": len(str(user_prompt or "")),
            "response_chars": len(str(response_text or "")),
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
            "cache_hit": cache_hit,
        }

        async with self._llm_trace_lock:
            with open(self._llm_trace_file_path, "a", encoding="utf-8") as file_obj:
                file_obj.write(json.dumps(record, ensure_ascii=False) + "\n")

    async def _complete_text_with_model_fallback(
        self,
        *,
        prompt: str,
        system_message: str,
        model: str,
    ) -> Tuple[str, Any, Any]:
        """优先透传模型参数；若桩客户端不支持 `model` 关键字则自动回退。"""
        if self._llm_client is None:
            raise RuntimeError("LLM client is not initialized")
        try:
            return await self._llm_client.complete_text(
                prompt=prompt,
                system_message=system_message,
                model=model,
            )
        except TypeError as exc:
            # 兼容历史测试桩：旧签名不接收 model 参数。
            if "unexpected keyword argument 'model'" not in str(exc):
                raise
            return await self._llm_client.complete_text(
                prompt=prompt,
                system_message=system_message,
            )

    @staticmethod
    def _flatten_semantic_units_payload(payload: Any) -> List[Dict[str, Any]]:
        units: List[Dict[str, Any]] = []
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    units.append(item)
            return units

        if not isinstance(payload, dict):
            return units

        raw_groups = payload.get("knowledge_groups", [])
        if isinstance(raw_groups, list):
            for group in raw_groups:
                if not isinstance(group, dict):
                    continue
                group_units = group.get("units", [])
                if not isinstance(group_units, list):
                    continue
                for unit in group_units:
                    if isinstance(unit, dict):
                        units.append(unit)

        raw_units = payload.get("semantic_units", [])
        if isinstance(raw_units, list):
            for unit in raw_units:
                if isinstance(unit, dict):
                    units.append(unit)
        return units

    def _load_concrete_canonical_by_unit(self, result_dir: str) -> Dict[str, Dict[str, Any]]:
        """
        concrete 单元 canonical 源：优先来自 phase2a 语义文件，而不是 result.json 的 body_text。
        """
        if not result_dir:
            return {}

        base_dir = Path(result_dir)
        candidate_paths: List[Path] = [
            base_dir / "semantic_units_phase2a.json",
            base_dir / "intermediates" / "semantic_units_phase2a.json",
        ]
        rpc_candidates = sorted(
            (base_dir / "intermediates").glob("semantic_units_from_rpc_*.json"),
            key=lambda path_item: path_item.stat().st_mtime,
            reverse=True,
        ) if (base_dir / "intermediates").exists() else []
        if rpc_candidates:
            candidate_paths.append(rpc_candidates[0])

        canonical_by_unit: Dict[str, Dict[str, Any]] = {}
        for path in candidate_paths:
            if not path.exists() or not path.is_file():
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:
                logger.warning(f"Failed to load concrete canonical payload: path={path}, err={exc}")
                continue

            for unit in self._flatten_semantic_units_payload(payload):
                unit_id = str(unit.get("unit_id", "") or "").strip()
                if not unit_id:
                    continue
                knowledge_type = self._normalize_knowledge_type(unit.get("knowledge_type"))
                if knowledge_type != "concrete":
                    continue
                raw_segments = unit.get("_vl_concrete_segments", unit.get("vl_concrete_segments", []))
                vl_segments = raw_segments if isinstance(raw_segments, list) else []
                main_content_blocks: List[str] = []
                for segment in vl_segments:
                    if not isinstance(segment, dict):
                        continue
                    main_content = str(segment.get("main_content", "") or "").strip()
                    if main_content:
                        main_content_blocks.append(main_content)

                canonical_body = "\n\n".join(main_content_blocks).strip()
                if not canonical_body:
                    canonical_body = str(
                        unit.get("full_text")
                        or unit.get("text")
                        or unit.get("body_text")
                        or ""
                    ).strip()
                if not canonical_body:
                    continue
                canonical_by_unit[unit_id] = {
                    "body_text": canonical_body,
                    "vl_concrete_segments": list(vl_segments),
                    "source_path": str(path),
                }
            if canonical_by_unit:
                logger.info(
                    f"Concrete canonical payload loaded: path={path}, units={len(canonical_by_unit)}"
                )
                return canonical_by_unit
        return canonical_by_unit

    def _sync_concrete_canonical_into_result_payload(
        self,
        payload: Any,
        canonical_by_unit: Dict[str, Dict[str, Any]],
    ) -> bool:
        if not isinstance(payload, dict) or not canonical_by_unit:
            return False

        changed = False

        def _apply(unit_node: Any) -> bool:
            if not isinstance(unit_node, dict):
                return False
            unit_id = str(unit_node.get("unit_id", "") or "").strip()
            if not unit_id:
                return False
            if self._normalize_knowledge_type(unit_node.get("knowledge_type")) != "concrete":
                return False
            canonical = canonical_by_unit.get(unit_id, {})
            if not isinstance(canonical, dict) or not canonical:
                return False

            local_changed = False
            canonical_body = str(canonical.get("body_text", "") or "").strip()
            if canonical_body and str(unit_node.get("body_text", "") or "").strip() != canonical_body:
                unit_node["body_text"] = canonical_body
                local_changed = True

            canonical_segments = canonical.get("vl_concrete_segments")
            if isinstance(canonical_segments, list):
                current_segments = unit_node.get("_vl_concrete_segments")
                if current_segments != canonical_segments:
                    unit_node["_vl_concrete_segments"] = list(canonical_segments)
                    local_changed = True
            return local_changed

        raw_groups = payload.get("knowledge_groups", [])
        if isinstance(raw_groups, list):
            for group in raw_groups:
                if not isinstance(group, dict):
                    continue
                units = group.get("units", [])
                if not isinstance(units, list):
                    continue
                for unit in units:
                    if _apply(unit):
                        changed = True

        raw_sections = payload.get("sections", [])
        if isinstance(raw_sections, list):
            for section in raw_sections:
                if _apply(section):
                    changed = True

        return changed
    
    @property
    def enabled(self) -> bool:
        """
        执行逻辑：
        1) 读取对象内部状态。
        2) 返回属性值。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：对外提供统一读路径，便于维护与扩展。
        输入参数：
        - 无。
        输出参数：
        - 布尔判断结果。"""
        return self._enabled
    
    async def enhance(
        self,
        result_json_path: str,
        subject: str = "数据结构与算法",
        markdown_dir: Optional[str] = None
    ) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not sections
        依据来源（证据链）：
        输入参数：
        - result_json_path: 文件路径（类型：str）。
        - subject: 函数入参（类型：str）。
        输出参数：
        - 字符串结果。"""
        # 加载数据
        with open(result_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 记录 Markdown 目录，便于计算 Obsidian 相对路径
        self._markdown_dir = os.path.abspath(markdown_dir) if markdown_dir else None
        self._result_dir = str(Path(result_json_path).resolve().parent)
        self._prepare_llm_trace_output()
        concrete_canonical_by_unit = self._load_concrete_canonical_by_unit(self._result_dir)
        if concrete_canonical_by_unit:
            try:
                if self._sync_concrete_canonical_into_result_payload(data, concrete_canonical_by_unit):
                    with open(result_json_path, "w", encoding="utf-8") as file_obj:
                        json.dump(data, file_obj, ensure_ascii=False, indent=2)
                    logger.info(
                        f"Synced concrete canonical fields into result.json: units={len(concrete_canonical_by_unit)}"
                    )
            except Exception as exc:
                logger.warning(f"Failed to sync concrete canonical fields into result.json: {exc}")

        raw_groups = data.get("knowledge_groups", [])
        sections = data.get("sections", [])
        title = data.get("title", "知识文档")

        # 优先使用新协议：knowledge_groups；兼容老协议：sections。
        grouped_payloads: List[Dict[str, Any]] = []
        if isinstance(raw_groups, list) and raw_groups:
            for index, group in enumerate(raw_groups, start=1):
                if not isinstance(group, dict):
                    continue
                units = group.get("units", [])
                if not isinstance(units, list):
                    units = []
                group_id_raw = group.get("group_id", index)
                try:
                    group_id = int(group_id_raw)
                except Exception:
                    group_id = index
                if group_id <= 0:
                    group_id = index
                group_name = str(group.get("group_name", "") or "").strip() or f"知识点分组{group_id}"
                grouped_payloads.append(
                    {
                        "group_id": group_id,
                        "group_name": group_name,
                        "reason": str(group.get("reason", "") or "").strip(),
                        "units": [item for item in units if isinstance(item, dict)],
                    }
                )
        elif isinstance(sections, list) and sections:
            # 老协议兜底：按 group_id/group_name 聚合；未提供则单元各自成组。
            legacy_groups: Dict[int, Dict[str, Any]] = {}
            fallback_name_to_id: Dict[str, int] = {}
            next_fallback_id = 1
            for section in sections:
                if not isinstance(section, dict):
                    continue
                section_group_name = str(section.get("group_name", "") or "").strip()
                if not section_group_name:
                    section_group_name = str(section.get("title", "") or "").strip() or "未命名知识点"
                group_id_raw = section.get("group_id", 0)
                try:
                    group_id = int(group_id_raw)
                except Exception:
                    group_id = 0
                if group_id <= 0:
                    normalized_name = section_group_name.lower()
                    if normalized_name not in fallback_name_to_id:
                        fallback_name_to_id[normalized_name] = next_fallback_id
                        next_fallback_id += 1
                    group_id = fallback_name_to_id[normalized_name]
                if group_id not in legacy_groups:
                    legacy_groups[group_id] = {
                        "group_id": group_id,
                        "group_name": section_group_name,
                        "reason": str(section.get("group_reason", "") or "").strip(),
                        "units": [],
                    }
                if (not legacy_groups[group_id]["reason"]) and str(section.get("group_reason", "") or "").strip():
                    legacy_groups[group_id]["reason"] = str(section.get("group_reason", "") or "").strip()
                legacy_groups[group_id]["units"].append(section)
            grouped_payloads = [legacy_groups[k] for k in sorted(legacy_groups.keys())]

        if not grouped_payloads:
            return "# 无内容"

        # Step 1: 解析分组与单元（固定两级：group -> unit）
        logger.info("Step 1: Parse grouped payload")
        enhanced_groups: List[EnhancedGroup] = []
        all_sections: List[EnhancedSection] = []
        for group_index, group_payload in enumerate(grouped_payloads, start=1):
            group_id = int(group_payload.get("group_id", group_index) or group_index)
            group_name = str(group_payload.get("group_name", "") or "").strip() or f"知识点分组{group_id}"
            group_reason = str(group_payload.get("reason", "") or "").strip()
            units = group_payload.get("units", [])
            if not isinstance(units, list):
                units = []

            enhanced_group = EnhancedGroup(
                group_id=group_id,
                group_name=group_name,
                reason=group_reason,
                units=[],
            )

            for section in units:
                if not isinstance(section, dict):
                    continue
                unit_id = str(section.get("unit_id", "") or "").strip()
                normalized_kt = self._normalize_knowledge_type(section.get("knowledge_type"))
                concrete_canonical = (
                    concrete_canonical_by_unit.get(unit_id, {})
                    if normalized_kt == "concrete"
                    else {}
                )
                materials = section.get("materials", {})
                if not isinstance(materials, dict):
                    materials = {}

                raw_screenshot_items = materials.get("screenshot_items", [])
                augment_screenshot_items = [
                    item for item in (raw_screenshot_items if isinstance(raw_screenshot_items, list) else [])
                    if isinstance(item, dict)
                ]
                filtered_screenshot_items = self._filter_screenshot_items(raw_screenshot_items)
                filtered_screenshots = self._filter_screenshot_paths(
                    materials.get("screenshots") or materials.get("screenshot_paths") or [],
                    filtered_screenshot_items,
                    drop_when_items_empty=self._has_explicit_screenshot_exclusion(raw_screenshot_items),
                )

                enhanced = EnhancedSection(
                    unit_id=unit_id,
                    title=str(section.get("title", "") or ""),
                    knowledge_type=str(section.get("knowledge_type", "") or ""),
                    level=2,
                    parent_id=None,
                    original_body=str(
                        concrete_canonical.get("body_text")
                        or section.get("body_text")
                        or ""
                    ),
                    screenshots=filtered_screenshots,
                    screenshot_items=filtered_screenshot_items,
                    augment_screenshot_items=augment_screenshot_items,
                    video_clip=str(materials.get("clip", "") or ""),
                    video_clips=materials.get("clips", []) if isinstance(materials.get("clips", []), list) else [],
                    action_classifications=materials.get("action_classifications", [])
                    if isinstance(materials.get("action_classifications", []), list)
                    else [],
                    mult_steps=bool(section.get("mult_steps", False)),
                    group_id=group_id,
                    group_name=group_name,
                    group_reason=group_reason,
                    vl_concrete_segments=(
                        concrete_canonical.get("vl_concrete_segments")
                        if isinstance(concrete_canonical.get("vl_concrete_segments"), list)
                        else (
                            section.get("_vl_concrete_segments")
                            if isinstance(section.get("_vl_concrete_segments"), list)
                            else (
                                section.get("vl_concrete_segments")
                                if isinstance(section.get("vl_concrete_segments"), list)
                                else []
                            )
                        )
                    ),
                )

                if enhanced.video_clip and enhanced.video_clip not in enhanced.video_clips:
                    enhanced.video_clips.insert(0, enhanced.video_clip)

                # V3: RichTextPipeline 已做截图验证，避免重复调用。
                enhanced.validated_screenshots = enhanced.screenshots
                enhanced.tutorial_steps = self._load_tutorial_steps(
                    unit_id=unit_id,
                    inline_steps=section.get("instructional_steps", []),
                )
                enhanced_group.units.append(enhanced)
                all_sections.append(enhanced)

            if enhanced_group.units:
                enhanced_groups.append(enhanced_group)

        if not all_sections:
            return "# 无内容"

        # Step 2: 正文增强 + 结构化（并行提交，按完成顺序流式处理）
        logger.info("Step 2: Parallel LLM enhance/structure by unit")
        section_max_inflight = max(1, int(getattr(self, "_section_max_inflight", 48)))
        section_sem = asyncio.Semaphore(section_max_inflight)
        logger.info(f"Step 2 inflight cap: {section_max_inflight}")

        async def _process_one(idx: int, sec: EnhancedSection) -> int:
            """
            做什么：对单个语义单元执行“正文增强 -> 逻辑提取”两步。
            为什么：两步互相依赖，但不同语义单元之间可并行，从而降低单任务总时延。
            权衡：并行会增加瞬时 in-flight，请确保 LLMClient 的调度器生效（token 加权 + 资源 cap）。
            """
            # compute adjacent section titles for cross-topic transitions
            prev_title = all_sections[idx - 1].title if idx > 0 else ""
            next_title = all_sections[idx + 1].title if idx < len(all_sections) - 1 else ""

            async with section_sem:
                normalized_kt = self._normalize_knowledge_type(sec.knowledge_type)

                if self._is_tutorial_process_section(sec):
                    # 教程型 process 直接走步骤渲染，不走通用逻辑抽取。
                    sec.enhanced_body = sec.original_body
                    sec.structured_content = ""
                    return idx

                if normalized_kt == "abstract":
                    # 仅 abstract 使用 structured LLM。
                    sec.enhanced_body = sec.original_body
                    sec.structured_content = await self._build_structured_text_for_concept(
                        sec, prev_title=prev_title, next_title=next_title
                    )
                    return idx

                if normalized_kt in {"concrete", "process"}:
                    # concrete/process 禁用 structured LLM，统一走确定性规则链路。
                    sec.enhanced_body = sec.original_body
                    sec.structured_content = self._build_deterministic_text_for_non_abstract(sec)
                    return idx

                if self._combine_llm_calls:
                    try:
                        sec.enhanced_body, sec.structured_content = await self._enhance_and_extract(sec)
                        return idx
                    except Exception as e:
                        logger.warning(
                            f"[{sec.unit_id}] Combined LLM call failed: {e} -> fallback to 2-step pipeline"
                        )

                sec.enhanced_body = await self._enhance_text(sec)
                sec.structured_content = await self._extract_logic(sec)
                return idx

        tasks = [asyncio.create_task(_process_one(i, s)) for i, s in enumerate(all_sections)]
        completed = 0
        for fut in asyncio.as_completed(tasks):
            try:
                await fut
            except Exception as e:
                logger.error(f"Section pipeline failed: {e}")
            finally:
                completed += 1
                if completed == len(tasks) or completed % 5 == 0:
                    logger.info(f"LLM sections completed: {completed}/{len(tasks)}")

        # Step 3: 组装 Markdown（固定 group -> unit 两级）
        logger.info("Step 3: Assembling grouped markdown")
        markdown = self._assemble_markdown(title, enhanced_groups)
        
        return markdown
    
    async def _classify_hierarchy(self, sections: List[Dict], subject: str) -> Dict[str, Dict]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not self._enabled
        依据来源（证据链）：
        - 对象内部状态：self._enabled。
        输入参数：
        - sections: 函数入参（类型：List[Dict]）。
        - subject: 函数入参（类型：str）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        if not self._enabled:
            # 默认: 所有单元为二级
            return {s.get("unit_id", f"SU{i}"): {"level": 2, "parent_id": None} 
                    for i, s in enumerate(sections)}
        
        # 构建标题列表
        titles = "\n".join([
            f"- {s.get('unit_id', '')}: {s.get('title', '')}"
            for s in sections
        ])
        
        prompt = self._hierarchy_prompt_template.format(subject=subject, titles=titles)
        start_ts = time.perf_counter()
        try:
            # 🚀 使用 LLMClient 进行异步调用
            content, meta, _ = await self._llm_client.complete_text(
                prompt=prompt
            )
            duration_ms = (time.perf_counter() - start_ts) * 1000.0
            await self._write_llm_trace_record(
                step_name="hierarchy_classification",
                unit_id="GLOBAL",
                system_prompt="",
                user_prompt=prompt,
                response_text=content,
                duration_ms=duration_ms,
                success=True,
                metadata=meta,
            )

            # 兼容代码块/前后缀的 JSON 输出
            result = self._safe_json_loads(content)
            
            # 转换为 dict
            hierarchy = {}
            for item in result.get("hierarchy", []):
                hierarchy[item["unit_id"]] = {
                    "level": item["level"],
                    "parent_id": item.get("parent_id")
                }
            
            logger.info(f"Hierarchy classified: {len(hierarchy)} units")
            return hierarchy
            
        except Exception as e:
            duration_ms = (time.perf_counter() - start_ts) * 1000.0
            await self._write_llm_trace_record(
                step_name="hierarchy_classification",
                unit_id="GLOBAL",
                system_prompt="",
                user_prompt=prompt,
                response_text="",
                duration_ms=duration_ms,
                success=False,
                error_msg=str(e),
            )
            logger.error(f"Hierarchy classification failed: {e}")
            return {s.get("unit_id", f"SU{i}"): {"level": 2, "parent_id": None} 
                    for i, s in enumerate(sections)}

    def _safe_json_loads(self, content: str) -> Dict[str, Any]:
        """
        处理 LLM 可能输出的代码块或前后缀，尽量提取 JSON。
        """
        cleaned = (content or "").strip()
        if "```" in cleaned:
            if "```json" in cleaned:
                cleaned = cleaned.split("```json", 1)[1].split("```", 1)[0].strip()
            else:
                cleaned = cleaned.split("```", 1)[1].split("```", 1)[0].strip()
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end != -1 and end > start:
            cleaned = cleaned[start:end + 1]
        return json.loads(cleaned)

    def _fallback_level(self, knowledge_type: str) -> int:
        """
        兜底层级映射：在无一级输出时提供最小可用层级。
        """
        kt = (knowledge_type or "").lower()
        if kt in ("abstract", "抽象", "讲解型", "explanation"):
            return 1
        if kt in ("process", "过程", "过程性", "procedural"):
            return 2
        if kt in ("concrete", "具象", "具体"):
            return 3
        return 2

    def _normalize_hierarchy(self, sections: List[EnhancedSection]) -> None:
        """
        修复缺失父级/异常层级，保证 Obsidian 嵌套结构可用。
        """
        if not sections:
            return

        has_level1 = any(s.level == 1 for s in sections)
        if not has_level1:
            for s in sections:
                s.level = self._fallback_level(getattr(s, "knowledge_type", ""))

        unit_index = {s.unit_id: i for i, s in enumerate(sections)}
        level_stack = {1: None, 2: None}

        for section in sections:
            if section.level not in (1, 2, 3):
                section.level = 2

            if section.level == 1:
                section.parent_id = None
                level_stack[1] = section.unit_id
                level_stack[2] = None
                continue

            if section.parent_id not in unit_index:
                if section.level == 2:
                    section.parent_id = level_stack.get(1)
                else:
                    section.parent_id = level_stack.get(2) or level_stack.get(1)

            if section.level == 2:
                level_stack[2] = section.unit_id
    def _normalize_knowledge_type(self, knowledge_type: str) -> str:
        lowered = (knowledge_type or "").strip().lower()
        if any(key in lowered for key in ("process", "过程", "操作", "procedural")):
            return "process"
        if any(key in lowered for key in ("concrete", "具象", "实例", "示例", "实操")):
            return "concrete"
        if any(key in lowered for key in ("abstract", "抽象", "讲解", "概念", "explanation")):
            return "abstract"
        return lowered or "abstract"

    def _is_tutorial_process_section(self, section: EnhancedSection) -> bool:
        if self._normalize_knowledge_type(section.knowledge_type) != "process":
            return False
        if not section.tutorial_steps:
            return False
        return True

    @staticmethod
    def _normalize_tutorial_step_type(value: Any) -> str:
        text = str(value or "").strip().upper()
        if not text:
            return "MAIN_FLOW"
        if text in {"MAIN_FLOW", "CONDITIONAL", "OPTIONAL", "TROUBLESHOOTING"}:
            return text
        alias_map = {
            "MAIN": "MAIN_FLOW",
            "PRIMARY": "MAIN_FLOW",
            "NORMAL": "MAIN_FLOW",
            "BRANCH": "CONDITIONAL",
            "CONDITION": "CONDITIONAL",
            "IF": "CONDITIONAL",
            "OPTION": "OPTIONAL",
            "ERROR": "TROUBLESHOOTING",
            "EXCEPTION": "TROUBLESHOOTING",
            "DEBUG": "TROUBLESHOOTING",
            "FIX": "TROUBLESHOOTING",
        }
        return alias_map.get(text, "MAIN_FLOW")

    @staticmethod
    def _quote_lines(block_lines: List[str]) -> List[str]:
        quoted: List[str] = []
        for line in block_lines:
            if line:
                quoted.append(f"> {line}")
            else:
                quoted.append(">")
        return quoted

    def _load_tutorial_steps(self, unit_id: str, inline_steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        def _safe_float(value: Any, default: float = 0.0) -> float:
            try:
                return float(value)
            except Exception:
                return float(default)

        def _safe_int(value: Any, default: int) -> int:
            try:
                return int(value)
            except Exception:
                return int(default)

        def _to_abs(path_value: Any, base_dir: Optional[Path] = None) -> str:
            raw = str(path_value or "").strip()
            if not raw:
                return ""
            path_obj = Path(raw)
            if not path_obj.is_absolute() and base_dir is not None:
                path_obj = (base_dir / path_obj).resolve()
            return str(path_obj)

        def _extract_timestamps(step: Dict[str, Any]) -> List[float]:
            values = step.get("instructional_keyframe_timestamp")
            if values is None:
                values = step.get("keyframe_timestamps")
            if values is None:
                values = step.get("suggested_screenshoot_timestamps")
            if values is None:
                values = step.get("suggested_screenshot_timestamps")
            if values is None:
                values = []
            if not isinstance(values, list):
                values = [values]
            return [_safe_float(v, 0.0) for v in values]

        def _normalize_text_list(value: Any) -> List[str]:
            if value is None:
                return []
            raw_items: List[Any]
            if isinstance(value, (list, tuple, set)):
                raw_items = list(value)
            elif isinstance(value, str):
                text = value.strip()
                if not text:
                    return []
                try:
                    parsed = json.loads(text)
                except Exception:
                    parsed = None
                if isinstance(parsed, list):
                    raw_items = parsed
                else:
                    raw_items = [segment for segment in re.split(r"[\n;；]+", text) if segment and segment.strip()]
            else:
                raw_items = [value]

            normalized: List[str] = []
            seen: set[str] = set()
            for item in raw_items:
                text_item = str(item or "").strip()
                if not text_item:
                    continue
                dedup_key = text_item.lower()
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)
                normalized.append(text_item)
            return normalized

        def _normalize_main_operation(value: Any) -> str:
            if value is None:
                return ""
            if isinstance(value, str):
                text = value.strip()
                if not text:
                    return ""
                try:
                    parsed = json.loads(text)
                except Exception:
                    parsed = None
                if isinstance(parsed, list):
                    return "\n".join(_normalize_text_list(parsed)).strip()
                return text
            if isinstance(value, (list, tuple, set)):
                return "\n".join(_normalize_text_list(list(value))).strip()
            return str(value).strip()

        def _normalize_bbox_1000(value: Any) -> Optional[List[int]]:
            if not isinstance(value, (list, tuple)) or len(value) != 4:
                return None
            try:
                xmin = int(round(float(value[0])))
                ymin = int(round(float(value[1])))
                xmax = int(round(float(value[2])))
                ymax = int(round(float(value[3])))
            except Exception:
                return None
            xmin = max(0, min(1000, xmin))
            ymin = max(0, min(1000, ymin))
            xmax = max(0, min(1000, xmax))
            ymax = max(0, min(1000, ymax))
            if xmax < xmin:
                xmin, xmax = xmax, xmin
            if ymax < ymin:
                ymin, ymax = ymax, ymin
            return [xmin, ymin, xmax, ymax]

        def _normalize_keyframe_entries(value: Any, base_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
            if not isinstance(value, list):
                return []
            normalized: List[Dict[str, Any]] = []
            for item in value:
                if isinstance(item, str):
                    image_path = _to_abs(item, base_dir=base_dir)
                    if image_path:
                        normalized.append({"image_path": image_path})
                    continue
                if not isinstance(item, dict):
                    continue

                entry: Dict[str, Any] = {}
                image_value = (
                    item.get("image_path")
                    or item.get("image_file")
                    or item.get("img_path")
                    or item.get("path")
                    or item.get("file_path")
                )
                image_path = _to_abs(image_value, base_dir=base_dir)
                if image_path:
                    entry["image_path"] = image_path

                raw_ts = item.get("timestamp_sec", item.get("timestamp"))
                if raw_ts is not None:
                    entry["timestamp_sec"] = _safe_float(raw_ts, 0.0)
                frame_reason = str(item.get("frame_reason", "") or "").strip()
                if frame_reason:
                    entry["frame_reason"] = frame_reason
                bbox = _normalize_bbox_1000(item.get("bbox"))
                if bbox is not None:
                    entry["bbox"] = bbox
                if entry:
                    normalized.append(entry)
            return normalized

        def _normalize_clip_entries(value: Any, base_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
            if not isinstance(value, list):
                return []
            normalized: List[Dict[str, Any]] = []
            for item in value:
                if isinstance(item, str):
                    clip_path = _to_abs(item, base_dir=base_dir)
                    if clip_path:
                        normalized.append({"clip_path": clip_path})
                    continue
                if not isinstance(item, dict):
                    continue

                entry: Dict[str, Any] = {}
                clip_value = (
                    item.get("clip_path")
                    or item.get("clip_file")
                    or item.get("video_path")
                    or item.get("path")
                    or item.get("file_path")
                )
                clip_path = _to_abs(clip_value, base_dir=base_dir)
                if clip_path:
                    entry["clip_path"] = clip_path
                clip_reason = str(item.get("clip_reason", "") or "").strip()
                if clip_reason:
                    entry["clip_reason"] = clip_reason
                clip_id = self._normalize_clip_id(
                    item.get("instructional_clip_id", item.get("clip_id", item.get("clipId")))
                )
                if clip_id:
                    entry["clip_id"] = clip_id
                raw_start = item.get("start_sec", item.get("clip_start_sec"))
                if raw_start is not None:
                    entry["start_sec"] = _safe_float(raw_start, 0.0)
                raw_end = item.get("end_sec", item.get("clip_end_sec"))
                if raw_end is not None:
                    entry["end_sec"] = _safe_float(raw_end, entry.get("start_sec", 0.0))
                if entry:
                    normalized.append(entry)
            return normalized

        def _normalize_step(raw_step: Dict[str, Any], order: int, base_dir: Optional[Path] = None) -> Dict[str, Any]:
            step_id = _safe_int(raw_step.get("step_id", order), order)
            step_desc = str(
                raw_step.get("step_description")
                or raw_step.get("description")
                or raw_step.get("title")
                or f"step_{step_id}"
            ).strip()
            step_type = self._normalize_tutorial_step_type(
                raw_step.get("step_type")
                or raw_step.get("stepType")
                or raw_step.get("step_category")
                or raw_step.get("type")
            )
            main_action = str(
                raw_step.get("main_action")
                or raw_step.get("主要动作")
                or ""
            ).strip()
            raw_main_operation = raw_step.get("main_operation")
            if raw_main_operation is None:
                raw_main_operation = raw_step.get("main_operations")
            if raw_main_operation is None:
                raw_main_operation = raw_step.get("主要操作")
            main_operation = _normalize_main_operation(raw_main_operation)
            raw_precautions = raw_step.get("precautions")
            if raw_precautions is None:
                raw_precautions = raw_step.get("notes")
            if raw_precautions is None:
                raw_precautions = raw_step.get("注意事项")
            if raw_precautions is None:
                raw_precautions = raw_step.get("cautions")
            precautions = _normalize_text_list(raw_precautions)
            step_summary = str(
                raw_step.get("step_summary")
                or raw_step.get("步骤小结")
                or raw_step.get("summary")
                or ""
            ).strip()
            raw_operation_guidance = raw_step.get("operation_guidance")
            if raw_operation_guidance is None:
                raw_operation_guidance = raw_step.get("操作指导")
            if raw_operation_guidance is None:
                raw_operation_guidance = raw_step.get("guidance")
            operation_guidance = _normalize_text_list(raw_operation_guidance)

            start_val = raw_step.get("clip_start_sec")
            end_val = raw_step.get("clip_end_sec")
            if start_val is None or end_val is None:
                time_range = raw_step.get("timestamp_range")
                if isinstance(time_range, list) and len(time_range) >= 2:
                    start_val = time_range[0]
                    end_val = time_range[1]

            if start_val is None:
                start_val = raw_step.get("start_sec", 0.0)
            if end_val is None:
                end_val = raw_step.get("end_sec", start_val)

            clip_start = _safe_float(start_val, 0.0)
            clip_end = _safe_float(end_val, clip_start)
            if clip_end < clip_start:
                clip_start, clip_end = clip_end, clip_start

            materials = raw_step.get("materials") if isinstance(raw_step.get("materials"), dict) else {}
            clip_file = raw_step.get("clip_file") or raw_step.get("clip_path")
            if not clip_file:
                clip_file = materials.get("clip_path") or materials.get("clip")

            clip_entries: List[Dict[str, Any]] = []
            clip_entries.extend(
                _normalize_clip_entries(
                    raw_step.get("instructional_clip_details"),
                    base_dir=base_dir,
                )
            )
            clip_entries.extend(
                _normalize_clip_entries(
                    raw_step.get("instructional_clips"),
                    base_dir=base_dir,
                )
            )
            if not clip_entries:
                material_clips = materials.get("clip_paths") or materials.get("clips")
                if isinstance(material_clips, list):
                    clip_entries.extend(_normalize_clip_entries(material_clips, base_dir=base_dir))
            if clip_file:
                clip_entries.extend(_normalize_clip_entries([clip_file], base_dir=base_dir))

            deduped_clips: List[Dict[str, Any]] = []
            clip_path_to_index: Dict[str, int] = {}
            seen_pathless_clips: set[str] = set()
            for item in clip_entries:
                clip_path_key = self._normalize_embed_path(item.get("clip_path", ""))
                if clip_path_key:
                    existed_idx = clip_path_to_index.get(clip_path_key)
                    if existed_idx is None:
                        clip_path_to_index[clip_path_key] = len(deduped_clips)
                        deduped_clips.append(dict(item))
                    else:
                        existed_item = deduped_clips[existed_idx]
                        if not existed_item.get("clip_reason") and item.get("clip_reason"):
                            existed_item["clip_reason"] = item.get("clip_reason")
                        if not existed_item.get("clip_id") and item.get("clip_id"):
                            existed_item["clip_id"] = item.get("clip_id")
                        if existed_item.get("start_sec") is None and item.get("start_sec") is not None:
                            existed_item["start_sec"] = item.get("start_sec")
                        if existed_item.get("end_sec") is None and item.get("end_sec") is not None:
                            existed_item["end_sec"] = item.get("end_sec")
                    continue

                key = json.dumps(item, ensure_ascii=False, sort_keys=True)
                if key in seen_pathless_clips:
                    continue
                seen_pathless_clips.add(key)
                deduped_clips.append(dict(item))

            keyframe_entries: List[Dict[str, Any]] = []
            keyframe_entries.extend(
                _normalize_keyframe_entries(
                    raw_step.get("instructional_keyframe_details"),
                    base_dir=base_dir,
                )
            )
            keyframe_entries.extend(
                _normalize_keyframe_entries(
                    raw_step.get("instructional_keyframes"),
                    base_dir=base_dir,
                )
            )
            if not keyframe_entries:
                material_items = materials.get("screenshot_items")
                if isinstance(material_items, list):
                    for item in material_items:
                        if not isinstance(item, dict):
                            continue
                        if not self._is_screenshot_item_includable(item):
                            continue
                        item_path = str(
                            item.get("img_path") or item.get("path") or item.get("file_path") or ""
                        ).strip()
                        if item_path:
                            frame_reason = str(item.get("frame_reason", "") or "").strip()
                            keyframe_entries.append(
                                {
                                    "image_path": _to_abs(item_path, base_dir=base_dir),
                                    "timestamp_sec": _safe_float(item.get("timestamp_sec", 0.0), 0.0),
                                    **({"frame_reason": frame_reason} if frame_reason else {}),
                                }
                            )
            if not keyframe_entries:
                material_images = materials.get("screenshot_paths") or materials.get("screenshots")
                if isinstance(material_images, list):
                    for path_item in material_images:
                        image_path = _to_abs(path_item, base_dir=base_dir)
                        if image_path:
                            keyframe_entries.append({"image_path": image_path})

            deduped_keyframes: List[Dict[str, Any]] = []
            keyframe_path_to_index: Dict[str, int] = {}
            seen_pathless_keyframes: set[str] = set()
            for item in keyframe_entries:
                image_path_key = self._normalize_embed_path(item.get("image_path", ""))
                if image_path_key:
                    existed_idx = keyframe_path_to_index.get(image_path_key)
                    if existed_idx is None:
                        keyframe_path_to_index[image_path_key] = len(deduped_keyframes)
                        deduped_keyframes.append(dict(item))
                    else:
                        existed_item = deduped_keyframes[existed_idx]
                        if not existed_item.get("frame_reason") and item.get("frame_reason"):
                            existed_item["frame_reason"] = item.get("frame_reason")
                        if existed_item.get("timestamp_sec") is None and item.get("timestamp_sec") is not None:
                            existed_item["timestamp_sec"] = item.get("timestamp_sec")
                        if existed_item.get("bbox") is None and item.get("bbox") is not None:
                            existed_item["bbox"] = item.get("bbox")
                    continue

                key = json.dumps(item, ensure_ascii=False, sort_keys=True)
                if key in seen_pathless_keyframes:
                    continue
                seen_pathless_keyframes.add(key)
                deduped_keyframes.append(dict(item))

            timestamps = _extract_timestamps(raw_step)
            for item in deduped_keyframes:
                ts_value = item.get("timestamp_sec")
                if ts_value is None:
                    continue
                timestamps.append(_safe_float(ts_value, 0.0))
            deduped_timestamps: List[float] = []
            seen_ts: set[float] = set()
            for ts in timestamps:
                marker = round(float(ts), 6)
                if marker in seen_ts:
                    continue
                seen_ts.add(marker)
                deduped_timestamps.append(float(ts))

            return {
                "step_id": step_id,
                "step_description": step_desc,
                "step_type": step_type,
                "main_action": main_action,
                "main_operation": main_operation,
                "precautions": precautions,
                "step_summary": step_summary,
                "operation_guidance": operation_guidance,
                "action_brief": str(raw_step.get("action_brief", "") or "").strip(),
                "clip_start_sec": clip_start,
                "clip_end_sec": clip_end,
                "instructional_keyframe_timestamp": deduped_timestamps,
                "clip_file": _to_abs(clip_file, base_dir=base_dir),
                "instructional_clips": deduped_clips,
                "instructional_keyframes": deduped_keyframes,
            }

        by_step: Dict[int, Dict[str, Any]] = {}
        for idx, raw in enumerate(inline_steps or [], start=1):
            if isinstance(raw, dict):
                normalized = _normalize_step(raw, idx)
                by_step[normalized["step_id"]] = normalized

        step_json_path: Optional[Path] = None
        if self._result_dir:
            result_dir = Path(self._result_dir)
            expected_path = result_dir / "vl_tutorial_units" / str(unit_id) / f"{unit_id}_steps.json"
            if expected_path.exists():
                step_json_path = expected_path
            else:
                matches = sorted(result_dir.rglob(f"{unit_id}_steps.json"))
                if matches:
                    step_json_path = matches[0]

        if step_json_path and step_json_path.exists():
            try:
                with open(step_json_path, "r", encoding="utf-8") as file_obj:
                    payload = json.load(file_obj)

                if isinstance(payload, dict):
                    raw_steps = payload.get("raw_response") or []
                    manifest_steps = payload.get("steps") or []
                elif isinstance(payload, list):
                    raw_steps = payload
                    manifest_steps = []
                else:
                    raw_steps = []
                    manifest_steps = []

                base_dir = step_json_path.parent

                for idx, raw in enumerate(raw_steps, start=1):
                    if not isinstance(raw, dict):
                        continue
                    normalized = _normalize_step(raw, idx, base_dir=base_dir)
                    existed = by_step.get(normalized["step_id"])
                    if not existed:
                        by_step[normalized["step_id"]] = normalized
                    elif not existed.get("instructional_keyframe_timestamp"):
                        existed["instructional_keyframe_timestamp"] = normalized["instructional_keyframe_timestamp"]
                    if existed:
                        incoming_step_type = normalized.get("step_type", "MAIN_FLOW")
                        existing_step_type = self._normalize_tutorial_step_type(existed.get("step_type"))
                        if incoming_step_type != "MAIN_FLOW" or not existing_step_type:
                            existed["step_type"] = incoming_step_type
                        else:
                            existed["step_type"] = existing_step_type
                        if not existed.get("main_action"):
                            existed["main_action"] = normalized["main_action"]
                        if normalized["main_operation"] and not existed.get("main_operation"):
                            existed["main_operation"] = normalized["main_operation"]
                        if normalized["precautions"] and not existed.get("precautions"):
                            existed["precautions"] = normalized["precautions"]
                        if not existed.get("step_summary"):
                            existed["step_summary"] = normalized["step_summary"]
                        if normalized["operation_guidance"] and not existed.get("operation_guidance"):
                            existed["operation_guidance"] = normalized["operation_guidance"]

                for idx, raw in enumerate(manifest_steps, start=1):
                    if not isinstance(raw, dict):
                        continue
                    normalized = _normalize_step(raw, idx, base_dir=base_dir)
                    existed = by_step.get(normalized["step_id"])
                    if not existed:
                        by_step[normalized["step_id"]] = normalized
                        continue
                    existed["step_description"] = normalized["step_description"] or existed["step_description"]
                    incoming_step_type = normalized.get("step_type", "MAIN_FLOW")
                    existing_step_type = self._normalize_tutorial_step_type(existed.get("step_type"))
                    if incoming_step_type != "MAIN_FLOW" or not existing_step_type:
                        existed["step_type"] = incoming_step_type
                    else:
                        existed["step_type"] = existing_step_type
                    existed["main_action"] = normalized["main_action"] or existed.get("main_action", "")
                    if normalized["main_operation"]:
                        existed["main_operation"] = normalized["main_operation"]
                    if normalized["precautions"]:
                        existed["precautions"] = normalized["precautions"]
                    if normalized["step_summary"]:
                        existed["step_summary"] = normalized["step_summary"]
                    if normalized["operation_guidance"]:
                        existed["operation_guidance"] = normalized["operation_guidance"]
                    existed["action_brief"] = normalized["action_brief"] or existed["action_brief"]
                    existed["clip_start_sec"] = normalized["clip_start_sec"]
                    existed["clip_end_sec"] = normalized["clip_end_sec"]
                    if normalized["clip_file"]:
                        existed["clip_file"] = normalized["clip_file"]
                    if normalized.get("instructional_clips"):
                        existed["instructional_clips"] = normalized["instructional_clips"]
                    if normalized["instructional_keyframes"]:
                        existed["instructional_keyframes"] = normalized["instructional_keyframes"]
            except Exception as exc:
                logger.warning(f"Failed to load tutorial steps for {unit_id}: {exc}")

        return sorted(
            by_step.values(),
            key=lambda item: (int(item.get("step_id", 0) or 0), float(item.get("clip_start_sec", 0.0))),
        )

    def _build_concept_image_items(self, section: EnhancedSection) -> List[Dict[str, Any]]:
        if section.screenshot_items:
            raw_items = section.screenshot_items
        else:
            raw_items = [
                {
                    "img_id": f"{section.unit_id}_img_{idx:02d}",
                    "img_path": path,
                    "img_description": f"image_{idx:02d}",
                }
                for idx, path in enumerate(section.validated_screenshots or section.screenshots, start=1)
            ]

        normalized: List[Dict[str, Any]] = []
        for idx, raw in enumerate(raw_items, start=1):
            if not isinstance(raw, dict):
                continue
            if not self._is_screenshot_item_includable(raw):
                continue
            img_path = str(raw.get("img_path") or raw.get("path") or raw.get("file_path") or "").strip()
            if not img_path:
                continue
            img_id = str(raw.get("img_id") or f"{section.unit_id}_img_{idx:02d}").strip()
            img_description = str(
                raw.get("img_description")
                or raw.get("img_desription")
                or raw.get("label")
                or f"image_{idx:02d}"
            ).strip()
            frame_reason = str(raw.get("frame_reason", "") or "").strip()
            normalized.append(
                {
                    "img_id": img_id,
                    "source_id": str(raw.get("source_id") or "").strip(),
                    "img_path": img_path,
                    "img_description": img_description,
                    "frame_reason": frame_reason,
                    "timestamp_sec": raw.get("timestamp_sec"),
                    "sentence_id": str(raw.get("sentence_id") or "").strip(),
                    "sentence_text": str(raw.get("sentence_text") or "").strip(),
                }
            )
        return normalized

    def _build_concept_clip_items(self, section: EnhancedSection) -> List[Dict[str, Any]]:
        raw_paths = list(section.video_clips or [])
        if section.video_clip and section.video_clip not in raw_paths:
            raw_paths.insert(0, section.video_clip)

        normalized: List[Dict[str, Any]] = []
        seen_paths: set[str] = set()
        for idx, clip_path in enumerate(raw_paths, start=1):
            clip_text = str(clip_path or "").strip()
            if not clip_text:
                continue
            normalized_path = self._normalize_embed_path(clip_text)
            if normalized_path and normalized_path in seen_paths:
                continue
            if normalized_path:
                seen_paths.add(normalized_path)
            source_id = Path(clip_text).stem
            normalized.append(
                {
                    "clip_id": self._extract_clip_id_from_source_id(source_id),
                    "segment_id": self._extract_segment_id_from_source_id(source_id),
                    "source_id": source_id,
                    "clip_path": clip_text,
                    "clip_reason": "",
                    "clip_index": idx,
                }
            )
        return normalized

    def _build_augment_image_items(self, section: EnhancedSection) -> List[Dict[str, Any]]:
        """构建用于 img-desc 增量补全的图片描述列表：不受 should_include 限制。"""
        if section.augment_screenshot_items:
            raw_items = section.augment_screenshot_items
        elif section.screenshot_items:
            raw_items = section.screenshot_items
        else:
            raw_items = [
                {
                    "img_id": f"{section.unit_id}_img_{idx:02d}",
                    "img_path": path,
                    "img_description": f"image_{idx:02d}",
                }
                for idx, path in enumerate(section.validated_screenshots or section.screenshots, start=1)
            ]

        normalized: List[Dict[str, Any]] = []
        for idx, raw in enumerate(raw_items, start=1):
            if not isinstance(raw, dict):
                continue
            if not self._has_related_img_description(raw):
                continue
            img_description = str(
                raw.get("img_description")
                or raw.get("img_desription")
                or ""
            ).strip()
            img_id = str(raw.get("img_id") or f"{section.unit_id}_img_{idx:02d}").strip()
            normalized.append(
                {
                    "img_id": img_id,
                    "img_path": str(raw.get("img_path") or raw.get("path") or raw.get("file_path") or "").strip(),
                    "img_description": img_description,
                    "timestamp_sec": raw.get("timestamp_sec"),
                    "sentence_id": str(raw.get("sentence_id") or "").strip(),
                    "sentence_text": str(raw.get("sentence_text") or "").strip(),
                }
            )
        return normalized

    @staticmethod
    def _clip_text_for_prompt(value: str, max_chars: int) -> str:
        """裁剪提示词字段，防止单字段过长占满预算。"""
        text = str(value or "").strip()
        limit = max(0, int(max_chars))
        if limit <= 0:
            return ""
        if len(text) <= limit:
            return text
        if limit <= 3:
            return text[:limit]
        return text[: limit - 3] + "..."

    @staticmethod
    def _estimate_tokens_from_chars(total_chars: int) -> int:
        """与 LLMClient 一致：按字符/4 粗估 token。"""
        return max(1, int(max(0, int(total_chars)) / 4))

    @staticmethod
    def _has_related_img_description(item: Dict[str, Any]) -> bool:
        """仅接受“可用于补全正文细节”的描述，过滤占位型/标签型描述。"""
        if not isinstance(item, dict):
            return False
        desc = str(
            item.get("img_description")
            or item.get("img_desription")
            or ""
        ).strip()
        if not desc:
            return False
        desc_lower = desc.lower()
        if re.fullmatch(r"image_\d{1,4}", desc_lower):
            return False
        placeholder_values = {
            "head",
            "tail",
            "stable",
            "fallback",
            "fallback_unit_scan",
            "image",
            "img",
            "screenshot",
        }
        if desc_lower in placeholder_values:
            return False
        label = str(item.get("label") or "").strip().lower()
        if label and desc_lower == label and label in placeholder_values:
            return False
        source_id = str(item.get("source_id") or "").strip().lower()
        if source_id and desc_lower == source_id:
            return False
        return True

    def _build_img_desc_adaptive_budget(self, body_text: str) -> Dict[str, int]:
        """按正文长度动态计算增量补全证据预算。"""
        body_chars = len(str(body_text or ""))

        # 目标：正文越长，证据预算按比例增加，但总提示词上限受控，避免 token 膨胀。
        target_prompt_chars = int(2200 + min(body_chars, 2800) * 0.9)
        target_prompt_chars = max(2200, min(5200, target_prompt_chars))

        template_overhead = len(
            self._img_desc_augment_user_prompt_template.format(
                body_text="",
                image_evidence="",
            )
        )
        system_chars = len(str(self._img_desc_augment_system_prompt or ""))
        response_reserve_chars = 400
        evidence_budget_chars = (
            target_prompt_chars
            - body_chars
            - template_overhead
            - system_chars
            - response_reserve_chars
        )
        evidence_budget_chars = max(280, evidence_budget_chars)

        if body_chars <= 600:
            max_items = 12
            max_desc_chars = 180
            max_sentence_chars = 120
        elif body_chars <= 1600:
            max_items = 9
            max_desc_chars = 130
            max_sentence_chars = 90
        else:
            max_items = 7
            max_desc_chars = 100
            max_sentence_chars = 70

        if evidence_budget_chars < 600:
            max_items = min(max_items, 4)
        elif evidence_budget_chars < 900:
            max_items = min(max_items, 6)

        return {
            "body_chars": body_chars,
            "target_prompt_chars": target_prompt_chars,
            "evidence_budget_chars": evidence_budget_chars,
            "max_items": max_items,
            "max_desc_chars": max_desc_chars,
            "max_sentence_chars": max_sentence_chars,
            "template_overhead": template_overhead,
            "system_chars": system_chars,
        }

    def _build_img_desc_evidence_with_budget(
        self,
        body_text: str,
        evidence_items: List[Dict[str, Any]],
    ) -> Tuple[List[str], Dict[str, int], set[str], List[str]]:
        """基于自适应预算裁剪证据，优先保证每个 sentence_id 至少保留一条。"""
        budget = self._build_img_desc_adaptive_budget(body_text)
        selected_lines: List[str] = []
        selected_sentence_ids: set[str] = set()
        raw_lines: List[str] = []
        raw_chars = 0
        selected_chars = 0
        dropped_by_budget = 0
        dropped_by_duplicate_sentence = 0

        for item in evidence_items:
            img_id = str(item.get("img_id") or "").strip()
            sentence_id = str(item.get("sentence_id") or "").strip()
            sentence_text = self._clip_text_for_prompt(
                str(item.get("sentence_text") or ""),
                budget["max_sentence_chars"],
            )
            img_desc = self._clip_text_for_prompt(
                str(item.get("img_description") or ""),
                budget["max_desc_chars"],
            )
            time_text = str(item.get("time_text") or "").strip()

            line = (
                f"- img_id={img_id or '(unknown)'} | timestamp={time_text or '(none)'} | "
                f"sentence_id={sentence_id or '(none)'} | sentence_text={sentence_text or '(none)'} | "
                f"img_description={img_desc}"
            )
            raw_lines.append(line)
            raw_chars += len(line) + 1

            if sentence_id and sentence_id in selected_sentence_ids:
                dropped_by_duplicate_sentence += 1
                continue
            if len(selected_lines) >= budget["max_items"]:
                dropped_by_budget += 1
                continue
            projected_chars = selected_chars + len(line) + 1
            if projected_chars > budget["evidence_budget_chars"]:
                dropped_by_budget += 1
                continue

            selected_lines.append(line)
            selected_chars = projected_chars
            if sentence_id:
                selected_sentence_ids.add(sentence_id)

        metrics = {
            "raw_items": len(raw_lines),
            "selected_items": len(selected_lines),
            "raw_evidence_chars": raw_chars,
            "selected_evidence_chars": selected_chars,
            "dropped_by_budget": dropped_by_budget,
            "dropped_by_duplicate_sentence": dropped_by_duplicate_sentence,
            "budget_evidence_chars": budget["evidence_budget_chars"],
            "budget_max_items": budget["max_items"],
            "body_chars": budget["body_chars"],
            "target_prompt_chars": budget["target_prompt_chars"],
            "template_overhead": budget["template_overhead"],
            "system_chars": budget["system_chars"],
        }
        return selected_lines, metrics, selected_sentence_ids, raw_lines

    @staticmethod
    def _apply_img_desc_incremental_ops(
        base_text: str,
        raw_payload_text: str,
    ) -> Tuple[str, Dict[str, int]]:
        metrics: Dict[str, int] = {}
        text = str(base_text or "")
        payload_text = str(raw_payload_text or "").strip()
        if not payload_text:
            metrics["empty_payload"] = 1
            return text, metrics

        payload = extract_first_json_dict(payload_text)
        if payload is None:
            metrics["legacy_full_text_path"] = 1
            return payload_text, metrics

        raw_ops = collect_patch_ops(payload)

        normalized_ops: List[Dict[str, str]] = []
        for item in raw_ops:
            normalized = normalize_replace_add_patch_item(item)
            if normalized is None:
                metrics["dropped_invalid_patch_item"] = metrics.get("dropped_invalid_patch_item", 0) + 1
                continue
            normalized_ops.append(normalized)

        if not normalized_ops:
            fallback_text = pick_full_text_fallback(payload)
            if fallback_text:
                metrics["json_full_text_fallback_used"] = 1
                return fallback_text, metrics
            metrics["empty_patch_payload_fallback_used"] = 1
            return text, metrics

        updated = text
        for op in normalized_ops:
            mode = op.get("mode", "")
            if mode == "r":
                original = op.get("o", "")
                replacement = op.get("n", "")
                left_context = op.get("l", "")
                right_context = op.get("r", "")
                positions = find_contextual_match_positions(
                    updated,
                    original,
                    left_context=left_context,
                    right_context=right_context,
                )
                if len(positions) == 1:
                    pos = positions[0]
                    updated = f"{updated[:pos]}{replacement}{updated[pos + len(original):]}"
                    metrics["applied_replace_ops"] = metrics.get("applied_replace_ops", 0) + 1
                else:
                    key = "skipped_ambiguous_replace_ops" if len(positions) > 1 else "skipped_unmatched_replace_ops"
                    metrics[key] = metrics.get(key, 0) + 1
                continue

            if mode == "a":
                add_text = op.get("n", "")
                left_context = op.get("l", "")
                right_context = op.get("r", "")
                position = op.get("p", "after")
                insert_positions = find_add_insert_positions(
                    updated,
                    left_context=left_context,
                    right_context=right_context,
                    position=position,
                )
                if len(insert_positions) == 1:
                    insert_pos = insert_positions[0]
                    updated = f"{updated[:insert_pos]}{add_text}{updated[insert_pos:]}"
                    metrics["applied_add_ops"] = metrics.get("applied_add_ops", 0) + 1
                else:
                    key = "skipped_ambiguous_add_ops" if len(insert_positions) > 1 else "skipped_unmatched_add_ops"
                    metrics[key] = metrics.get(key, 0) + 1
                continue

            metrics["dropped_unknown_mode_ops"] = metrics.get("dropped_unknown_mode_ops", 0) + 1

        metrics["patch_ops_total"] = len(normalized_ops)
        if metrics.get("applied_replace_ops", 0) + metrics.get("applied_add_ops", 0) > 0:
            metrics["patch_mode_used"] = 1
            return updated, metrics

        fallback_text = pick_full_text_fallback(payload)
        if fallback_text:
            metrics["json_full_text_fallback_used"] = 1
            return fallback_text, metrics

        metrics["patch_noop_fallback_used"] = 1
        return text, metrics

    async def _augment_body_with_image_descriptions(
        self,
        section: EnhancedSection,
        base_text: str,
        image_items: List[Dict[str, Any]],
    ) -> str:
        """在结构化前基于图片描述做一次增量补全（实验开关）。"""
        if not self._enable_img_desc_text_augment:
            logger.info(f"[{section.unit_id}] img-desc augment skipped: switch_off")
            return base_text
        if not self._enabled or not self._llm_client:
            logger.info(f"[{section.unit_id}] img-desc augment skipped: llm_unavailable")
            return base_text

        text = str(base_text or "").strip()
        if not text or not image_items:
            reason = "empty_text" if not text else "no_image_items"
            logger.info(f"[{section.unit_id}] img-desc augment skipped: {reason}")
            return text

        evidence_items: List[Dict[str, Any]] = []
        for item in image_items:
            if not isinstance(item, dict):
                continue
            img_desc = str(item.get("img_description") or "").strip()
            if not img_desc:
                continue

            img_id = str(item.get("img_id") or "").strip()
            sentence_id = str(item.get("sentence_id") or "").strip()
            sentence_text = str(item.get("sentence_text") or "").strip()
            timestamp = item.get("timestamp_sec")

            time_text = ""
            try:
                if timestamp is not None:
                    time_text = f"{float(timestamp):.2f}s"
            except Exception:
                time_text = ""

            # 仅在存在“图-句对齐证据”时触发增量补全，避免把纯图片描述误当作正文事实扩写。
            if not sentence_id and not sentence_text and not time_text:
                continue

            evidence_items.append(
                {
                    "img_id": img_id,
                    "sentence_id": sentence_id,
                    "sentence_text": sentence_text,
                    "time_text": time_text,
                    "img_description": img_desc,
                }
            )

        if not evidence_items:
            logger.info(f"[{section.unit_id}] img-desc augment skipped: no_alignment_evidence")
            return text

        evidence_lines, budget_metrics, used_sentence_ids, raw_evidence_lines = self._build_img_desc_evidence_with_budget(
            text,
            evidence_items,
        )
        if not evidence_lines:
            logger.info(
                f"[{section.unit_id}] img-desc augment skipped: no_related_img_description_after_budget, "
                f"raw_items={budget_metrics.get('raw_items', 0)}"
            )
            return text

        raw_evidence_text = "\n".join(raw_evidence_lines)
        selected_evidence_text = "\n".join(evidence_lines)
        raw_prompt_chars = (
            int(budget_metrics.get("template_overhead", 0))
            + len(text)
            + len(raw_evidence_text)
            + int(budget_metrics.get("system_chars", 0))
        )
        selected_prompt_chars = (
            int(budget_metrics.get("template_overhead", 0))
            + len(text)
            + len(selected_evidence_text)
            + int(budget_metrics.get("system_chars", 0))
        )
        prompt_tokens_before = self._estimate_tokens_from_chars(raw_prompt_chars)
        prompt_tokens_after = self._estimate_tokens_from_chars(selected_prompt_chars)
        token_saved = max(0, prompt_tokens_before - prompt_tokens_after)
        token_saved_pct = (float(token_saved) / float(prompt_tokens_before) * 100.0) if prompt_tokens_before > 0 else 0.0

        sentence_ids_text = ",".join(sorted(used_sentence_ids)) if used_sentence_ids else "(none)"
        logger.info(
            f"[{section.unit_id}] img-desc augment triggered: evidence={len(evidence_lines)}, "
            f"sentence_ids={sentence_ids_text}"
        )
        logger.info(
            f"[{section.unit_id}] img-desc augment budget: raw_items={budget_metrics.get('raw_items', 0)}, "
            f"selected_items={budget_metrics.get('selected_items', 0)}, raw_chars={budget_metrics.get('raw_evidence_chars', 0)}, "
            f"selected_chars={budget_metrics.get('selected_evidence_chars', 0)}, budget_chars={budget_metrics.get('budget_evidence_chars', 0)}, "
            f"prompt_tokens_before={prompt_tokens_before}, prompt_tokens_after={prompt_tokens_after}, "
            f"saved_tokens={token_saved}, saved_pct={token_saved_pct:.1f}%"
        )

        prompt = self._img_desc_augment_user_prompt_template.format(
            body_text=text,
            image_evidence=selected_evidence_text,
        )

        start_ts = time.perf_counter()
        try:
            content, meta, _ = await self._llm_client.complete_text(
                prompt=prompt,
                system_message=self._img_desc_augment_system_prompt,
            )
            duration_ms = (time.perf_counter() - start_ts) * 1000.0
            await self._write_llm_trace_record(
                step_name="img_desc_augment",
                unit_id=str(section.unit_id),
                system_prompt=self._img_desc_augment_system_prompt,
                user_prompt=prompt,
                response_text=content,
                duration_ms=duration_ms,
                success=True,
                metadata=meta,
            )
            candidate = str(content or "").strip()
            if not candidate:
                logger.info(f"[{section.unit_id}] img-desc augment result: empty_response_fallback")
                return text
            merged_text, patch_metrics = self._apply_img_desc_incremental_ops(text, candidate)
            changed = merged_text != text
            metrics_text = ", ".join(f"{k}={v}" for k, v in sorted(patch_metrics.items())) or "none"
            logger.info(
                f"[{section.unit_id}] img-desc augment result: "
                f"changed={str(changed).lower()}, patch_metrics={metrics_text}"
            )
            return merged_text
        except Exception as exc:
            duration_ms = (time.perf_counter() - start_ts) * 1000.0
            await self._write_llm_trace_record(
                step_name="img_desc_augment",
                unit_id=str(section.unit_id),
                system_prompt=self._img_desc_augment_system_prompt,
                user_prompt=prompt,
                response_text="",
                duration_ms=duration_ms,
                success=False,
                error_msg=str(exc),
            )
            logger.warning(f"Image-description augmentation failed for {section.unit_id}: {exc}")
            return text

    def _replace_image_placeholders(self, content: str, screenshot_items: List[Dict[str, Any]]) -> str:
        if not content or not screenshot_items:
            return content

        def _normalize_img_id(raw_id: Any) -> str:
            value = str(raw_id or "").strip().strip("`'[]{}()<>")
            value = re.sub(r"[^A-Za-z0-9_\-]", "", value)
            return value.lower()

        def _iter_img_id_aliases(item: Dict[str, Any]) -> List[str]:
            aliases: List[str] = []
            primary_img_id = str(item.get("img_id", "") or "").strip()
            if primary_img_id:
                aliases.append(primary_img_id)

            source_id = str(item.get("source_id", "") or "").strip()
            if source_id:
                aliases.append(source_id)
                aliases.append(source_id.replace("\\", "_").replace("/", "_"))
                source_name = Path(source_id).name
                if source_name:
                    aliases.append(source_name)
                    source_stem = Path(source_name).stem
                    if source_stem and source_stem != source_name:
                        aliases.append(source_stem)
            return aliases

        by_id: Dict[str, Dict[str, Any]] = {}
        for item in screenshot_items:
            if not isinstance(item, dict):
                continue
            for candidate_img_id in _iter_img_id_aliases(item):
                img_id = _normalize_img_id(candidate_img_id)
                if not img_id:
                    continue
                by_id.setdefault(img_id, item)

        if not by_id:
            return content

        # 仅支持新占位符格式：【imgneeded_{img_id}】
        pattern = re.compile(
            r"【\s*imgneeded_([A-Za-z0-9_\-]+)\s*】",
            flags=re.IGNORECASE,
        )

        # Find all matches first
        matches = list(pattern.finditer(content))
        if not matches:
            return content

        # Identify the last occurrence for each img_id
        last_occurrence_indices = {}
        for i, match in enumerate(matches):
            match_img_id = _normalize_img_id(match.group(1))
            if match_img_id in by_id:
                 last_occurrence_indices[match_img_id] = i

        # Build the result string
        result_parts = []
        last_pos = 0
        
        for i, match in enumerate(matches):
            # Append text before this match
            result_parts.append(content[last_pos:match.start()])
            
            match_img_id = _normalize_img_id(match.group(1))
            item = by_id.get(match_img_id)
            
            replacement = match.group(0) # Default to keeping it if not found
            
            if item:
                if i == last_occurrence_indices.get(match_img_id):
                    # It's the last one, replace with image
                    img_path = str(item.get("img_path", "") or "").strip()
                    if img_path:
                        frame_reason = str(item.get("frame_reason", "") or "").strip()
                        replacement = self._format_obsidian_embed(img_path, alias=frame_reason)
                else:
                    # Not the last one, remove it (replace with empty string)
                    replacement = ""
            
            result_parts.append(replacement)
            last_pos = match.end()

        # Append remaining text
        result_parts.append(content[last_pos:])
        
        return "".join(result_parts)

    @staticmethod
    def _strip_imgneeded_placeholders(content: str) -> str:
        if not content:
            return content
        stripped = re.sub(r"【\s*imgneeded_[^】]*】", "", content, flags=re.IGNORECASE)
        stripped = re.sub(r"[【\[\(]?\s*imgneeded_[A-Za-z0-9_\-{}]*\s*[】\]\)]?", "", stripped, flags=re.IGNORECASE)
        stripped = re.sub(r"[ \t]+\n", "\n", stripped)
        return stripped

    @staticmethod
    def _normalize_embed_path(path_text: Any) -> str:
        path = str(path_text or "").strip()
        if not path:
            return ""
        return path.replace("\\", "/")

    @classmethod
    def _extract_obsidian_embed_paths(cls, content: str) -> set[str]:
        if not content:
            return set()
        paths: set[str] = set()
        pattern = re.compile(r"!\[\[\s*([^|\]]+)(?:\|[^\]]*)?\]\]")
        for match in pattern.finditer(content):
            normalized = cls._normalize_embed_path(match.group(1))
            if normalized:
                paths.add(normalized)
        return paths

    @staticmethod
    def _normalize_keyframe_id(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        matched = re.search(r"KEYFRAME[_\-\s]*(\d+)", text, flags=re.IGNORECASE)
        if matched:
            return f"KEYFRAME_{int(matched.group(1))}"
        if re.fullmatch(r"\d+", text):
            return f"KEYFRAME_{int(text)}"
        return ""

    @staticmethod
    def _normalize_clip_id(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        matched = re.search(r"CLIP[_\-\s]*(\d+)", text, flags=re.IGNORECASE)
        if matched:
            return f"CLIP_{int(matched.group(1))}"
        if re.fullmatch(r"\d+", text):
            return f"CLIP_{int(text)}"
        return ""

    @classmethod
    def _replace_tutorial_keyframe_placeholders(
        cls,
        content: str,
        keyframe_embeds: List[str],
        *,
        keyframe_embed_map: Optional[Dict[str, str]] = None,
    ) -> str:
        if not content:
            return content
        normalized_map: Dict[str, str] = {}
        for raw_key, embed in (keyframe_embed_map or {}).items():
            normalized_key = cls._normalize_keyframe_id(raw_key)
            if not normalized_key:
                continue
            embed_text = str(embed or "").strip()
            if not embed_text:
                continue
            if normalized_key not in normalized_map:
                normalized_map[normalized_key] = embed_text
        if not keyframe_embeds and not normalized_map:
            return re.sub(r"\[\s*KEYFRAME_\d+\s*\]", "", content, flags=re.IGNORECASE)

        def _replace(match: re.Match) -> str:
            try:
                idx = int(match.group(1))
            except Exception:
                idx = 0
            keyframe_id = f"KEYFRAME_{idx}" if idx > 0 else ""
            if keyframe_id and keyframe_id in normalized_map:
                return normalized_map[keyframe_id]
            if idx <= 0 or idx > len(keyframe_embeds):
                return ""
            return keyframe_embeds[idx - 1]

        return re.sub(r"\[\s*KEYFRAME_(\d+)\s*\]", _replace, content, flags=re.IGNORECASE)

    @classmethod
    def _replace_clip_placeholders(
        cls,
        content: str,
        clip_embeds: List[str],
        *,
        clip_embed_map: Optional[Dict[str, str]] = None,
    ) -> str:
        if not content:
            return content
        normalized_map: Dict[str, str] = {}
        for raw_key, embed in (clip_embed_map or {}).items():
            normalized_key = cls._normalize_clip_id(raw_key)
            if not normalized_key:
                continue
            embed_text = str(embed or "").strip()
            if not embed_text:
                continue
            if normalized_key not in normalized_map:
                normalized_map[normalized_key] = embed_text
        if not clip_embeds and not normalized_map:
            return re.sub(r"\[\s*CLIP_\d+\s*\]", "", content, flags=re.IGNORECASE)

        embed_index = 0

        def _replace(match: re.Match) -> str:
            nonlocal embed_index
            try:
                idx = int(match.group(1))
            except Exception:
                idx = 0
            clip_id = f"CLIP_{idx}" if idx > 0 else ""
            if clip_id and clip_id in normalized_map:
                return normalized_map[clip_id]
            if embed_index >= len(clip_embeds):
                return ""
            embed = clip_embeds[embed_index]
            embed_index += 1
            return embed

        return re.sub(r"\[\s*CLIP_(\d+)\s*\]", _replace, content, flags=re.IGNORECASE)

    @staticmethod
    def _replace_tutorial_legacy_placeholders(content: str, keyframe_embeds: List[str]) -> str:
        if not content:
            return content
        if not keyframe_embeds:
            text = re.sub(r"【\s*imgneeded_[^】]*】", "", content, flags=re.IGNORECASE)
            return re.sub(r"\[\s*IMG:[^\]]+\]", "", text, flags=re.IGNORECASE)

        embed_index = 0

        def _replace(_match: re.Match) -> str:
            nonlocal embed_index
            if embed_index >= len(keyframe_embeds):
                return ""
            replacement = keyframe_embeds[embed_index]
            embed_index += 1
            return replacement

        text = re.sub(r"【\s*imgneeded_[^】]*】", _replace, content, flags=re.IGNORECASE)
        return re.sub(r"\[\s*IMG:[^\]]+\]", _replace, text, flags=re.IGNORECASE)

    def _build_concrete_keyframe_embeds(self, screenshot_items: List[Dict[str, Any]]) -> List[str]:
        if not screenshot_items:
            return []

        ordered_items: List[Tuple[int, int, float, int, Dict[str, Any]]] = []
        for idx, item in enumerate(screenshot_items, start=1):
            if not isinstance(item, dict):
                continue
            img_path = str(item.get("img_path", "") or "").strip()
            if not img_path:
                continue

            sort_group = 3
            sort_idx = idx
            timestamp_sort = float("inf")

            source_id = str(item.get("source_id") or "").strip()
            source_match = re.search(r"_key_(\d+)", source_id, flags=re.IGNORECASE)
            if source_match:
                sort_group = 0
                sort_idx = int(source_match.group(1))
            else:
                img_id = str(item.get("img_id") or "").strip()
                img_match = re.search(r"_img_(\d+)", img_id, flags=re.IGNORECASE)
                if img_match:
                    sort_group = 1
                    sort_idx = int(img_match.group(1))
                else:
                    try:
                        timestamp_sort = float(item.get("timestamp_sec"))
                        if timestamp_sort >= 0:
                            sort_group = 2
                            sort_idx = int(timestamp_sort * 1000)
                    except Exception:
                        pass

            ordered_items.append((sort_group, sort_idx, timestamp_sort, idx, item))

        ordered_items.sort(key=lambda entry: (entry[0], entry[1], entry[2], entry[3]))

        embeds: List[str] = []
        seen_paths: set[str] = set()
        for _, _, _, _, item in ordered_items:
            img_path = str(item.get("img_path", "") or "").strip()
            normalized_path = self._normalize_embed_path(img_path)
            if normalized_path and normalized_path in seen_paths:
                continue
            frame_reason = str(item.get("frame_reason", "") or "").strip()
            embed = self._format_obsidian_embed(img_path, alias=frame_reason)
            if not embed:
                continue
            embeds.append(embed)
            if normalized_path:
                seen_paths.add(normalized_path)
        return embeds

    @staticmethod
    def _sort_concrete_segments(section: EnhancedSection) -> List[Dict[str, Any]]:
        raw_segments = section.vl_concrete_segments or []
        if not isinstance(raw_segments, list):
            return []

        sortable: List[Tuple[int, int, Dict[str, Any]]] = []
        for index, segment in enumerate(raw_segments, start=1):
            if not isinstance(segment, dict):
                continue
            segment_id_raw = segment.get("segment_id", segment.get("id", index))
            try:
                segment_id = int(float(segment_id_raw))
            except Exception:
                segment_id = index
            if segment_id <= 0:
                segment_id = index
            sortable.append((segment_id, index, segment))
        sortable.sort(key=lambda item: (item[0], item[1]))
        return [item[2] for item in sortable]

    def _resolve_concrete_base_text(self, section: EnhancedSection) -> str:
        fallback_text = str(section.original_body or "").strip()
        if self._normalize_knowledge_type(section.knowledge_type) != "concrete":
            return fallback_text

        segments = self._sort_concrete_segments(section)
        if not segments:
            return fallback_text

        main_content_blocks: List[str] = []
        for segment in segments:
            main_content = str(segment.get("main_content", "") or "").strip()
            if main_content:
                main_content_blocks.append(main_content)
        if not main_content_blocks:
            return fallback_text
        return "\n\n".join(main_content_blocks).strip()

    @classmethod
    def _extract_keyframe_id_from_source_id(cls, source_id: Any) -> str:
        source_text = str(source_id or "").strip()
        if not source_text:
            return ""
        matched = re.search(r"_key_(\d+)", source_text, flags=re.IGNORECASE)
        if not matched:
            return ""
        return cls._normalize_keyframe_id(f"KEYFRAME_{matched.group(1)}")

    @staticmethod
    def _extract_segment_id_from_source_id(source_id: Any) -> int:
        source_text = str(source_id or "").strip()
        if not source_text:
            return 0
        matched = re.search(r"_seg_(\d+)", source_text, flags=re.IGNORECASE)
        if not matched:
            return 0
        try:
            return int(matched.group(1))
        except Exception:
            return 0

    @classmethod
    def _extract_clip_id_from_source_id(cls, source_id: Any) -> str:
        source_text = str(source_id or "").strip()
        if not source_text:
            return ""
        matched = re.search(r"_clip_(\d+)", source_text, flags=re.IGNORECASE)
        if not matched:
            return ""
        return cls._normalize_clip_id(f"CLIP_{matched.group(1)}")

    def _build_concrete_clip_embeds_by_segment_order(
        self,
        section: EnhancedSection,
        clip_items: List[Dict[str, Any]],
    ) -> List[str]:
        segments = self._sort_concrete_segments(section)
        if not segments or not clip_items:
            return []

        available: List[Dict[str, Any]] = []
        for idx, item in enumerate(clip_items, start=1):
            if not isinstance(item, dict):
                continue
            clip_path = str(item.get("clip_path", "") or "").strip()
            if not clip_path:
                continue
            segment_id = int(item.get("segment_id", 0) or 0)
            if segment_id <= 0:
                segment_id = self._extract_segment_id_from_source_id(item.get("source_id") or clip_path)
            clip_id = self._normalize_clip_id(item.get("clip_id"))
            if not clip_id:
                clip_id = self._extract_clip_id_from_source_id(item.get("source_id") or clip_path)
            start_sort = float("inf")
            try:
                start_sort = float(item.get("start_sec"))
            except Exception:
                pass
            available.append(
                {
                    "idx": idx,
                    "clip_path": clip_path,
                    "normalized_path": self._normalize_embed_path(clip_path),
                    "clip_id": clip_id,
                    "segment_id": segment_id,
                    "start_sec": start_sort,
                }
            )

        if not available:
            return []

        def _pop_entry(entry_index: int) -> Dict[str, Any]:
            return available.pop(entry_index)

        def _take_by_segment_and_clip(segment_id: int, clip_id: str) -> Optional[Dict[str, Any]]:
            for idx, entry in enumerate(available):
                if segment_id > 0 and entry.get("segment_id") == segment_id and entry.get("clip_id") == clip_id:
                    return _pop_entry(idx)
            return None

        def _take_by_clip_id(clip_id: str) -> Optional[Dict[str, Any]]:
            for idx, entry in enumerate(available):
                if entry.get("clip_id") == clip_id:
                    return _pop_entry(idx)
            return None

        def _take_by_start(start_sec: float) -> Optional[Dict[str, Any]]:
            best_index = -1
            best_delta = float("inf")
            for idx, entry in enumerate(available):
                entry_start = float(entry.get("start_sec", float("inf")))
                if entry_start == float("inf"):
                    continue
                delta = abs(entry_start - start_sec)
                if delta < best_delta:
                    best_delta = delta
                    best_index = idx
            if best_index < 0:
                return None
            return _pop_entry(best_index)

        embeds: List[str] = []
        seen_paths: set[str] = set()
        for segment in segments:
            segment_id = int(segment.get("segment_id", 0) or 0)
            raw_clips = segment.get("instructional_clips")
            if not isinstance(raw_clips, list):
                continue
            for index, clip_meta in enumerate(raw_clips, start=1):
                if not isinstance(clip_meta, dict):
                    continue
                desired_clip_id = self._normalize_clip_id(
                    clip_meta.get("clip_id", clip_meta.get("clipId", clip_meta.get("id")))
                )
                candidate = None
                if desired_clip_id:
                    candidate = _take_by_segment_and_clip(segment_id, desired_clip_id)
                    if candidate is None:
                        candidate = _take_by_clip_id(desired_clip_id)
                if candidate is None:
                    try:
                        candidate = _take_by_start(float(clip_meta.get("start_sec")))
                    except Exception:
                        candidate = None
                if candidate is None and available:
                    candidate = _pop_entry(0)
                if candidate is None:
                    continue
                clip_path = str(candidate.get("clip_path", "") or "").strip()
                normalized_path = self._normalize_embed_path(clip_path)
                if normalized_path and normalized_path in seen_paths:
                    continue
                clip_reason = str(clip_meta.get("clip_reason", "") or "").strip()
                embed = self._format_obsidian_embed(clip_path, alias=clip_reason)
                if not embed:
                    continue
                embeds.append(embed)
                if normalized_path:
                    seen_paths.add(normalized_path)
        return embeds

    def _build_concrete_keyframe_embeds_bundle_by_segment_order(
        self,
        section: EnhancedSection,
        screenshot_items: List[Dict[str, Any]],
    ) -> Tuple[List[str], Dict[str, str]]:
        segments = self._sort_concrete_segments(section)
        if not segments or not screenshot_items:
            return [], {}

        available: List[Dict[str, Any]] = []
        for idx, item in enumerate(screenshot_items, start=1):
            if not isinstance(item, dict):
                continue
            img_path = str(item.get("img_path", "") or "").strip()
            if not img_path:
                continue
            timestamp_sort = float("inf")
            try:
                timestamp_value = float(item.get("timestamp_sec"))
                if timestamp_value >= 0:
                    timestamp_sort = timestamp_value
            except Exception:
                pass
            keyframe_id = self._normalize_keyframe_id(
                item.get("keyframe_id", item.get("keyframeId"))
            )
            if not keyframe_id:
                keyframe_id = self._extract_keyframe_id_from_source_id(item.get("source_id"))
            available.append(
                {
                    "idx": idx,
                    "item": item,
                    "img_path": img_path,
                    "normalized_path": self._normalize_embed_path(img_path),
                    "timestamp": timestamp_sort,
                    "keyframe_id": keyframe_id,
                }
            )

        if not available:
            return [], {}

        def _pop_entry(entry_index: int) -> Dict[str, Any]:
            return available.pop(entry_index)

        def _take_by_keyframe_id(keyframe_id: str) -> Optional[Dict[str, Any]]:
            normalized = self._normalize_keyframe_id(keyframe_id)
            if not normalized:
                return None
            for idx, entry in enumerate(available):
                if str(entry.get("keyframe_id", "") or "").upper() == normalized:
                    return _pop_entry(idx)
            return None

        def _take_by_path(path_text: str) -> Optional[Dict[str, Any]]:
            normalized = self._normalize_embed_path(path_text)
            path_name = Path(str(path_text or "")).name
            for idx, entry in enumerate(available):
                if normalized and entry["normalized_path"] == normalized:
                    return _pop_entry(idx)
            if path_name:
                for idx, entry in enumerate(available):
                    if Path(entry["img_path"]).name == path_name:
                        return _pop_entry(idx)
            return None

        def _take_by_timestamp(timestamp_sec: float) -> Optional[Dict[str, Any]]:
            best_index = -1
            best_delta = float("inf")
            for idx, entry in enumerate(available):
                entry_ts = float(entry.get("timestamp", float("inf")))
                if entry_ts == float("inf"):
                    continue
                delta = abs(entry_ts - timestamp_sec)
                if delta < best_delta:
                    best_delta = delta
                    best_index = idx
            if best_index < 0:
                return None
            return _pop_entry(best_index)

        ordered_embeds: List[str] = []
        embed_map: Dict[str, str] = {}
        seen_paths: set[str] = set()

        for segment in segments:
            keyframes = segment.get("instructional_keyframes")
            if not isinstance(keyframes, list):
                continue
            for keyframe in keyframes:
                candidate: Optional[Dict[str, Any]] = None
                frame_reason = ""
                explicit_path = ""
                timestamp_sec = None
                keyframe_id = ""
                if isinstance(keyframe, dict):
                    frame_reason = str(keyframe.get("frame_reason", "") or "").strip()
                    keyframe_id = self._normalize_keyframe_id(
                        keyframe.get("keyframe_id", keyframe.get("keyframeId"))
                    )
                    if not keyframe_id:
                        keyframe_id = f"KEYFRAME_{len(ordered_embeds) + 1}"
                    explicit_path = str(
                        keyframe.get("image_path")
                        or keyframe.get("image_file")
                        or keyframe.get("img_path")
                        or ""
                    ).strip()
                    try:
                        timestamp_sec = float(keyframe.get("timestamp_sec"))
                    except Exception:
                        timestamp_sec = None

                if keyframe_id:
                    candidate = _take_by_keyframe_id(keyframe_id)

                if explicit_path:
                    candidate = candidate or _take_by_path(explicit_path)
                    if candidate is None:
                        candidate = {
                            "item": {
                                "img_path": explicit_path,
                                "frame_reason": frame_reason,
                            },
                            "img_path": explicit_path,
                            "normalized_path": self._normalize_embed_path(explicit_path),
                            "keyframe_id": keyframe_id,
                        }

                if candidate is None and timestamp_sec is not None:
                    candidate = _take_by_timestamp(timestamp_sec)

                if candidate is None and available:
                    candidate = _pop_entry(0)

                if candidate is None:
                    continue

                candidate_item = dict(candidate.get("item", {}) or {})
                img_path = str(candidate_item.get("img_path", "") or "").strip()
                if not img_path:
                    img_path = str(candidate.get("img_path", "") or "").strip()
                if not img_path:
                    continue
                normalized_path = self._normalize_embed_path(img_path)
                if normalized_path and normalized_path in seen_paths:
                    continue
                candidate_keyframe_id = self._normalize_keyframe_id(
                    candidate_item.get("keyframe_id", candidate.get("keyframe_id"))
                )
                if not keyframe_id:
                    keyframe_id = candidate_keyframe_id
                alias = frame_reason or str(candidate_item.get("frame_reason", "") or "").strip()
                embed = self._format_obsidian_embed(img_path, alias=alias)
                if not embed:
                    continue
                ordered_embeds.append(embed)
                if keyframe_id and keyframe_id not in embed_map:
                    embed_map[keyframe_id] = embed
                if normalized_path:
                    seen_paths.add(normalized_path)

        return ordered_embeds, embed_map

    def _build_concrete_keyframe_embeds_by_segment_order(
        self,
        section: EnhancedSection,
        screenshot_items: List[Dict[str, Any]],
    ) -> List[str]:
        ordered, _ = self._build_concrete_keyframe_embeds_bundle_by_segment_order(
            section,
            screenshot_items,
        )
        return ordered

    @staticmethod
    def _build_sequential_keyframe_embed_map(keyframe_embeds: List[str]) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for index, embed in enumerate(keyframe_embeds, start=1):
            embed_text = str(embed or "").strip()
            if not embed_text:
                continue
            mapping[f"KEYFRAME_{index}"] = embed_text
        return mapping

    def _build_concrete_keyframe_embeds_for_section(
        self,
        section: EnhancedSection,
        screenshot_items: List[Dict[str, Any]],
    ) -> Tuple[List[str], Dict[str, str]]:
        ordered, ordered_map = self._build_concrete_keyframe_embeds_bundle_by_segment_order(
            section,
            screenshot_items,
        )
        # concrete 单元只保留“instructional_keyframes 顺序回填”单路径，禁用其他回填分支。
        if self._normalize_knowledge_type(section.knowledge_type) == "concrete":
            merged_map = self._build_sequential_keyframe_embed_map(ordered)
            for keyframe_id, embed in ordered_map.items():
                merged_map[keyframe_id] = embed
            return ordered, merged_map

        fallback = self._build_concrete_keyframe_embeds(screenshot_items)
        if not ordered:
            return fallback, self._build_sequential_keyframe_embed_map(fallback)

        merged = list(ordered)
        seen_paths = self._extract_obsidian_embed_paths("\n".join(merged))
        for embed in fallback:
            embed_paths = self._extract_obsidian_embed_paths(embed)
            embed_path = next(iter(embed_paths), "")
            if embed_path and embed_path in seen_paths:
                continue
            merged.append(embed)
            if embed_path:
                seen_paths.add(embed_path)
        merged_map = self._build_sequential_keyframe_embed_map(merged)
        for keyframe_id, embed in ordered_map.items():
            merged_map[keyframe_id] = embed
        return merged, merged_map

    def _append_missing_image_embeds(self, content: str, screenshot_items: List[Dict[str, Any]]) -> str:
        if not screenshot_items:
            return content

        missing: List[str] = []
        existing_embed_paths = self._extract_obsidian_embed_paths(content)
        for item in screenshot_items:
            img_path = str(item.get("img_path", "") or "").strip()
            if not img_path:
                continue
            frame_reason = str(item.get("frame_reason", "") or "").strip()
            embed = self._format_obsidian_embed(img_path, alias=frame_reason)
            if not embed:
                continue
            embed_paths = self._extract_obsidian_embed_paths(embed)
            embed_path = next(iter(embed_paths), "")
            if embed_path:
                if embed_path in existing_embed_paths:
                    continue
                existing_embed_paths.add(embed_path)
            elif embed in content:
                continue
            desc = frame_reason or str(item.get("img_description", "") or "").strip()
            if desc:
                missing.append(f"- {desc}: {embed}")
            else:
                missing.append(f"- {embed}")

        if not missing:
            return content

        base = content or ""
        if base and not base.endswith("\n"):
            base += "\n"
        return base + "\n" + "Supplemental images:\n" + "\n".join(missing)

    def _append_missing_clip_embeds(self, content: str, clip_items: List[Dict[str, Any]]) -> str:
        if not clip_items:
            return content

        missing: List[str] = []
        existing_embed_paths = self._extract_obsidian_embed_paths(content)
        for item in clip_items:
            clip_path = str(item.get("clip_path", "") or "").strip()
            if not clip_path:
                continue
            clip_reason = str(item.get("clip_reason", "") or "").strip()
            embed = self._format_obsidian_embed(clip_path, alias=clip_reason)
            if not embed:
                continue
            embed_paths = self._extract_obsidian_embed_paths(embed)
            embed_path = next(iter(embed_paths), "")
            if embed_path:
                if embed_path in existing_embed_paths:
                    continue
                existing_embed_paths.add(embed_path)
            elif embed in content:
                continue
            missing.append(embed)

        if not missing:
            return content

        base = content or ""
        if base and not base.endswith("\n"):
            base += "\n"
        return base + "\n" + "Supplemental clips:\n" + "\n".join(missing)

    def _build_deterministic_text_for_non_abstract(self, section: EnhancedSection) -> str:
        """
        Deterministic fallback for concrete/process sections.
        - Do not call the structured LLM here.
        - Preserve image and clip placeholder order.
        """
        base_text = self._resolve_concrete_base_text(section)
        normalized_kt = self._normalize_knowledge_type(section.knowledge_type)
        image_items = self._build_concept_image_items(section)
        clip_items = self._build_concept_clip_items(section)
        has_keyframe_placeholder = bool(
            re.search(r"\[\s*KEYFRAME_\d+\s*\]", base_text, flags=re.IGNORECASE)
        )
        has_clip_placeholder = bool(
            re.search(r"\[\s*CLIP_\d+\s*\]", base_text, flags=re.IGNORECASE)
        )

        if normalized_kt == "concrete" and (has_keyframe_placeholder or has_clip_placeholder):
            keyframe_embeds, keyframe_embed_map = self._build_concrete_keyframe_embeds_for_section(
                section,
                image_items,
            )
            clip_embeds = self._build_concrete_clip_embeds_by_segment_order(section, clip_items)
            structured = self._replace_tutorial_keyframe_placeholders(
                base_text,
                keyframe_embeds,
                keyframe_embed_map=keyframe_embed_map,
            )
            structured = self._replace_clip_placeholders(structured, clip_embeds)
            structured = self._replace_image_placeholders(structured, image_items)
            structured = self._replace_tutorial_legacy_placeholders(structured, keyframe_embeds)
            structured = self._strip_imgneeded_placeholders(structured).strip()
            structured = self._append_missing_clip_embeds(structured or base_text, clip_items)
            return structured or base_text

        keyframe_embeds, _ = self._build_concrete_keyframe_embeds_for_section(section, image_items)
        clip_embeds = self._build_concrete_clip_embeds_by_segment_order(section, clip_items)
        structured = base_text
        structured = self._replace_clip_placeholders(structured, clip_embeds)
        structured = self._replace_image_placeholders(structured, image_items)
        structured = self._replace_tutorial_legacy_placeholders(structured, keyframe_embeds)
        structured = self._strip_imgneeded_placeholders(structured).strip()
        structured = self._append_missing_clip_embeds(structured or base_text, clip_items)
        if not image_items:
            return structured or base_text
        return self._append_missing_image_embeds(structured or base_text, image_items)

    async def _build_structured_text_for_concept(
        self, section: EnhancedSection,
        prev_title: str = "", next_title: str = "",
    ) -> str:
        base_text = self._resolve_concrete_base_text(section)
        normalized_kt = self._normalize_knowledge_type(section.knowledge_type)
        image_items = self._build_concept_image_items(section)
        clip_items = self._build_concept_clip_items(section)
        has_keyframe_placeholder = bool(
            re.search(r"\[\s*KEYFRAME_\d+\s*\]", base_text, flags=re.IGNORECASE)
        )
        has_clip_placeholder = bool(
            re.search(r"\[\s*CLIP_\d+\s*\]", base_text, flags=re.IGNORECASE)
        )

        if normalized_kt == "concrete" and (has_keyframe_placeholder or has_clip_placeholder):
            keyframe_embeds, keyframe_embed_map = self._build_concrete_keyframe_embeds_for_section(
                section,
                image_items,
            )
            clip_embeds = self._build_concrete_clip_embeds_by_segment_order(section, clip_items)
            structured = self._replace_tutorial_keyframe_placeholders(
                base_text,
                keyframe_embeds,
                keyframe_embed_map=keyframe_embed_map,
            )
            structured = self._replace_clip_placeholders(structured, clip_embeds)
            structured = self._replace_image_placeholders(structured, image_items)
            structured = self._replace_tutorial_legacy_placeholders(structured, keyframe_embeds)
            structured = self._strip_imgneeded_placeholders(structured).strip()
            structured = self._append_missing_clip_embeds(structured or base_text, clip_items)
            return structured or base_text

        if normalized_kt != "concrete":
            augment_image_items = self._build_augment_image_items(section)
            base_text = await self._augment_body_with_image_descriptions(section, base_text, augment_image_items)

        image_context = "(none)"
        if image_items:
            image_context = "\n".join(
                [f"- img_id={item['img_id']} | img_description={item['img_description']}" for item in image_items]
            )

        adjacent_parts = []
        if prev_title:
            adjacent_parts.append(f"- Previous section: {prev_title}")
        if next_title:
            adjacent_parts.append(f"- Next section: {next_title}")
        adjacent_context = "\n".join(adjacent_parts) if adjacent_parts else "(none)"

        if not self._enabled or not self._llm_client:
            structured = self._append_missing_clip_embeds(base_text, clip_items)
            return self._append_missing_image_embeds(structured, image_items)

        structured_system_prompt = self._structured_system_prompt
        structured_user_prompt_template = self._structured_user_prompt_template
        if normalized_kt == "concrete":
            structured_system_prompt = (
                self._structured_system_preserve_img_prompt
                or self._structured_system_prompt
            )
            structured_user_prompt_template = (
                self._structured_user_preserve_img_prompt_template
                or self._structured_user_prompt_template
            )

        prompt = structured_user_prompt_template.format(
            title=section.title,
            knowledge_type=normalized_kt,
            body_text=base_text,
            image_context=image_context,
            adjacent_context=adjacent_context,
        )

        start_ts = time.perf_counter()
        try:
            content, meta, _ = await self._complete_text_with_model_fallback(
                prompt=prompt,
                system_message=structured_system_prompt,
                model=self._structured_text_model,
            )
            duration_ms = (time.perf_counter() - start_ts) * 1000.0
            await self._write_llm_trace_record(
                step_name="structured_text",
                unit_id=str(section.unit_id),
                system_prompt=structured_system_prompt,
                user_prompt=prompt,
                response_text=content,
                duration_ms=duration_ms,
                success=True,
                metadata=meta,
            )
            structured = (content or "").strip() or base_text
        except Exception as exc:
            duration_ms = (time.perf_counter() - start_ts) * 1000.0
            await self._write_llm_trace_record(
                step_name="structured_text",
                unit_id=str(section.unit_id),
                system_prompt=structured_system_prompt,
                user_prompt=prompt,
                response_text="",
                duration_ms=duration_ms,
                success=False,
                error_msg=str(exc),
            )
            logger.warning(f"Structured text generation failed for {section.unit_id}: {exc}")
            structured = base_text

        structured = self._replace_clip_placeholders(
            structured,
            self._build_concrete_clip_embeds_by_segment_order(section, clip_items),
        )
        structured = self._append_missing_clip_embeds(structured, clip_items)
        if not image_items:
            structured = self._strip_imgneeded_placeholders(structured)
            return structured

        structured = self._replace_image_placeholders(structured, image_items)
        structured = self._strip_imgneeded_placeholders(structured)
        return self._append_missing_image_embeds(structured, image_items)

    def _render_tutorial_steps(self, section: EnhancedSection) -> List[str]:
        steps = section.tutorial_steps or []
        if not steps:
            return []

        lines: List[str] = []
        main_flow_index = 0
        for step in steps:
            if not isinstance(step, dict):
                continue

            step_type = self._normalize_tutorial_step_type(step.get("step_type"))
            desc = str(step.get("step_description") or step.get("title") or "").strip() or "Untitled step"
            if step_type == "CONDITIONAL":
                lines.append(f"> [!NOTE] Conditional step: {desc}")
            elif step_type == "OPTIONAL":
                lines.append(f"> [!NOTE] Optional step: {desc}")
            elif step_type == "TROUBLESHOOTING":
                lines.append(f"> [!WARNING] Troubleshooting: {desc}")
            else:
                main_flow_index += 1
                lines.append(f"#### {main_flow_index}.{desc}")

            keyframe_entries = step.get("instructional_keyframes") or []
            if not isinstance(keyframe_entries, list):
                keyframe_entries = []
            keyframe_embeds: List[str] = []
            seen_keyframe_paths: set[str] = set()
            for item in keyframe_entries:
                image_path = ""
                frame_reason = ""
                if isinstance(item, dict):
                    image_path = str(item.get("image_path") or item.get("image_file") or "").strip()
                    frame_reason = str(item.get("frame_reason") or "").strip()
                elif isinstance(item, str):
                    image_path = str(item).strip()
                if not image_path:
                    continue
                normalized_image_path = self._normalize_embed_path(image_path)
                if normalized_image_path and normalized_image_path in seen_keyframe_paths:
                    continue
                embed = self._format_obsidian_embed(image_path, alias=frame_reason)
                if embed:
                    caption = re.sub(r"[\r\n]+", " ", frame_reason).strip()
                    if caption:
                        keyframe_embeds.append(f"- {caption}: {embed}")
                    else:
                        keyframe_embeds.append(embed)
                    if normalized_image_path:
                        seen_keyframe_paths.add(normalized_image_path)

            clip_entries = step.get("instructional_clips") or []
            if not isinstance(clip_entries, list):
                clip_entries = []
            clip_embeds: List[str] = []
            clip_embed_map: Dict[str, str] = {}
            seen_clip_paths: set[str] = set()
            for item in clip_entries:
                clip_path = ""
                clip_reason = ""
                clip_id = ""
                if isinstance(item, dict):
                    clip_path = str(item.get("clip_path") or item.get("clip_file") or "").strip()
                    clip_reason = str(item.get("clip_reason") or "").strip()
                    clip_id = self._normalize_clip_id(item.get("clip_id", item.get("instructional_clip_id")))
                elif isinstance(item, str):
                    clip_path = str(item).strip()
                if not clip_path:
                    continue
                normalized_clip_path = self._normalize_embed_path(clip_path)
                if normalized_clip_path and normalized_clip_path in seen_clip_paths:
                    continue
                embed = self._format_obsidian_embed(clip_path, alias=clip_reason)
                if not embed:
                    continue
                clip_embeds.append(embed)
                if clip_id and clip_id not in clip_embed_map:
                    clip_embed_map[clip_id] = embed
                if normalized_clip_path:
                    seen_clip_paths.add(normalized_clip_path)

            raw_main_operation = step.get("main_operation")
            if raw_main_operation is None:
                raw_main_operation = step.get("main_operations")
            if isinstance(raw_main_operation, list):
                main_operation = "\n".join(
                    [str(item or "").strip() for item in raw_main_operation if str(item or "").strip()]
                ).strip()
            else:
                main_operation = str(raw_main_operation or "").strip()

            if not main_operation:
                main_operation = str(step.get("main_action") or "").strip()

            has_keyframe_placeholder = bool(
                re.search(r"\[\s*KEYFRAME_\d+\s*\]", main_operation, flags=re.IGNORECASE)
            )
            has_clip_placeholder = bool(
                re.search(r"\[\s*CLIP_\d+\s*\]", main_operation, flags=re.IGNORECASE)
            )
            rendered_operation = self._replace_tutorial_keyframe_placeholders(
                main_operation,
                keyframe_embeds,
            )
            rendered_operation = self._replace_clip_placeholders(
                rendered_operation,
                clip_embeds,
                clip_embed_map=clip_embed_map,
            )
            rendered_operation = self._replace_tutorial_legacy_placeholders(
                rendered_operation,
                keyframe_embeds,
            )
            rendered_operation = self._strip_imgneeded_placeholders(rendered_operation).strip()

            if rendered_operation:
                operation_lines = rendered_operation.splitlines()
                rendered_embed_paths = self._extract_obsidian_embed_paths(rendered_operation)
                if keyframe_embeds and not has_keyframe_placeholder:
                    for embed in keyframe_embeds:
                        embed_paths = self._extract_obsidian_embed_paths(embed)
                        embed_path = next(iter(embed_paths), "")
                        if embed_path:
                            if embed_path in rendered_embed_paths:
                                continue
                            rendered_embed_paths.add(embed_path)
                        elif embed in rendered_operation:
                            continue
                        operation_lines.append(embed)
                if clip_embeds and not has_clip_placeholder:
                    for embed in clip_embeds:
                        embed_paths = self._extract_obsidian_embed_paths(embed)
                        embed_path = next(iter(embed_paths), "")
                        if embed_path:
                            if embed_path in rendered_embed_paths:
                                continue
                            rendered_embed_paths.add(embed_path)
                        elif embed in rendered_operation:
                            continue
                        operation_lines.append(embed)
                if step_type == "MAIN_FLOW":
                    lines.extend(operation_lines)
                else:
                    lines.extend(self._quote_lines(operation_lines))
            elif keyframe_embeds or clip_embeds:
                fallback_embeds = list(keyframe_embeds) + list(clip_embeds)
                if step_type == "MAIN_FLOW":
                    lines.extend(fallback_embeds)
                else:
                    lines.extend(self._quote_lines(fallback_embeds))

            if not clip_embeds:
                clip_path = str(step.get("clip_file") or step.get("clip_path") or "").strip()
                if clip_path:
                    clip_embed = self._format_obsidian_embed(clip_path)
                    if step_type == "MAIN_FLOW":
                        lines.append(clip_embed)
                    else:
                        lines.extend(self._quote_lines([clip_embed]))
            lines.append("")

        return lines

    async def _enhance_and_extract(self, section: EnhancedSection) -> Tuple[str, str]:
        """
        做什么：一次 LLM 调用同时完成「正文增强」与「逻辑结构化」。
        为什么：减少 LLM 调用次数（2 次 -> 1 次），在 DeepSeek 成为瓶颈时可显著降低端到端时延与成本。
        权衡：合并提示词会让单次请求更长；若模型输出不稳定则回退到两次调用路径。
        """
        if not self._enabled or not self._llm_client:
            return section.original_body, section.original_body

        # 构建动作单元信息（合并版，尽量覆盖增强与结构化所需证据）
        action_info_list = []
        for ac in section.action_classifications:
            kt = ac.get("knowledge_type", "")
            if kt != "讲解型":
                action_info_list.append(
                    f"- [{kt}] {ac.get('subject', '')} - {ac.get('description', '')}: {ac.get('key_evidence', '')}"
                )
        action_info = "\n".join(action_info_list) if action_info_list else "(无)"

        # TODO: 实际项目中应调用 OCR 服务
        ocr_text = "(OCR 功能待集成)"

        level_names = {1: "一级(核心知识点)", 2: "二级(子知识点)", 3: "三级(支撑信息)"}
        level_info = f"当前层级: {level_names.get(section.level, '二级')}"
        if section.parent_id:
            level_info += f", 父节点: {section.parent_id}"

        user_prompt = self._combined_user_prompt_template.format(
            title=section.title,
            level_info=level_info,
            body_text=section.original_body,
            ocr_text=ocr_text,
            action_info=action_info,
        )

        result, _, _ = await self._llm_client.complete_json(
            prompt=user_prompt,
            system_message=self._combined_system_prompt,
        )

        enhanced_body = (result.get("enhanced_body") or "").strip()
        structured_content = (result.get("structured_content") or "").strip()

        if not enhanced_body:
            enhanced_body = section.original_body
        
        # 移除可能存在的标题（避免 LLM 重复输出 # Title）
        enhanced_body = self._strip_header_title(enhanced_body, section.title)
        
        if not structured_content:
            structured_content = enhanced_body
        else:
            structured_content = self._strip_header_title(structured_content, section.title)

        return enhanced_body, structured_content

    def _strip_header_title(self, content: str, title: str) -> str:
        """
        移除内容开头的标题（如果存在），避免 Markdown 渲染重复标题。
        """
        if not content or not title:
            return content
        
        lines = content.strip().split('\n')
        if not lines:
            return content
        
        first_line = lines[0].strip()
        # 匹配模式：开头是 #，后面跟着标题（允许少量标点/空格差异）
        if first_line.startswith("#"):
            # 移除 markdown 标记和空白
            clean_first = first_line.lstrip("#").strip()
            # 简单模糊匹配：如果标题出现在第一行中
            if title.strip() in clean_first:
                return "\n".join(lines[1:]).strip()
        
        return content

    async def _enhance_text(self, section: EnhancedSection) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not self._enabled
        - 条件：ac.get('knowledge_type') != '讲解型'
        - 条件：action_info_list
        依据来源（证据链）：
        - 配置字段：knowledge_type。
        - 对象内部状态：self._enabled。
        输入参数：
        - section: 函数入参（类型：EnhancedSection）。
        输出参数：
        - 字符串结果。"""
        if not self._enabled:
            return section.original_body
        
        # 构建动作单元信息 (非讲解型)
        action_info_list = []
        for ac in section.action_classifications:
            if ac.get("knowledge_type") != "讲解型":
                action_info_list.append(
                    f"- [{ac.get('knowledge_type')}] {ac.get('subject')} - {ac.get('description')}: {ac.get('key_evidence', '')}"
                )
        
        action_info = "\n".join(action_info_list) if action_info_list else "(无)"
        
        # TODO: 实际项目中应调用 OCR 服务
        ocr_text = "(OCR 功能待集成)"
        
        prompt = self._text_enhance_prompt_template.format(
            body_text=section.original_body,
            ocr_text=ocr_text,
            action_info=action_info
        )
        
        try:
            content, _, _ = await self._llm_client.complete_text(
                prompt=prompt
            )
            return self._strip_header_title(content.strip(), section.title)
            
        except Exception as e:
            logger.error(f"Text enhancement failed: {e}")
            return section.original_body
    
    async def _extract_logic(self, section: EnhancedSection) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not self._enabled
        - 条件：section.parent_id
        - 条件：ac.get('knowledge_type') != '讲解型'
        依据来源（证据链）：
        - 输入参数：section。
        - 配置字段：knowledge_type。
        - 对象内部状态：self._enabled。
        输入参数：
        - section: 函数入参（类型：EnhancedSection）。
        输出参数：
        - 字符串结果。"""
        if not self._enabled:
            return section.enhanced_body
        
        # 构建动作单元信息 (非讲解型)
        action_info_list = []
        for ac in section.action_classifications:
            if ac.get("knowledge_type") != "讲解型":
                action_info_list.append(
                    f"- **{ac.get('knowledge_type')}**: {ac.get('key_evidence', '')}"
                )
        
        action_info = "\n".join(action_info_list) if action_info_list else "(无非讲解型动作)"
        
        level_names = {1: "一级(核心知识点)", 2: "二级(子知识点)", 3: "三级(支撑信息)"}
        level_info = f"当前层级: {level_names.get(section.level, '二级')}"
        if section.parent_id:
            level_info += f", 父节点: {section.parent_id}"
        
        prompt = self._logic_extract_prompt_template.format(
            title=section.title,
            body_text=section.enhanced_body,
            level_info=level_info,
            action_info=action_info
        )
        
        try:
            content, _, _ = await self._llm_client.complete_text(
                prompt=prompt
            )
            
            return content.strip()
            
        except Exception as e:
            logger.error(f"Logic extraction failed: {e}")
            return section.enhanced_body
    
    def _assemble_markdown(self, title: str, groups: List[EnhancedGroup]) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        输入参数：
        - title: 函数入参（类型：str）。
        - groups: 函数入参（类型：List[EnhancedGroup]）。
        输出参数：
        - 字符串结果。"""
        lines = []
        
        # 标题
        lines.append(f"# {title}")
        lines.append("")

        # 固定两级输出：group_name 作为一级，unit.title 作为二级。
        for group in groups:
            lines.append(f"## {group.group_name}")
            lines.append("")
            if str(group.reason or "").strip():
                lines.append(f"> 分组依据：{group.reason}")
                lines.append("")
            for section in group.units:
                lines.extend(self._render_section(section))
                lines.append("")
            lines.append("")

        while lines and not lines[-1].strip():
            lines.pop()
        lines.append("")

        return "\n".join(lines)

    def _format_obsidian_embed(self, file_path: str, alias: str = "") -> str:
        """
        生成 Obsidian 嵌入路径，优先使用 Markdown 目录的相对路径。
        """
        if not file_path:
            return ""

        def _preserve_assets_hierarchy(path_text: str) -> str:
            normalized = str(path_text).replace("\\", "/")
            marker = f"/{self._assets_dir}/"
            if marker in normalized:
                suffix = normalized.split(marker, 1)[1].strip("/")
                return f"{self._assets_dir}/{suffix}"
            rel_prefix = f"{self._assets_dir}/"
            if normalized.startswith(rel_prefix):
                return normalized
            return f"{self._assets_dir}/{Path(path_text).name}"

        if os.path.isabs(file_path):
            rel_path = ""
            if self._markdown_dir:
                try:
                    rel_path = os.path.relpath(file_path, self._markdown_dir)
                except Exception:
                    rel_path = ""
            if not rel_path:
                rel_path = _preserve_assets_hierarchy(file_path)
        else:
            rel_path = _preserve_assets_hierarchy(file_path)

        rel_path = rel_path.replace("\\", "/")
        safe_alias = str(alias or "").strip()
        if safe_alias:
            safe_alias = re.sub(r"[\r\n]+", " ", safe_alias)
            safe_alias = safe_alias.replace("|", "/").replace("[", "(").replace("]", ")").strip()
        if safe_alias:
            return f"![[{rel_path}|{safe_alias}]]"
        return f"![[{rel_path}]]"
    
    
    def _render_section(self, section: EnhancedSection) -> List[str]:
        lines: List[str] = []

        lines.append(f"### {section.title}")
        lines.append("")

        normalized_kt = self._normalize_knowledge_type(section.knowledge_type)
        if self._is_tutorial_process_section(section):
            lines.extend(self._render_tutorial_steps(section))
            return lines

        body = (section.structured_content or section.enhanced_body or section.original_body or "").strip()
        if body:
            lines.extend(body.split("\n"))
            lines.append("")

        # abstract/concrete: render structured body only.
        if normalized_kt in {"abstract", "concrete"}:
            return lines

        if section.video_clips:
            lines.append(f"> Video **{self._build_video_title(section)}**")
            lines.append("")
            for clip_item in section.video_clips:
                lines.append(self._format_obsidian_embed(clip_item))
                lines.append("")

        # process（非 tutorial）正文已完成图片占位替换，不再重复追加末尾图片块。
        if section.validated_screenshots and normalized_kt != "process":
            lines.append("> Images **Keyframes**")
            lines.append("")
            for img_path in section.validated_screenshots:
                lines.append(self._format_obsidian_embed(img_path))
            lines.append("")

        return lines

    def _build_video_title(self, section: EnhancedSection) -> str:
        """
        根据动作单元的知识类型生成视频标题，避免固定“操作演示”。
        """
        def normalize_kt(value: str) -> str:
            return (value or "").lower()

        def map_title(kt: str) -> str:
            if any(key in kt for key in ["讲解", "explanation", "abstract", "抽象"]):
                return "概念讲解"
            if any(key in kt for key in ["过程", "process"]):
                return "过程演示"
            if any(key in kt for key in ["具象", "concrete", "实例", "示例"]):
                return "实例演示"
            return "操作演示"

        # 优先使用动作单元分类结果
        if section.action_classifications:
            best = None
            best_conf = -1.0
            for item in section.action_classifications:
                conf = float(item.get("confidence", 0.0) or 0.0)
                if conf > best_conf:
                    best_conf = conf
                    best = item
            if best:
                kt = normalize_kt(best.get("knowledge_type", ""))
                return map_title(kt)

        # 兜底使用段落知识类型
        return map_title(normalize_kt(section.knowledge_type))


# ==============================================================================
# Test Entry
# ==============================================================================
