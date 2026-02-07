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
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
# 🚀 使用集中式 LLMClient (连接池+HTTP/2+自适应并发)
import asyncio



logger = logging.getLogger(__name__)


# ==============================================================================
# Prompt Templates
# ==============================================================================

HIERARCHY_PROMPT = """你是{subject}学科的资深教师，擅长将教学内容按认知规律划分为三级层级。

## 任务
对以下语义单元标题进行层级划分:

{titles}

## 层级定义
- **一级层级（核心知识点）**: 教学的核心概念、定理、公式、核心结论
- **二级层级（子知识点）**: 对核心知识点的拆解、细化、关键特征
- **三级层级（支撑信息）**: 案例、举例、推导过程、易错点、解释说明

## 输出格式 (JSON)
{{
  "hierarchy": [
    {{"unit_id": "SU001", "level": 1, "parent_id": null}},
    {{"unit_id": "SU002", "level": 2, "parent_id": "SU001"}},
    {{"unit_id": "SU003", "level": 3, "parent_id": "SU002"}}
  ]
}}

请只输出JSON。"""


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
3. 通过 **Tab 缩进** 展现逻辑层次关系
4. 用 **-** 等 markdown 语法展现序列
5. 还原隐性逻辑
6. 将动作单元的 key_evidence 作为具体示例插入对应位置

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
- 通过 **Tab 缩进** 展现逻辑层次
- 使用 **-** 等 Markdown 语法展现序列
- 还原隐性逻辑
- 不要在内容中直接写“总分关系/因果关系”等显式描述词
- 将动作单元的 key_evidence 作为具体示例插入对应位置

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
4) 禁止使用其他占位符格式（例如 [IMG:img_id]、{IMG=img_id} 等）。
"""

STRUCTURED_TEXT_USER_PROMPT = """## 语义单元
- 标题: {title}
- 知识类型: {knowledge_type}

## 原始文本
{body_text}

## 图片候选（可为空）
{image_context}

请输出结构化 Markdown；若有图片候选，请根据图片描述把对应图片插入到匹配句子的末尾，
占位符必须使用【imgneeded_{{img_id}}】。"""
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
    validated_screenshots: List[str] = field(default_factory=list)  # V2: 验证后的截图
    video_clip: str = ""                    # V2: 视频片段路径
    action_classifications: List[Dict] = field(default_factory=list)
    mult_steps: bool = False
    tutorial_steps: List[Dict[str, Any]] = field(default_factory=list)


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
            from .llm_client import LLMClient
            self._llm_client = LLMClient(
                api_key=self.api_key,
                base_url=self.base_url + "/v1"  # LLMClient 需要 /v1 后缀
            )
        
        
        # V2: assets 目录 (用于 Obsidian 相对路径)
        self._assets_dir = "assets"
        # Obsidian 嵌入路径基准目录（默认使用输出 Markdown 所在目录）
        self._markdown_dir = None
        self._result_dir = None
        # 🚀 调用合并开关：默认开启，失败时自动回退到两次调用
        raw = (os.getenv("MODULE2_MARKDOWN_ENHANCER_COMBINE_CALLS", "1") or "").strip().lower()
        self._combine_llm_calls = raw in ("1", "true", "yes", "y", "on")
    
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
        
        sections = data.get("sections", [])
        title = data.get("title", "知识文档")
        
        if not sections:
            return "# 无内容"
        
        # Step 1: 层级划分
        logger.info("Step 1: Hierarchy Classification")
        hierarchy = await self._classify_hierarchy(sections, subject)
        
        # Step 2: 正文增强
        logger.info("Step 2: Text Enhancement (build tasks)")
        enhanced_sections = []
        for section in sections:
            unit_id = section.get("unit_id", "")
            level_info = hierarchy.get(unit_id, {"level": 2, "parent_id": None})
            
            materials = section.get("materials", {})
            
            enhanced = EnhancedSection(
                unit_id=unit_id,
                title=section.get("title", ""),
                knowledge_type=section.get("knowledge_type", ""),
                level=level_info.get("level", 2),
                parent_id=level_info.get("parent_id"),
                original_body=section.get("body_text", ""),
                screenshots=materials.get("screenshots", []),
                screenshot_items=materials.get("screenshot_items", []),
                video_clip=materials.get("clip", ""),
                action_classifications=materials.get("action_classifications", []),
                mult_steps=bool(section.get("mult_steps", False)),
            )
            
            # V2: 截图验证由上游完成，这里直接使用过滤后的截图。
            # enhanced.validated_screenshots = self._validate_screenshots(enhanced.screenshots)
            # V3: RichTextPipeline 已做过验证，避免重复调用。
            enhanced.validated_screenshots = enhanced.screenshots
            enhanced.tutorial_steps = self._load_tutorial_steps(
                unit_id=unit_id,
                inline_steps=section.get("instructional_steps", []),
            )
            enhanced_sections.append(enhanced)

        # 层级兜底：修复无父级/无一级的情况，保证 Obsidian 能显示嵌套
        self._normalize_hierarchy(enhanced_sections)

        # Step 2-3: 正文增强 + 逻辑提取（并行提交，按完成顺序流式处理）
        logger.info("Step 2-3: Parallel LLM Enhance + Extract (as_completed)")

        async def _process_one(idx: int, sec: EnhancedSection) -> int:
            """
            做什么：对单个语义单元执行“正文增强 -> 逻辑提取”两步。
            为什么：两步互相依赖，但不同语义单元之间可并行，从而降低单任务总时延。
            权衡：并行会增加瞬时 in-flight，请确保 LLMClient 的调度器生效（token 加权 + 资源 cap）。
            """
            normalized_kt = self._normalize_knowledge_type(sec.knowledge_type)

            if self._is_tutorial_process_section(sec):
                # 教程型 process 直接走步骤渲染，不走通用逻辑抽取。
                sec.enhanced_body = sec.original_body
                sec.structured_content = ""
                return idx

            if normalized_kt in {"abstract", "concrete"}:
        # abstract/concrete: render structured body only.
                sec.enhanced_body = sec.original_body
                sec.structured_content = await self._build_structured_text_for_concept(sec)
                return idx

            if normalized_kt == "process":
                # process（非 tutorial_stepwise）走与 abstract/concrete 一致的结构化插图链路：
                # 1) DeepSeek 结构化正文
                # 2) 【imgneeded_{img_id}】占位替换
                # 3) 缺失图片兜底追加
                sec.enhanced_body = sec.original_body
                sec.structured_content = await self._build_structured_text_for_concept(sec)
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

        tasks = [asyncio.create_task(_process_one(i, s)) for i, s in enumerate(enhanced_sections)]
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
        
        # Step 4: 组装 Markdown
        logger.info("Step 4: Assembling Markdown")
        markdown = self._assemble_markdown(title, enhanced_sections)
        
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
        
        prompt = HIERARCHY_PROMPT.format(subject=subject, titles=titles)
        try:
            # 🚀 使用 LLMClient 进行异步调用
            content, _, _ = await self._llm_client.complete_text(
                prompt=prompt
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
        return bool(section.mult_steps or len(section.tutorial_steps) > 1)

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

        def _normalize_step(raw_step: Dict[str, Any], order: int, base_dir: Optional[Path] = None) -> Dict[str, Any]:
            step_id = _safe_int(raw_step.get("step_id", order), order)
            step_desc = str(
                raw_step.get("step_description")
                or raw_step.get("description")
                or raw_step.get("title")
                or f"step_{step_id}"
            ).strip()

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

            keyframe_files = raw_step.get("instructional_keyframes")
            if not isinstance(keyframe_files, list):
                keyframe_files = []
            if not keyframe_files:
                material_images = materials.get("screenshot_paths") or materials.get("screenshots")
                if isinstance(material_images, list):
                    keyframe_files = material_images

            return {
                "step_id": step_id,
                "step_description": step_desc,
                "action_brief": str(raw_step.get("action_brief", "") or "").strip(),
                "clip_start_sec": clip_start,
                "clip_end_sec": clip_end,
                "instructional_keyframe_timestamp": _extract_timestamps(raw_step),
                "clip_file": _to_abs(clip_file, base_dir=base_dir),
                "instructional_keyframes": [
                    _to_abs(path_item, base_dir=base_dir)
                    for path_item in keyframe_files
                    if str(path_item or "").strip()
                ],
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

                for idx, raw in enumerate(manifest_steps, start=1):
                    if not isinstance(raw, dict):
                        continue
                    normalized = _normalize_step(raw, idx, base_dir=base_dir)
                    existed = by_step.get(normalized["step_id"])
                    if not existed:
                        by_step[normalized["step_id"]] = normalized
                        continue
                    existed["step_description"] = normalized["step_description"] or existed["step_description"]
                    existed["action_brief"] = normalized["action_brief"] or existed["action_brief"]
                    existed["clip_start_sec"] = normalized["clip_start_sec"]
                    existed["clip_end_sec"] = normalized["clip_end_sec"]
                    if normalized["clip_file"]:
                        existed["clip_file"] = normalized["clip_file"]
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
            normalized.append(
                {
                    "img_id": img_id,
                    "img_path": img_path,
                    "img_description": img_description,
                }
            )
        return normalized

    def _replace_image_placeholders(self, content: str, screenshot_items: List[Dict[str, Any]]) -> str:
        if not content or not screenshot_items:
            return content

        def _normalize_img_id(raw_id: Any) -> str:
            value = str(raw_id or "").strip().strip("`'[]{}()<>")
            value = re.sub(r"[^A-Za-z0-9_\-]", "", value)
            return value.lower()

        by_id: Dict[str, Dict[str, Any]] = {}
        for item in screenshot_items:
            if not isinstance(item, dict):
                continue
            img_id = _normalize_img_id(item.get("img_id", ""))
            if not img_id:
                continue
            by_id[img_id] = item

        if not by_id:
            return content

        # 仅支持新占位符格式：【imgneeded_{img_id}】
        pattern = re.compile(
            r"【\s*imgneeded_([A-Za-z0-9_\-]+)\s*】",
            flags=re.IGNORECASE,
        )

        def _replace(match: re.Match[str]) -> str:
            img_id = _normalize_img_id(match.group(1))
            item = by_id.get(img_id)
            if not item:
                return match.group(0)
            img_path = str(item.get("img_path", "") or "").strip()
            if not img_path:
                return match.group(0)
            return self._format_obsidian_embed(img_path)

        return pattern.sub(_replace, content)

    def _append_missing_image_embeds(self, content: str, screenshot_items: List[Dict[str, Any]]) -> str:
        if not screenshot_items:
            return content

        missing: List[str] = []
        for item in screenshot_items:
            embed = self._format_obsidian_embed(str(item.get("img_path", "") or ""))
            if not embed or embed in content:
                continue
            desc = str(item.get("img_description", "") or "").strip()
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

    async def _build_structured_text_for_concept(self, section: EnhancedSection) -> str:
        base_text = (section.original_body or "").strip()
        image_items = self._build_concept_image_items(section)

        image_context = "(none)"
        if image_items:
            image_context = "\n".join(
                [f"- img_id={item['img_id']} | img_description={item['img_description']}" for item in image_items]
            )

        if not self._enabled or not self._llm_client:
            return self._append_missing_image_embeds(base_text, image_items)

        prompt = STRUCTURED_TEXT_USER_PROMPT.format(
            title=section.title,
            knowledge_type=self._normalize_knowledge_type(section.knowledge_type),
            body_text=base_text,
            image_context=image_context,
        )

        try:
            content, _, _ = await self._llm_client.complete_text(
                prompt=prompt,
                system_message=STRUCTURED_TEXT_SYSTEM_PROMPT,
            )
            structured = (content or "").strip() or base_text
        except Exception as exc:
            logger.warning(f"Structured text generation failed for {section.unit_id}: {exc}")
            structured = base_text

        structured = self._replace_image_placeholders(structured, image_items)
        return self._append_missing_image_embeds(structured, image_items)

    def _render_tutorial_steps(self, section: EnhancedSection) -> List[str]:
        steps = section.tutorial_steps or []
        if not steps:
            return []

        lines: List[str] = []
        for order, step in enumerate(steps, start=1):
            step_id = int(step.get("step_id", order) or order)
            desc = str(step.get("step_description", "") or f"step_{step_id}").strip()
            start_sec = float(step.get("clip_start_sec", 0.0) or 0.0)
            end_sec = float(step.get("clip_end_sec", start_sec) or start_sec)
            if end_sec < start_sec:
                start_sec, end_sec = end_sec, start_sec

            lines.append(f"{order}. {step_id}. {desc}: from {start_sec:.2f}s to {end_sec:.2f}s")

            keyframes = step.get("instructional_keyframes") or []
            timestamps = step.get("instructional_keyframe_timestamp") or []
            if keyframes:
                for idx, key_path in enumerate(keyframes, start=1):
                    suffix = ""
                    if idx <= len(timestamps):
                        try:
                            suffix = f" ({float(timestamps[idx - 1]):.2f}s)"
                        except Exception:
                            suffix = ""
                    lines.append(f"    - Keyframe {idx}{suffix}: {self._format_obsidian_embed(str(key_path))}")
            elif timestamps:
                for idx, ts in enumerate(timestamps, start=1):
                    lines.append(f"    - Keyframe {idx}: {float(ts):.2f}s")

            clip_path = str(step.get("clip_file") or step.get("clip_path") or "").strip()
            if clip_path:
                lines.append(f"    - Step video: {self._format_obsidian_embed(clip_path)}")

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

        user_prompt = COMBINED_USER_PROMPT.format(
            title=section.title,
            level_info=level_info,
            body_text=section.original_body,
            ocr_text=ocr_text,
            action_info=action_info,
        )

        result, _, _ = await self._llm_client.complete_json(
            prompt=user_prompt,
            system_message=COMBINED_SYSTEM_PROMPT,
        )

        enhanced_body = (result.get("enhanced_body") or "").strip()
        structured_content = (result.get("structured_content") or "").strip()

        if not enhanced_body:
            enhanced_body = section.original_body
        if not structured_content:
            structured_content = enhanced_body

        return enhanced_body, structured_content

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
        
        prompt = TEXT_ENHANCE_PROMPT.format(
            body_text=section.original_body,
            ocr_text=ocr_text,
            action_info=action_info
        )
        
        try:
            content, _, _ = await self._llm_client.complete_text(
                prompt=prompt
            )
            return content.strip()
            
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
        
        prompt = LOGIC_EXTRACT_PROMPT.format(
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
    
    def _assemble_markdown(self, title: str, sections: List[EnhancedSection]) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not level1_sections
        依据来源（证据链）：
        输入参数：
        - title: 函数入参（类型：str）。
        - sections: 函数入参（类型：List[EnhancedSection]）。
        输出参数：
        - 字符串结果。"""
        lines = []
        
        # 标题
        lines.append(f"# {title}")
        lines.append("")
        
        # 按原始顺序输出，并用标题层级体现嵌套
        for section in sections:
            # 移除缩进计算，直接渲染
            lines.extend(self._render_section(section))
            lines.append("")
        
        return "\n".join(lines)

    def _format_obsidian_embed(self, file_path: str) -> str:
        """
        生成 Obsidian 嵌入路径，优先使用 Markdown 目录的相对路径。
        """
        if not file_path:
            return ""

        if os.path.isabs(file_path):
            rel_path = ""
            if self._markdown_dir:
                try:
                    rel_path = os.path.relpath(file_path, self._markdown_dir)
                except Exception:
                    rel_path = ""
            if not rel_path:
                rel_path = f"{self._assets_dir}/{Path(file_path).name}"
        else:
            rel_path = file_path

        rel_path = rel_path.replace("\\", "/")
        return f"![[{rel_path}]]"
    
    
    def _render_section(self, section: EnhancedSection) -> List[str]:
        lines: List[str] = []

        header_level = min(section.level + 1, 6)
        header = "#" * header_level
        lines.append(f"{header} {section.title}")
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

        if section.video_clip:
            lines.append(f"> Video **{self._build_video_title(section)}**")
            lines.append("")
            lines.append(self._format_obsidian_embed(section.video_clip))
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
