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
    validated_screenshots: List[str] = field(default_factory=list)  # V2: 验证后的截图
    video_clip: str = ""                    # V2: 视频片段路径
    action_classifications: List[Dict] = field(default_factory=list)


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
                video_clip=materials.get("clip", ""),
                action_classifications=materials.get("action_classifications", [])
            )
            
            # V2: 验证截图是否包含具象性知识
            # enhanced.validated_screenshots = self._validate_screenshots(enhanced.screenshots)
            # 🚀 V3: 已经在 RichTextPipeline 中验证过，直接使用 (假设 screenshots 已过滤)
            enhanced.validated_screenshots = enhanced.screenshots
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
            indent = max(0, min(section.level - 1, 5))
            lines.extend(self._render_section(section, indent=indent))
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
    
    
    def _render_section(self, section: EnhancedSection, indent: int = 0) -> List[str]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：section.structured_content
        - 条件：section.video_clip and os.path.exists(section.video_clip)
        - 条件：section.validated_screenshots
        依据来源（证据链）：
        - 输入参数：section。
        输入参数：
        - section: 函数入参（类型：EnhancedSection）。
        - indent: 函数入参（类型：int）。
        输出参数：
        - str 列表（与输入或处理结果一一对应）。"""
        lines = []
        
        # 标题层级
        header_level = min(section.level + 1, 6)  # ## 开始
        header = "#" * header_level
        
        lines.append(f"{header} {section.title}")
        lines.append("")
        
        # 结构化内容
        if section.structured_content:
            # 添加缩进
            tab = "\t" * indent
            for line in section.structured_content.split("\n"):
                lines.append(f"{tab}{line}")
        else:
            lines.append(section.enhanced_body or section.original_body)
        
        lines.append("")
        
        # V2: Obsidian 格式媒体嵌入
        # 视频片段
        if section.video_clip:
            lines.append(f"> 📹 **{self._build_video_title(section)}**")
            lines.append(f"")
            lines.append(self._format_obsidian_embed(section.video_clip))
            lines.append("")
        
        # 验证后的截图
        if section.validated_screenshots:
            lines.append(f"> 🖼️ **关键帧**")
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
