"""
Markdown 增强导出模块

功能:
1. HierarchyClassifier: 知识点层级划分 (一级/二级/三级)
2. TextEnhancer: 正文补全 (OCR + 指代消解)
3. LogicExtractor: 隐性逻辑提取 (Tab缩进 + 语义标签)
4. ConcreteKnowledgeValidator: 具象性知识验证 (公式/图形检测)

V2.0 - Obsidian 格式 + 具象性知识验证
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
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
# Data Classes
# ==============================================================================

@dataclass
class EnhancedSection:
    """增强后的语义单元"""
    unit_id: str
    title: str
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
    Markdown 增强导出器
    
    使用 LLM 进行:
    1. 知识点层级划分
    2. 正文补全增强
    3. 隐性逻辑提取
    """
    
    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None):
        """
        初始化 Markdown 增强器
        
        🚀 V3: 使用集中式 LLMClient (连接池+HTTP/2+自适应并发)
        """
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
    
    @property
    def enabled(self) -> bool:
        return self._enabled
    
    async def enhance(self, result_json_path: str, subject: str = "数据结构与算法") -> str:
        """
        增强 Markdown 导出
        
        Args:
            result_json_path: result.json 路径
            subject: 学科名称 (用于层级划分 prompt)
            
        Returns:
            增强后的 Markdown 内容
        """
        # 加载数据
        with open(result_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        sections = data.get("sections", [])
        title = data.get("title", "知识文档")
        
        if not sections:
            return "# 无内容"
        
        # Step 1: 层级划分
        logger.info("Step 1: Hierarchy Classification")
        hierarchy = await self._classify_hierarchy(sections, subject)
        
        # Step 2: 正文增强
        logger.info("Step 2: Text Enhancement")
        enhanced_sections = []
        for section in sections:
            unit_id = section.get("unit_id", "")
            level_info = hierarchy.get(unit_id, {"level": 2, "parent_id": None})
            
            materials = section.get("materials", {})
            
            enhanced = EnhancedSection(
                unit_id=unit_id,
                title=section.get("title", ""),
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
            
            # 增强正文
            enhanced.enhanced_body = await self._enhance_text(enhanced)
            
            enhanced_sections.append(enhanced)
        
        # Step 3: 逻辑提取
        logger.info("Step 3: Logic Extraction")
        for section in enhanced_sections:
            section.structured_content = await self._extract_logic(section)
        
        # Step 4: 组装 Markdown
        logger.info("Step 4: Assembling Markdown")
        markdown = self._assemble_markdown(title, enhanced_sections)
        
        return markdown
    
    async def _classify_hierarchy(self, sections: List[Dict], subject: str) -> Dict[str, Dict]:
        """知识点层级划分"""
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
            
            result = json.loads(content)
            
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
    
    async def _enhance_text(self, section: EnhancedSection) -> str:
        """正文补全增强"""
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
        """隐性逻辑提取"""
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
        """组装最终 Markdown"""
        lines = []
        
        # 标题
        lines.append(f"# {title}")
        lines.append("")
        
        # 按层级组织
        level1_sections = [s for s in sections if s.level == 1]
        
        if not level1_sections:
            # 没有一级标题，直接按顺序输出
            for section in sections:
                lines.extend(self._render_section(section))
                lines.append("")
        else:
            # 有一级标题，按层级嵌套
            for l1 in level1_sections:
                lines.extend(self._render_section(l1))
                lines.append("")
                
                # 找二级子节点
                l2_sections = [s for s in sections if s.parent_id == l1.unit_id]
                for l2 in l2_sections:
                    lines.extend(self._render_section(l2, indent=1))
                    lines.append("")
                    
                    # 找三级子节点
                    l3_sections = [s for s in sections if s.parent_id == l2.unit_id]
                    for l3 in l3_sections:
                        lines.extend(self._render_section(l3, indent=2))
                        lines.append("")
        
        return "\n".join(lines)
    
    
    def _render_section(self, section: EnhancedSection, indent: int = 0) -> List[str]:
        """渲染单个语义单元 (V2: Obsidian 格式)"""
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
        if section.video_clip and os.path.exists(section.video_clip):
            video_name = Path(section.video_clip).name
            lines.append(f"> 📹 **操作演示**")
            lines.append(f"")
            lines.append(f"![[{self._assets_dir}/{video_name}]]")
            lines.append("")
        
        # 验证后的截图
        if section.validated_screenshots:
            lines.append(f"> 🖼️ **关键帧**")
            lines.append("")
            for img_path in section.validated_screenshots:
                img_name = Path(img_path).name
                lines.append(f"![[{self._assets_dir}/{img_name}]]")
            lines.append("")
        
        return lines


# ==============================================================================
# Test Entry
# ==============================================================================
