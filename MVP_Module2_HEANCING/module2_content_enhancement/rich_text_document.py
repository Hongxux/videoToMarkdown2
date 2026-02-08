"""
模块说明：Module2 内容增强中的 rich_text_document 模块。
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
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Tuple, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class MaterialSet:
    """
    类说明：封装 MaterialSet 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    clip_path: Optional[str] = None           # 视频片段路径
    clip_paths: List[str] = field(default_factory=list)
    screenshot_paths: List[str] = field(default_factory=list)  # 截图路径列表
    
    # 元信息
    # 元信息
    screenshot_labels: List[str] = field(default_factory=list)  # 截图标签 ["首帧", "稳定岛", "末帧"]
    screenshot_items: List[Dict[str, Any]] = field(default_factory=list)  # 截图元信息（img_id/img_description/path）
    
    # V7.4: LLM 三要素分类结果
    action_classifications: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class RichTextSection:
    """
    类说明：封装 RichTextSection 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    # 基础信息
    unit_id: str
    title: str                                # knowledge_topic
    body_text: str                            # full_text (cleaned from step6)
    knowledge_type: str                       # abstract / process / concrete
    
    # 时间范围
    start_sec: float
    end_sec: float
    
    # 素材
    # 素材
    materials: MaterialSet = field(default_factory=MaterialSet)
    
    # V8: Instructional Steps (for process type)
    instructional_steps: List[Dict[str, Any]] = field(default_factory=list)
    mult_steps: bool = False
    layout_hint: str = "default"

    def duration_str(self) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - 无。
        输出参数：
        - 字符串结果。"""
        def fmt(sec):
            """
            执行逻辑：
            1) 准备必要上下文与参数。
            2) 执行核心处理并返回结果。
            实现方式：通过内部函数组合与条件判断实现。
            核心价值：封装逻辑单元，提升复用与可维护性。
            输入参数：
            - sec: 函数入参（类型：未标注）。
            输出参数：
            - 函数计算/封装后的结果对象。"""
            m, s = divmod(int(sec), 60)
            return f"{m}:{s:02d}"
        return f"{fmt(self.start_sec)} - {fmt(self.end_sec)}"


@dataclass
class RichTextDocument:
    """
    类说明：封装 RichTextDocument 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    title: str = ""
    sections: List[RichTextSection] = field(default_factory=list)
    
    # 元信息
    source_video: str = ""
    total_duration_sec: float = 0.0
    generated_at: str = ""
    
    def add_section(self, section: RichTextSection):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - section: 函数入参（类型：RichTextSection）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.sections.append(section)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - 无。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        return {
            "title": self.title,
            "source_video": self.source_video,
            "total_duration_sec": self.total_duration_sec,
            "generated_at": self.generated_at,
            "sections": [
                {
                    "unit_id": s.unit_id,
                    "title": s.title,
                    "body_text": s.body_text,
                    "knowledge_type": s.knowledge_type,
                    "time_range": [s.start_sec, s.end_sec],
                    "materials": {
                        "clip": s.materials.clip_path,
                        "clips": s.materials.clip_paths,
                        "screenshots": s.materials.screenshot_paths,
                        "labels": s.materials.screenshot_labels,
                        "screenshot_items": s.materials.screenshot_items,
                        "action_classifications": s.materials.action_classifications
                    },
                    "instructional_steps": s.instructional_steps,
                    "mult_steps": s.mult_steps,
                    "layout_hint": s.layout_hint
                }
                for s in self.sections
            ]
        }
    
    def to_json(self, output_path: str) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - output_path: 文件路径（类型：str）。
        输出参数：
        - 字符串结果。"""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)
        logger.info(f"Exported JSON: {output_path}")
        return output_path
    
    def to_markdown(self, output_path: str, assets_relative_dir: str = "assets") -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self.source_video
        - 条件：self.total_duration_sec > 0
        依据来源（证据链）：
        - 对象内部状态：self.source_video, self.total_duration_sec。
        输入参数：
        - output_path: 文件路径（类型：str）。
        - assets_relative_dir: 目录路径（类型：str）。
        输出参数：
        - 字符串结果。"""
        lines = []
        
        # 标题
        lines.append(f"# {self.title or '视频知识文档'}")
        lines.append("")
        
        # 元信息
        if self.source_video:
            lines.append(f"> 📹 源视频: `{Path(self.source_video).name}`")
        if self.total_duration_sec > 0:
            m, s = divmod(int(self.total_duration_sec), 60)
            lines.append(f"> ⏱️ 总时长: {m}分{s}秒")
        lines.append("")
        lines.append("---")
        lines.append("")
        
        # 各段落
        for i, section in enumerate(self.sections, 1):
            lines.extend(self._render_section_markdown(section, i, assets_relative_dir))
            lines.append("")
            lines.append("---")
            lines.append("")
        
        content = "\n".join(lines)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"Exported Markdown: {output_path} ({len(self.sections)} sections)")
        return output_path
    
    def _render_section_markdown(
        self, 
        section: RichTextSection, 
        idx: int,
        assets_dir: str
    ) -> List[str]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：materials.clip_path
        - 条件：materials.screenshot_paths
        - 条件：len(materials.screenshot_paths) >= 2 or str(section.knowledge_type) in ['process', '过程性知识', '过程']
        依据来源（证据链）：
        - 输入参数：section。
        输入参数：
        - section: 函数入参（类型：RichTextSection）。
        - idx: 函数入参（类型：int）。
        - assets_dir: 目录路径（类型：str）。
        输出参数：
        - str 列表（与输入或处理结果一一对应）。"""
        lines = []
        
        # 标题
        lines.append(f"## {idx}. {section.title}")
        lines.append("")
        
        # 元信息块
        kt_emoji = {"abstract": "📚", "process": "🔄", "concrete": "🎯"}.get(section.knowledge_type, "📝")
        lines.append(f"> {kt_emoji} **{section.knowledge_type}** | ⏱️ {section.duration_str()}")
        lines.append("")
        
        # 正文
        lines.append(section.body_text)
        lines.append("")
        
        # 正文后的步骤渲染
        if section.instructional_steps:
             for step in section.instructional_steps:
                 step_desc = step.get('step_description') or step.get('description') or ""
                 lines.append(f"### Step {step.get('step_id')}: {step_desc}")
                 mats = step.get('materials', {})
                 # 步骤截图
                 ss_paths = mats.get('screenshot_paths', [])
                 if ss_paths:
                     for ss in ss_paths:
                        ss_path = self._relative_path(ss, assets_dir)
                        lines.append(f"![Step Snapshot]({ss_path})")
                 # 步骤 Clip
                 step_clip_paths = mats.get('clip_paths', [])
                 if not step_clip_paths:
                     clip_p = mats.get('clip_path')
                     if clip_p:
                         step_clip_paths = [clip_p]
                 for clip_p in step_clip_paths:
                     clip_path = self._relative_path(clip_p, assets_dir)
                     lines.append(f"![Step Clip]({clip_path})")
                 lines.append("")

        # 素材渲染 (Fallback / Top-level)
        materials = section.materials
        section_clip_paths = list(getattr(materials, 'clip_paths', []) or [])
        if materials.clip_path and materials.clip_path not in section_clip_paths:
            section_clip_paths.insert(0, materials.clip_path)
        
        if not section.instructional_steps:
            if section_clip_paths:
                # 视频优先
                clip_path = self._relative_path(section_clip_paths[0], assets_dir)
                lines.append("**视频演示**")
                lines.append("")
                lines.append(f"![[{clip_path}]]")
                lines.append("")
                for extra_clip in section_clip_paths[1:]:
                    extra_clip_path = self._relative_path(extra_clip, assets_dir)
                    lines.append(f"![[{extra_clip_path}]]")
                    lines.append("")
                
                # 辅助关键帧
                if materials.screenshot_paths:
                    lines.append("**关键帧**")
                    lines.append("")
                    for i, ss in enumerate(materials.screenshot_paths):
                        label = materials.screenshot_labels[i] if i < len(materials.screenshot_labels) else f"图{i+1}"
                        ss_path = self._relative_path(ss, assets_dir)
                        lines.append(f"{label}")
                        lines.append(f"![[{ss_path}]]")
                        lines.append("")
                
            elif materials.screenshot_paths:
                # 纯图
                lines.append("**图解**")
                lines.append("")
                for i, ss in enumerate(materials.screenshot_paths):
                    label = materials.screenshot_labels[i] if i < len(materials.screenshot_labels) else f"图{i+1}"
                    ss_path = self._relative_path(ss, assets_dir)
                    lines.append(f"{label}")
                    lines.append(f"![[{ss_path}]]")
                    lines.append("")
        
        return lines
    
    def _relative_path(self, abs_path: str, assets_dir: str) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not abs_path
        依据来源（证据链）：
        - 输入参数：abs_path。
        输入参数：
        - abs_path: 文件路径（类型：str）。
        - assets_dir: 目录路径（类型：str）。
        输出参数：
        - 字符串结果。"""
        if not abs_path:
            return ""
        normalized = str(abs_path).replace("\\", "/")
        marker = f"/{assets_dir}/"
        if marker in normalized:
            suffix = normalized.split(marker, 1)[1].strip("/")
            return f"{assets_dir}/{suffix}"
        filename = Path(abs_path).name
        return f"{assets_dir}/{filename}"


# =============================================================================
# 便捷工厂函数
# =============================================================================

def create_section_from_semantic_unit(
    unit,  # SemanticUnit
    materials: MaterialSet
) -> RichTextSection:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    输入参数：
    - unit: 函数入参（类型：未标注）。
    - materials: 函数入参（类型：MaterialSet）。
    输出参数：
    - RichTextSection 对象（包含字段：unit_id, title, body_text, knowledge_type, start_sec, end_sec, materials, layout_hint）。"""
    return RichTextSection(
        unit_id=unit.unit_id,
        title=unit.knowledge_topic,
        body_text=unit.full_text,
        knowledge_type=unit.knowledge_type,
        start_sec=unit.start_sec,
        end_sec=unit.end_sec,

        materials=materials,
        instructional_steps=getattr(unit, "instructional_steps", []) or [],
        mult_steps=bool(getattr(unit, "mult_steps", False)),
        layout_hint="default"
    )
