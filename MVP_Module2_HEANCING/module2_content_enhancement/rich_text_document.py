"""
Rich Text Document - 富文本文档数据结构

用于表示语义单元到富文本的转换结果，支持 Markdown/HTML 导出。

设计原则:
- 每个 Section 对应一个 SemanticUnit
- 素材路径使用相对路径 (便于迁移)
- 布局灵活可配置
"""

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
    语义单元的素材集合
    """
    clip_path: Optional[str] = None           # 视频片段路径
    screenshot_paths: List[str] = field(default_factory=list)  # 截图路径列表
    
    # 元信息
    # 元信息
    screenshot_labels: List[str] = field(default_factory=list)  # 截图标签 ["首帧", "稳定岛", "末帧"]
    
    # V7.4: LLM 三要素分类结果
    action_classifications: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class RichTextSection:
    """
    单个语义单元的富文本表示
    """
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
    
    # 布局提示
    layout_hint: str = "default"              # 可选: inline / block / gallery
    
    def duration_str(self) -> str:
        """格式化时间范围字符串"""
        def fmt(sec):
            m, s = divmod(int(sec), 60)
            return f"{m}:{s:02d}"
        return f"{fmt(self.start_sec)} - {fmt(self.end_sec)}"


@dataclass
class RichTextDocument:
    """
    完整富文本文档
    """
    title: str = ""
    sections: List[RichTextSection] = field(default_factory=list)
    
    # 元信息
    source_video: str = ""
    total_duration_sec: float = 0.0
    generated_at: str = ""
    
    def add_section(self, section: RichTextSection):
        """添加一个段落"""
        self.sections.append(section)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为可序列化字典"""
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
                        "screenshots": s.materials.screenshot_paths,
                        "labels": s.materials.screenshot_labels,
                        "action_classifications": s.materials.action_classifications
                    },
                    "layout_hint": s.layout_hint
                }
                for s in self.sections
            ]
        }
    
    def to_json(self, output_path: str) -> str:
        """导出为 JSON"""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)
        logger.info(f"Exported JSON: {output_path}")
        return output_path
    
    def to_markdown(self, output_path: str, assets_relative_dir: str = "assets") -> str:
        """
        导出为 Markdown
        
        Args:
            output_path: 输出文件路径
            assets_relative_dir: 素材相对目录 (相对于 md 文件)
        """
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
        """渲染单个段落为 Markdown"""
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
        
        # 素材渲染 (根据 modality 灵活布局)
        materials = section.materials
        
        if materials.clip_path:
            # 优先渲染视频
            clip_path = self._relative_path(materials.clip_path, assets_dir)
            lines.append(f"**📹 过程演示**")
            lines.append("")
            lines.append(f'<video src="{clip_path}" controls width="100%"></video>')
            lines.append("")
            
            # 如果有截图，作为辅助关键帧图集 (video_screenshot 模式)
            if materials.screenshot_paths:
                if len(materials.screenshot_paths) >= 2 or str(section.knowledge_type) in ["process", "过程性知识", "过程"]:
                     lines.append("**关键帧图集:**")
                     lines.append("")
                     headers = materials.screenshot_labels if materials.screenshot_labels else [f"帧{i+1}" for i in range(len(materials.screenshot_paths))]
                     lines.append("| " + " | ".join(headers) + " |")
                     lines.append("|" + "---|" * len(headers))
                     imgs = [f"![]({self._relative_path(ss, assets_dir)})" for ss in materials.screenshot_paths]
                     lines.append("| " + " | ".join(imgs) + " |")
                     lines.append("")
            
        elif materials.screenshot_paths:
            # 仅截图
            if len(materials.screenshot_paths) == 1:
                # 单张图 (screenshot 模式)
                ss_path = self._relative_path(materials.screenshot_paths[0], assets_dir)
                lines.append(f"![截图]({ss_path})")
                lines.append("")
            else:
                 # 多张图 (图集模式)
                 lines.append("**截图图集:**")
                 lines.append("")
                 headers = materials.screenshot_labels if materials.screenshot_labels else [f"帧{i+1}" for i in range(len(materials.screenshot_paths))]
                 lines.append("| " + " | ".join(headers) + " |")
                 lines.append("|" + "---|" * len(headers))
                 imgs = [f"![]({self._relative_path(ss, assets_dir)})" for ss in materials.screenshot_paths]
                 lines.append("| " + " | ".join(imgs) + " |")
                 lines.append("")
        
        return lines
    
    def _relative_path(self, abs_path: str, assets_dir: str) -> str:
        """转换为相对路径"""
        if not abs_path:
            return ""
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
    从 SemanticUnit 创建 RichTextSection
    """
    return RichTextSection(
        unit_id=unit.unit_id,
        title=unit.knowledge_topic,
        body_text=unit.full_text,
        knowledge_type=unit.knowledge_type,
        start_sec=unit.start_sec,
        end_sec=unit.end_sec,
        end_sec=unit.end_sec,
        materials=materials
    )
