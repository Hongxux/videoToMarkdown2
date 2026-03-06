"""
模块说明：Module2 Prompt 注册表。
执行逻辑：
1) 统一声明 Prompt Key 与默认文件路径映射。
2) 为调用方提供稳定的 Key 检索接口，避免业务代码散落硬编码路径。
实现方式：通过 dataclass + 常量键值表维护注册信息。
核心价值：把 Prompt 的“命名”和“存储位置”从业务实现中解耦，降低后续维护成本。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class PromptRegistryEntry:
    """单个 Prompt 注册信息。"""

    key: str
    relative_path: str
    description: str = ""


class PromptKeys:
    """Module2 Prompt Key 常量。"""

    DEEPSEEK_SEMANTIC_SEGMENT_SYSTEM = "deepseek.semantic.segment.system"
    DEEPSEEK_SEMANTIC_SEGMENT_USER = "deepseek.semantic.segment.user"
    DEEPSEEK_SEMANTIC_RESEGMENT_SYSTEM = "deepseek.semantic.resegment.system"
    DEEPSEEK_SEMANTIC_RESEGMENT_USER = "deepseek.semantic.resegment.user"

    DEEPSEEK_KC_SYSTEM = "deepseek.knowledge_classifier.system"
    DEEPSEEK_KC_USER = "deepseek.knowledge_classifier.user"
    DEEPSEEK_KC_BATCH_SYSTEM = "deepseek.knowledge_classifier.batch.system"
    DEEPSEEK_KC_BATCH_USER = "deepseek.knowledge_classifier.batch.user"
    DEEPSEEK_KC_MULTI_UNIT_USER = "deepseek.knowledge_classifier.multi_unit.user"

    DEEPSEEK_MD_HIERARCHY = "deepseek.markdown.hierarchy"
    DEEPSEEK_MD_TEXT_ENHANCE = "deepseek.markdown.text_enhance"
    DEEPSEEK_MD_LOGIC_EXTRACT = "deepseek.markdown.logic_extract"
    DEEPSEEK_MD_COMBINED_SYSTEM = "deepseek.markdown.combined.system"
    DEEPSEEK_MD_COMBINED_USER = "deepseek.markdown.combined.user"
    DEEPSEEK_MD_STRUCTURED_SYSTEM = "deepseek.markdown.structured.system"
    DEEPSEEK_MD_STRUCTURED_USER = "deepseek.markdown.structured.user"
    DEEPSEEK_MD_STRUCTURED_SYSTEM_PRESERVE_IMG = "deepseek.markdown.structured.system.preserve_img"
    DEEPSEEK_MD_STRUCTURED_USER_PRESERVE_IMG = "deepseek.markdown.structured.user.preserve_img"
    DEEPSEEK_MD_IMG_DESC_AUG_SYSTEM = "deepseek.markdown.img_desc_augment.system"
    DEEPSEEK_MD_IMG_DESC_AUG_USER = "deepseek.markdown.img_desc_augment.user"
    DEEPSEEK_VL_ARG_STRUCTURED_SYSTEM = "deepseek.vl_arg.structured.system"
    DEEPSEEK_VL_ARG_STRUCTURED_USER = "deepseek.vl_arg.structured.user"

    VISION_AI_CONCRETE_KNOWLEDGE_SYSTEM = "vision_ai.concrete_knowledge.system"
    VISION_AI_CONCRETE_KNOWLEDGE_USER = "vision_ai.concrete_knowledge.user"

    VL_VIDEO_ANALYSIS_DEFAULT_USER = "vl.video_analysis.default.user"
    VL_VIDEO_ANALYSIS_TUTORIAL_SYSTEM = "vl.video_analysis.tutorial.system"
    VL_VIDEO_ANALYSIS_CONCRETE_SYSTEM = "vl.video_analysis.concrete.system"
    VL_VIDEO_ANALYSIS_CONSTRAINTS_DEFAULT = "vl.video_analysis.constraints.default"
    VL_VIDEO_ANALYSIS_CONSTRAINTS_TUTORIAL = "vl.video_analysis.constraints.tutorial"
    VL_VIDEO_ANALYSIS_CONSTRAINTS_CONCRETE = "vl.video_analysis.constraints.concrete"
    VL_VIDEO_ANALYSIS_GRID_SPATIAL_ANCHOR = "vl.video_analysis.grid_spatial_anchor"

    DEEPSEEK_VIDEO_CLIP_MOTION_VALUE_SYSTEM = "deepseek.video_clip.motion_value.system"
    DEEPSEEK_VIDEO_CLIP_TRANSITION_SYSTEM = "deepseek.video_clip.transition.system"


PROMPT_REGISTRY: Dict[str, PromptRegistryEntry] = {
    PromptKeys.DEEPSEEK_SEMANTIC_SEGMENT_SYSTEM: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_SEMANTIC_SEGMENT_SYSTEM,
        relative_path="deepseek/semantic/segment_system.md",
        description="语义单元切分 System Prompt",
    ),
    PromptKeys.DEEPSEEK_SEMANTIC_SEGMENT_USER: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_SEMANTIC_SEGMENT_USER,
        relative_path="deepseek/semantic/segment_user.md",
        description="语义单元切分 User Prompt",
    ),
    PromptKeys.DEEPSEEK_SEMANTIC_RESEGMENT_SYSTEM: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_SEMANTIC_RESEGMENT_SYSTEM,
        relative_path="deepseek/semantic/resegment_system.md",
        description="跨模态冲突重切分 System Prompt",
    ),
    PromptKeys.DEEPSEEK_SEMANTIC_RESEGMENT_USER: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_SEMANTIC_RESEGMENT_USER,
        relative_path="deepseek/semantic/resegment_user.md",
        description="跨模态冲突重切分 User Prompt",
    ),

    PromptKeys.DEEPSEEK_KC_SYSTEM: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_KC_SYSTEM,
        relative_path="deepseek/knowledge_classifier/system.md",
        description="知识分类基础 System Prompt",
    ),
    PromptKeys.DEEPSEEK_KC_USER: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_KC_USER,
        relative_path="deepseek/knowledge_classifier/user.md",
        description="知识分类基础 User Prompt",
    ),
    PromptKeys.DEEPSEEK_KC_BATCH_SYSTEM: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_KC_BATCH_SYSTEM,
        relative_path="deepseek/knowledge_classifier/batch_system.md",
        description="知识分类批处理 System Prompt",
    ),
    PromptKeys.DEEPSEEK_KC_BATCH_USER: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_KC_BATCH_USER,
        relative_path="deepseek/knowledge_classifier/batch_user.md",
        description="知识分类批处理 User Prompt",
    ),
    PromptKeys.DEEPSEEK_KC_MULTI_UNIT_USER: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_KC_MULTI_UNIT_USER,
        relative_path="deepseek/knowledge_classifier/multi_unit_user.md",
        description="知识分类多语义单元 User Prompt",
    ),

    PromptKeys.DEEPSEEK_MD_HIERARCHY: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_MD_HIERARCHY,
        relative_path="deepseek/markdown_enhancer/hierarchy.md",
        description="Markdown 层级规划 Prompt",
    ),
    PromptKeys.DEEPSEEK_MD_TEXT_ENHANCE: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_MD_TEXT_ENHANCE,
        relative_path="deepseek/markdown_enhancer/text_enhance.md",
        description="Markdown 文本增强 Prompt",
    ),
    PromptKeys.DEEPSEEK_MD_LOGIC_EXTRACT: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_MD_LOGIC_EXTRACT,
        relative_path="deepseek/markdown_enhancer/logic_extract.md",
        description="Markdown 逻辑提取 Prompt",
    ),
    PromptKeys.DEEPSEEK_MD_COMBINED_SYSTEM: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_MD_COMBINED_SYSTEM,
        relative_path="deepseek/markdown_enhancer/combined_system.md",
        description="Markdown 合并调用 System Prompt",
    ),
    PromptKeys.DEEPSEEK_MD_COMBINED_USER: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_MD_COMBINED_USER,
        relative_path="deepseek/markdown_enhancer/combined_user.md",
        description="Markdown 合并调用 User Prompt",
    ),
    PromptKeys.DEEPSEEK_MD_STRUCTURED_SYSTEM: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_MD_STRUCTURED_SYSTEM,
        relative_path="deepseek/markdown_enhancer/structured_system.md",
        description="结构化文本 System Prompt",
    ),
    PromptKeys.DEEPSEEK_MD_STRUCTURED_USER: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_MD_STRUCTURED_USER,
        relative_path="deepseek/markdown_enhancer/structured_user.md",
        description="结构化文本 User Prompt",
    ),
    PromptKeys.DEEPSEEK_MD_STRUCTURED_SYSTEM_PRESERVE_IMG: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_MD_STRUCTURED_SYSTEM_PRESERVE_IMG,
        relative_path="deepseek/markdown_enhancer/structured_system_preserve_img.md",
        description="Structured markdown system prompt (preserve existing markdown images)",
    ),
    PromptKeys.DEEPSEEK_MD_STRUCTURED_USER_PRESERVE_IMG: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_MD_STRUCTURED_USER_PRESERVE_IMG,
        relative_path="deepseek/markdown_enhancer/structured_user_preserve_img.md",
        description="Structured markdown user prompt (preserve existing markdown images)",
    ),
    PromptKeys.DEEPSEEK_MD_IMG_DESC_AUG_SYSTEM: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_MD_IMG_DESC_AUG_SYSTEM,
        relative_path="deepseek/markdown_enhancer/img_desc_augment_system.md",
        description="图像描述增补 System Prompt",
    ),
    PromptKeys.DEEPSEEK_MD_IMG_DESC_AUG_USER: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_MD_IMG_DESC_AUG_USER,
        relative_path="deepseek/markdown_enhancer/img_desc_augment_user.md",
        description="图像描述增补 User Prompt",
    ),

    # 兼容旧 key：历史命名为 user，但语义已迁移为 system。
    PromptKeys.VISION_AI_CONCRETE_KNOWLEDGE_USER: PromptRegistryEntry(
        key=PromptKeys.VISION_AI_CONCRETE_KNOWLEDGE_USER,
        relative_path="vision_ai/concrete_knowledge/user.md",
        description="Vision AI 具象知识判断 Prompt",
    ),

    # 新 key：用于表达该提示词在调用时应作为 system 角色。
    PromptKeys.VISION_AI_CONCRETE_KNOWLEDGE_SYSTEM: PromptRegistryEntry(
        key=PromptKeys.VISION_AI_CONCRETE_KNOWLEDGE_SYSTEM,
        relative_path="vision_ai/concrete_knowledge/user.md",
        description="Vision AI concrete knowledge system prompt",
    ),

    PromptKeys.VL_VIDEO_ANALYSIS_DEFAULT_USER: PromptRegistryEntry(
        key=PromptKeys.VL_VIDEO_ANALYSIS_DEFAULT_USER,
        relative_path="vl/video_analysis/default_user.md",
        description="VL 默认分析 Prompt",
    ),
    PromptKeys.VL_VIDEO_ANALYSIS_TUTORIAL_SYSTEM: PromptRegistryEntry(
        key=PromptKeys.VL_VIDEO_ANALYSIS_TUTORIAL_SYSTEM,
        relative_path="vl/video_analysis/tutorial_system.md",
        description="VL 教程模式 System Prompt",
    ),
    PromptKeys.VL_VIDEO_ANALYSIS_CONCRETE_SYSTEM: PromptRegistryEntry(
        key=PromptKeys.VL_VIDEO_ANALYSIS_CONCRETE_SYSTEM,
        relative_path="vl/video_analysis/concrete_system.md",
        description="VL concrete 模式 System Prompt",
    ),
    PromptKeys.VL_VIDEO_ANALYSIS_CONSTRAINTS_DEFAULT: PromptRegistryEntry(
        key=PromptKeys.VL_VIDEO_ANALYSIS_CONSTRAINTS_DEFAULT,
        relative_path="vl/video_analysis/output_constraints_default.md",
        description="VL 默认模式输出约束",
    ),
    PromptKeys.VL_VIDEO_ANALYSIS_CONSTRAINTS_TUTORIAL: PromptRegistryEntry(
        key=PromptKeys.VL_VIDEO_ANALYSIS_CONSTRAINTS_TUTORIAL,
        relative_path="vl/video_analysis/output_constraints_tutorial.md",
        description="VL 教程模式输出约束",
    ),
    PromptKeys.VL_VIDEO_ANALYSIS_CONSTRAINTS_CONCRETE: PromptRegistryEntry(
        key=PromptKeys.VL_VIDEO_ANALYSIS_CONSTRAINTS_CONCRETE,
        relative_path="vl/video_analysis/output_constraints_concrete.md",
        description="VL concrete 模式输出约束",
    ),

    PromptKeys.VL_VIDEO_ANALYSIS_GRID_SPATIAL_ANCHOR: PromptRegistryEntry(
        key=PromptKeys.VL_VIDEO_ANALYSIS_GRID_SPATIAL_ANCHOR,
        relative_path="vl/video_analysis/grid_spatial_anchor.md",
        description="VL tutorial grid spatial anchor prompt",
    ),
    PromptKeys.DEEPSEEK_VL_ARG_STRUCTURED_SYSTEM: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_VL_ARG_STRUCTURED_SYSTEM,
        relative_path="deepseek/vl_arg/structured_system.md",
        description="VL main_operation 增强 System Prompt",
    ),
    PromptKeys.DEEPSEEK_VL_ARG_STRUCTURED_USER: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_VL_ARG_STRUCTURED_USER,
        relative_path="deepseek/vl_arg/structured_user.md",
        description="VL main_operation 增强 User Prompt",
    ),
    PromptKeys.DEEPSEEK_VIDEO_CLIP_MOTION_VALUE_SYSTEM: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_VIDEO_CLIP_MOTION_VALUE_SYSTEM,
        relative_path="deepseek/video_clip/motion_value_system.md",
        description="视频片段动态价值判断 System Prompt",
    ),
    PromptKeys.DEEPSEEK_VIDEO_CLIP_TRANSITION_SYSTEM: PromptRegistryEntry(
        key=PromptKeys.DEEPSEEK_VIDEO_CLIP_TRANSITION_SYSTEM,
        relative_path="deepseek/video_clip/transition_system.md",
        description="过渡语生成 System Prompt",
    ),
}


def get_prompt_entry(key: str) -> PromptRegistryEntry:
    """按 key 获取注册信息，不存在则抛出 KeyError。"""

    if key not in PROMPT_REGISTRY:
        raise KeyError(f"Unknown prompt key: {key}")
    return PROMPT_REGISTRY[key]

