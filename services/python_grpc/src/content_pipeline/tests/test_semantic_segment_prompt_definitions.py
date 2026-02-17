from services.python_grpc.src.content_pipeline.infra.llm.prompt_loader import get_prompt
from services.python_grpc.src.content_pipeline.infra.llm.prompt_registry import PromptKeys


def test_segment_system_prompt_keeps_decision_tree_and_type_sections():
    content = get_prompt(PromptKeys.DEEPSEEK_SEMANTIC_SEGMENT_SYSTEM)

    assert "核心判定逻辑：知识类型决策树（按顺序严格执行）" in content
    assert "1. 判定 Concrete（具象知识 k=1）" in content
    assert "2. 判定 Process（过程知识 k=2）" in content
    assert "3. 判定 Abstract（抽象知识 k=0）" in content


def test_segment_system_prompt_keeps_corner_cases_and_merge_rules():
    content = get_prompt(PromptKeys.DEEPSEEK_SEMANTIC_SEGMENT_SYSTEM)

    assert "伪 Process（Narrative 陷阱）" in content
    assert "伪 Abstract（借图喻理陷阱）" in content
    assert "伪 Process（概括性描述陷阱）" in content
    assert "【最高优先级】视觉材料聚合" in content
    assert "核心论点聚合 (Core Argument Aggregation)" in content
    assert "叙事流聚合 (Narrative Flow)" in content
    assert "多步骤操作流聚合 (Multi-step Process Integration)" in content


def test_segment_system_prompt_keeps_required_examples_and_group_schema():
    content = get_prompt(PromptKeys.DEEPSEEK_SEMANTIC_SEGMENT_SYSTEM)

    assert "想象你有一个机器人，它长得像龙虾。" in content
    assert "大家看屏幕上这个机器人，它长得像龙虾。" in content
    assert "工厂模式的核心是解耦创建逻辑。" in content
    assert "顶层仅允许：`knowledge_groups`。" in content
    assert "Group 仅允许字段：`group_name`, `reason`, `units`。" in content
    assert "Unit 仅允许字段：`pids`, `k`, `m`, `title`。" in content
    assert '"knowledge_groups": [' in content
