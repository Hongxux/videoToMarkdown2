"""
模块说明：analyze_action_units 相关能力的封装。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import json
import os
import asyncio
from pathlib import Path
from services.python_grpc.src.content_pipeline.infra.llm.llm_client import LLMClient

# 脚本职责边界：离线分析单个视频任务的 action unit，并输出结构化分类结果。
# 主要功能：读取富文本产物 + 字幕，调用 LLM 对每个 action unit 做知识类型判定。

REPO_ROOT = Path(__file__).resolve().parents[3]


def _pick_existing_path(candidates: list[Path]) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


# 路径配置
RESULT_PATH = _pick_existing_path(
    [
        REPO_ROOT / "storage" / "sample_data" / "video_01" / "rich_text_output" / "result.json",
        REPO_ROOT / "storage" / "video_01" / "rich_text_output" / "result.json",
    ]
)
SUBTITLE_PATH = _pick_existing_path(
    [
        REPO_ROOT / "storage" / "sample_data" / "video_01" / "step2_correction_output.json",
        REPO_ROOT / "storage" / "video_01" / "step2_correction_output.json",
    ]
)

PROMPT_TEMPLATE = """你是一个知识类型分析专家。请分析以下教学视频的动作单元，提取三要素并判断知识类型。

## 语义单元上下文 (用于理解主体和目标)
**标题**: {title}
**完整文本**: {full_text}

## 动作单元的字幕内容 (用于判断核心描述)
**时间范围**: {action_start:.1f}s - {action_end:.1f}s
**字幕文本**: 
{action_subtitles}

## 分析框架

### 核心主体 (Subject) - 根据上下文判断
- **抽象知识/算法/机制**: 讲解某个概念、算法、数据结构的定义或工作原理
- **人/操作者**: 描述人如何操作软件、工具、界面
- **逻辑/公式/问题**: 讨论为什么、证明、推导、解释原因
- **概念/定义**: 单纯介绍某个概念是什么，无步骤/操作/推导

### 核心描述 (Description) - 必须基于动作单元字幕判断
- **标准化步骤**: "第一步...第二步..."、"首先...然后..."、"按照顺序..."、具体的操作序列
- **动手操作动作**: "点击"、"输入"、"拖拽"、"选择"、"右键"等操作指令
- **推理/演算/论证步骤**: "因为...所以..."、"由此可知"、公式推导、数学计算、证明过程
- **解释/说明**: 纯粹解释某事是什么、有什么特点，无步骤/操作/推导

### 核心目标 (Goal) - 根据上下文判断
- **还原流程**: 让读者理解某个过程是怎么进行的
- **复刻操作**: 让读者能够照着做出来
- **展示思维**: 让读者理解背后的道理和逻辑
- **知晓概念**: 让读者知道某事是什么

## 知识类型判定规则 (四分类)
- **过程性知识**: 描述=标准化步骤，有明确的执行序列
- **实操**: 描述=动手操作动作，涉及软件/工具操作
- **推演**: 描述=推理/演算/论证步骤，涉及因果推理或数学推导
- **讲解型**: 描述=解释/说明，单纯概念介绍/背景说明/总结回顾，无明确步骤

## 严格判定标准
- 如果字幕中没有明确的"步骤词"（第一/第二/然后/接着/按照顺序），不应判为"过程性知识"
- 如果字幕中没有操作动词（点击/输入/拖拽/选择），不应判为"实操"
- 如果字幕中没有因果/推导词（因为/所以/因此/由此可知/等于），不应判为"推演"
- 其余情况应判为"讲解型"

## 输出格式 (JSON)
{{
  "subject": "抽象知识/算法/机制" | "人/操作者" | "逻辑/公式/问题" | "概念/定义",
  "description": "标准化步骤" | "动手操作动作" | "推理/演算/论证步骤" | "解释/说明",
  "goal": "还原流程" | "复刻操作" | "展示思维" | "知晓概念",
  "knowledge_type": "过程性知识" | "实操" | "推演" | "讲解型",
  "confidence": 0.0-1.0,
  "is_uncertain": true/false,
  "uncertainty_reason": "如有不确定，说明原因" | null,
  "key_subtitle_evidence": "从字幕中提取的关键证据(30字以内)",
  "reasoning": "简要分析理由(50字以内)"
}}

请只输出JSON，不要有其他内容。"""


def load_subtitles(subtitle_path: str) -> list:
    """
    执行逻辑：
    1) 校验输入路径与参数。
    2) 读取并解析为结构化对象。
    实现方式：通过JSON 解析/序列化、文件系统读写实现。
    核心价值：将外部数据转为内部结构，统一输入口径。
    输入参数：
    - subtitle_path: 文件路径（类型：str）。
    输出参数：
    - 列表结果（与输入或处理结果一一对应）。"""
    with open(subtitle_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["output"]["corrected_subtitles"]


def get_subtitles_in_range(subtitles: list, start_sec: float, end_sec: float) -> str:
    """
    执行逻辑：
    1) 读取内部状态或外部资源。
    2) 返回读取结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：提供一致读取接口，降低调用耦合。
    决策逻辑：
    - 条件：sub_start < end_sec and sub_end > start_sec
    - 条件：texts
    依据来源（证据链）：
    - 输入参数：end_sec, start_sec。
    输入参数：
    - subtitles: 数据列表/集合（类型：list）。
    - start_sec: 起止时间/区间边界（类型：float）。
    - end_sec: 起止时间/区间边界（类型：float）。
    输出参数：
    - 字符串结果。"""
    texts = []
    for sub in subtitles:
        sub_start = sub["start_sec"]
        sub_end = sub["end_sec"]
        # 字幕与时间范围有重叠
        if sub_start < end_sec and sub_end > start_sec:
            texts.append(f"[{sub_start:.1f}s] {sub['corrected_text']}")
    return "\n".join(texts) if texts else "(无字幕)"


async def analyze_with_llm(
    title: str,
    full_text: str,
    action_start: float,
    action_end: float,
    action_subtitles: str,
    llm_client: LLMClient
) -> dict:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：依赖 LLMClient.complete_json 返回结构化结果
    依据来源（证据链）：
    输入参数：
    - title: 函数入参（类型：str）。
    - full_text: 函数入参（类型：str）。
    - action_start: 起止时间/区间边界（类型：float）。
    - action_end: 起止时间/区间边界（类型：float）。
    - action_subtitles: 函数入参（类型：str）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    prompt = PROMPT_TEMPLATE.format(
        title=title,
        full_text=full_text,
        action_start=action_start,
        action_end=action_end,
        action_subtitles=action_subtitles
    )

    parsed, _, _ = await llm_client.complete_json(prompt=prompt)
    return parsed


async def main():
    # 加载数据
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化、文件系统读写实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：not clip
    - 条件：r.get('is_uncertain')
    - 条件：r.get('confidence', 1.0) < 0.7
    依据来源（证据链）：
    - 配置字段：confidence, is_uncertain。
    输入参数：
    - 无。
    输出参数：
    - 无（仅产生副作用，如日志/写盘/状态更新）。"""
    with open(RESULT_PATH, "r", encoding="utf-8") as f:
        result_data = json.load(f)
    
    subtitles = load_subtitles(SUBTITLE_PATH)
    llm_client = LLMClient(
        api_key=os.getenv("DEEPSEEK_API_KEY"),
        base_url="https://api.deepseek.com/v1",
        model="deepseek-chat",
        temperature=0.1
    )
    
    print("=" * 90)
    print("动作单元三要素分析 V4 (含讲解型 + 置信度)")
    print("=" * 90)
    
    results = []
    
    for section in result_data["sections"]:
        unit_id = section["unit_id"]
        title = section["title"]
        body_text = section["body_text"]
        time_range = section.get("time_range", [0, 0])
        
        # 检查是否有动作单元
        materials = section.get("materials", {})
        clip = materials.get("clip", "")
        
        if not clip:
            continue
        
        # 从 labels 中推断动作数量
        labels = materials.get("labels", [])
        action_count = len([l for l in labels if "首帧" in l])
        
        print(f"\n{'='*90}")
        print(f"【{unit_id}】{title}")
        print(f"时间范围: {time_range[0]:.1f}s - {time_range[1]:.1f}s | 动作数: {action_count}")
        
        # 对每个动作单元进行分析
        unit_duration = time_range[1] - time_range[0]
        action_duration = unit_duration / max(action_count, 1)
        
        for i in range(action_count):
            action_start = time_range[0] + i * action_duration
            action_end = time_range[0] + (i + 1) * action_duration
            
            # 获取动作单元对应的字幕
            action_subs = get_subtitles_in_range(subtitles, action_start, action_end)
            
            print(f"\n  --- 动作 {i+1} [{action_start:.1f}s - {action_end:.1f}s] ---")
            
            try:
                analysis = await analyze_with_llm(
                    title=title,
                    full_text=body_text,
                    action_start=action_start,
                    action_end=action_end,
                    action_subtitles=action_subs,
                    llm_client=llm_client
                )
                
                conf = analysis.get('confidence', 0)
                uncertain = "⚠️ " if analysis.get('is_uncertain', False) else ""
                
                print(f"  {uncertain}知识类型: {analysis.get('knowledge_type', 'N/A')} (置信度: {conf:.0%})")
                print(f"  主体: {analysis.get('subject', 'N/A')}")
                print(f"  描述: {analysis.get('description', 'N/A')}")
                print(f"  目标: {analysis.get('goal', 'N/A')}")
                print(f"  证据: {analysis.get('key_subtitle_evidence', 'N/A')}")
                
                if analysis.get('is_uncertain'):
                    print(f"  ⚠️ 不确定原因: {analysis.get('uncertainty_reason', 'N/A')}")
                
                results.append({
                    "unit_id": unit_id,
                    "action_index": i + 1,
                    "title": title,
                    "action_time_range": [action_start, action_end],
                    "action_subtitles": action_subs,
                    **analysis
                })
                
            except Exception as e:
                print(f"  分析失败: {e}")
                import traceback
                traceback.print_exc()
    
    # 保存结果
    output_path = RESULT_PATH.parent / "action_unit_analysis_v4.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n\n结果已保存到: {output_path}")
    
    # 打印汇总
    print("\n" + "=" * 90)
    print("分类统计")
    print("=" * 90)
    
    type_counts = {}
    uncertain_count = 0
    low_conf_count = 0
    
    for r in results:
        kt = r.get("knowledge_type", "unknown")
        type_counts[kt] = type_counts.get(kt, 0) + 1
        if r.get("is_uncertain"):
            uncertain_count += 1
        if r.get("confidence", 1.0) < 0.7:
            low_conf_count += 1
    
    for kt, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {kt}: {count}个")
    
    print(f"\n  ⚠️ 不确定判断: {uncertain_count}个")
    print(f"  ⚠️ 低置信度(<70%): {low_conf_count}个")
    
    # 打印详细表格
    print("\n" + "=" * 110)
    print("详细对比表")
    print("=" * 110)
    print(f"{'单元':<8} | {'动作':<4} | {'知识类型':<10} | {'置信度':<6} | {'描述':<20} | {'证据':<30}")
    print("-" * 110)
    for r in results:
        uncertain = "⚠️" if r.get('is_uncertain') else "  "
        conf = r.get('confidence', 0)
        print(f"{r['unit_id']:<8} | {r['action_index']:<4} | {uncertain}{r['knowledge_type']:<8} | {conf:>5.0%} | "
              f"{r['description']:<20} | {r.get('key_subtitle_evidence', 'N/A')[:28]:<30}")


if __name__ == "__main__":
    asyncio.run(main())
