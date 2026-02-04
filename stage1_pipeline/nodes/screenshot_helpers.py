"""
模块说明：阶段流程节点 screenshot_helpers 的实现。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import re
from typing import Dict, List


def generate_simplified_questions(source_type: str, source_data: Dict, must_content: str, secondary_content: str) -> List[Dict]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：source_type == 'fault'
    - 条件：fault_type == 3
    - 条件：key_elements
    依据来源（证据链）：
    - 输入参数：must_content, secondary_content, source_type。
    输入参数：
    - source_type: 函数入参（类型：str）。
    - source_data: 函数入参（类型：Dict）。
    - must_content: 函数入参（类型：str）。
    - secondary_content: 函数入参（类型：str）。
    输出参数：
    - Dict 列表（与输入或处理结果一一对应）。
    补充说明：
    策略：
    1. 将长问题拆分为多个短问题
    2. 根据断层类型定制问题模板
    3. 避免要求完全匹配，改为检查关键要素"""
    questions = []
    
    if source_type == "fault":
        fault_type = source_data.get("fault_type", 1)
        
        # 概念无定义类（fault_type=3）
        if fault_type == 3:
            # 提取概念名称
            concept_match = re.search(r'"([^"]+)".*定义', must_content)
            concept = concept_match.group(1) if concept_match else "该概念"
            
            questions = [
                {
                    "question_id": "Q1",
                    "question": f"图中是否包含'{concept}'的定义文字？",
                    "is_core": True
                },
                {
                    "question_id": "Q2",
                    "question": f"图中是否包含'{concept}'的计算公式？",
                    "is_core": True
                },
                {
                    "question_id": "Q3",
                    "question": "图中是否包含公式中各符号的含义说明？",
                    "is_core": False
                }
            ]
        
        # 结论无推导类（fault_type=2）
        elif fault_type == 2:
            questions = [
                {
                    "question_id": "Q1",
                    "question": "图中是否包含推导过程的分步展示？",
                    "is_core": True
                },
                {
                    "question_id": "Q2",
                    "question": "图中是否包含推导过程中的数学公式？",
                    "is_core": True
                },
                {
                    "question_id": "Q3",
                    "question": "图中是否包含推导步骤的文字说明？",
                    "is_core": False
                }
            ]
        
        # 显性指引类（fault_type=1）
        elif fault_type == 1:
            questions = [
                {
                    "question_id": "Q1",
                    "question": "图中是否包含具体的示例或演示内容？",
                    "is_core": True
                },
                {
                    "question_id": "Q2",
                    "question": "图中是否包含关键步骤的可视化展示？",
                    "is_core": False
                }
            ]
        
        # 量化数据缺失类（fault_type=6）
        elif fault_type == 6:
            questions = [
                {
                    "question_id": "Q1",
                    "question": "图中是否包含具体的数值或数据？",
                    "is_core": True
                },
                {
                    "question_id": "Q2",
                    "question": "图中是否包含数据的计算公式或来源说明？",
                    "is_core": False
                }
            ]
        
        # 指代模糊类（fault_type=7）
        elif fault_type == 7:
            questions = [
                {
                    "question_id": "Q1",
                    "question": "图中是否明确标注了指代对象（如箭头、高亮、标签）？",
                    "is_core": True
                },
                {
                    "question_id": "Q2",
                    "question": "图中是否包含指代对象的详细说明？",
                    "is_core": False
                }
            ]
        
        # 其他类型使用通用问题（但简化）
        else:
            # 截取must_content的前50字作为关键点
            key_point = must_content[:50] + "..." if len(must_content) > 50 else must_content
            questions = [
                {
                    "question_id": "Q1",
                    "question": f"图中是否包含以下核心内容：{key_point}？",
                    "is_core": True
                }
            ]
            
            if secondary_content:
                sec_point = secondary_content[:50] + "..." if len(secondary_content) > 50 else secondary_content
                questions.append({
                    "question_id": "Q2",
                    "question": f"图中是否包含补充说明：{sec_point}？",
                    "is_core": False
                })
    
    else:  # visualization
        # 可视化场景使用key_elements
        key_elements = source_data.get("key_elements", [])
        
        if key_elements:
            for i, elem in enumerate(key_elements[:3], 1):  # 最多3个问题
                questions.append({
                    "question_id": f"Q{i}",
                    "question": f"图中是否包含'{elem}'？",
                    "is_core": i == 1  # 第一个为核心问题
                })
        else:
            # 回退到通用问题
            questions = [
                {
                    "question_id": "Q1",
                    "question": f"图中是否包含'{must_content}'？",
                    "is_core": True
                }
            ]
    
    return questions
