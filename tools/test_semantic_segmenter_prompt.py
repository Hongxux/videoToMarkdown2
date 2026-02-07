"""
测试 semantic_unit_segmenter.py 中使用的提示词效果

该脚本会:
1. 读取 storage/95bf71bd0768fa4d2a0b2968c775c312/intermediates/step6_merge_cross_output.json
2. 使用 semantic_unit_segmenter.py 中的 SYSTEM_PROMPT 和 USER_PROMPT_TEMPLATE
3. 调用 LLM 并打印返回的原始 JSON
4. 将结果保存到文件
"""

import os
import sys
import json
import asyncio
from datetime import datetime

# 添加项目路径到 sys.path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from MVP_Module2_HEANCING.module2_content_enhancement.semantic_unit_segmenter import (
    SYSTEM_PROMPT,
    USER_PROMPT_TEMPLATE
)
from MVP_Module2_HEANCING.module2_content_enhancement.llm_client import LLMClient


async def test_prompt():
    """测试语义单元切分提示词"""
    
    # 准备输出文件
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(project_root, "tools", f"test_result_{timestamp}.txt")
    
    def log(msg, also_print=True):
        """同时输出到控制台和文件"""
        if also_print:
            print(msg)
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write(msg + '\n')
    
    # 1. 读取输入数据
    input_file = os.path.join(
        project_root,
        "storage",
        "95bf71bd0768fa4d2a0b2968c775c312",
        "intermediates",
        "step6_merge_cross_output.json"
    )
    
    log(f"📖 读取输入文件: {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 提取段落数据
    paragraphs = data.get("output", {}).get("pure_text_script", [])
    log(f"✅ 读取到 {len(paragraphs)} 个段落\n")
    
    # 2. 准备测试数据 - 使用全部段落
    test_paragraphs = paragraphs
    
    # 格式化为 LLM 输入格式
    paragraphs_for_llm = [
        {
            "paragraph_id": p.get("paragraph_id", f"P{idx+1:03d}"),
            "text": p.get("text", ""),
            "source_sentence_ids": p.get("source_sentence_ids", [])
        }
        for idx, p in enumerate(test_paragraphs)
    ]
    
    log("=" * 80)
    log("📝 测试输入段落:")
    log("=" * 80)
    for p in paragraphs_for_llm:
        log(f"\n[{p['paragraph_id']}]")
        log(f"文本: {p['text'][:100]}..." if len(p['text']) > 100 else f"文本: {p['text']}")
        log(f"来源句子: {p['source_sentence_ids']}")
    log("\n" + "=" * 80)
    
    # 3. 构建提示词
    user_prompt = USER_PROMPT_TEMPLATE.format(
        paragraphs_json=json.dumps(paragraphs_for_llm, ensure_ascii=False, indent=2)
    )
    
    log("\n" + "=" * 80)
    log("🤖 SYSTEM PROMPT:")
    log("=" * 80)
    log(SYSTEM_PROMPT)
    
    log("\n" + "=" * 80)
    log("💬 USER PROMPT:")
    log("=" * 80)
    log(user_prompt)
    
    # 4. 调用 LLM
    log("\n" + "=" * 80)
    log("🚀 调用 LLM...")
    log("=" * 80)
    
    llm_client = LLMClient()
    
    try:
        result_json, metadata, raw_response = await llm_client.complete_json(
            prompt=user_prompt,
            system_message=SYSTEM_PROMPT
        )
        
        log("\n" + "=" * 80)
        log("✅ LLM 返回的原始 JSON:")
        log("=" * 80)
        log(json.dumps(result_json, ensure_ascii=False, indent=2))
        
        log("\n" + "=" * 80)
        log("📊 元数据信息:")
        log("=" * 80)
        log(f"总 Token 数: {metadata.total_tokens}")
        log(f"输入 Token 数: {metadata.prompt_tokens}")
        log(f"输出 Token 数: {metadata.completion_tokens}")
        log(f"模型: {metadata.model}")
        
        # 5. 分析结果
        log("\n" + "=" * 80)
        log("📈 结果分析:")
        log("=" * 80)
        
        semantic_units = result_json.get("semantic_units", [])
        log(f"输入段落数: {len(paragraphs_for_llm)}")
        log(f"输出语义单元数: {len(semantic_units)}")
        
        merge_count = sum(1 for u in semantic_units if u.get("action") == "merge")
        split_count = sum(1 for u in semantic_units if u.get("action") == "split")
        keep_count = sum(1 for u in semantic_units if u.get("action") == "keep")
        
        log(f"合并操作: {merge_count}")
        log(f"拆分操作: {split_count}")
        log(f"保持操作: {keep_count}")
        
        log("\n语义单元详情:")
        for unit in semantic_units:
            log(f"\n  [{unit.get('unit_id')}]")
            log(f"  - 知识类型: {unit.get('knowledge_type')}")
            log(f"  - 知识主题: {unit.get('knowledge_topic')}")
            log(f"  - 操作: {unit.get('action')}")
            log(f"  - 多步骤: {unit.get('mult_steps')}")
            log(f"  - 置信度: {unit.get('confidence')}")
            log(f"  - 来源段落: {unit.get('source_paragraph_ids')}")
            text = unit.get('text', '')
            log(f"  - 文本: {text[:80]}..." if len(text) > 80 else f"  - 文本: {text}")
        
        if "reasoning" in result_json:
            log("\n" + "=" * 80)
            log("💭 LLM 推理过程:")
            log("=" * 80)
            log(result_json["reasoning"])
        
        log(f"\n\n✅ 测试结果已保存到: {output_file}")
        
    except Exception as e:
        log(f"\n❌ 调用 LLM 失败: {e}")
        import traceback
        log(traceback.format_exc())


if __name__ == "__main__":
    print("=" * 80)
    print("🧪 语义单元切分提示词测试")
    print("=" * 80)
    print()
    
    asyncio.run(test_prompt())
    
    print("\n" + "=" * 80)
    print("✅ 测试完成")
    print("=" * 80)
