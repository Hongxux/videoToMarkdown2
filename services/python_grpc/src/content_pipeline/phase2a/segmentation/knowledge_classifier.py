"""
模块说明：Module2 内容增强中的 knowledge_classifier 模块。
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
import re
import ast
import logging
from typing import Dict, Optional, List, Any

from services.python_grpc.src.common.utils.numbers import safe_float
from services.python_grpc.src.config_paths import resolve_video_config_path
# 统一 LLM 调用入口
from services.python_grpc.src.content_pipeline.infra.llm import llm_gateway
from services.python_grpc.src.content_pipeline.infra.llm.prompt_loader import get_prompt, render_prompt
from services.python_grpc.src.content_pipeline.infra.llm.prompt_registry import PromptKeys
from services.python_grpc.src.content_pipeline.shared.subtitle.subtitle_repository import SubtitleRepository
# 🚀 使用集中式 LLMClient (连接池+HTTP/2+自适应并发)
import asyncio

logger = logging.getLogger(__name__)


# =============================================================================
# 🚀 Prompt Prefix Caching: Split into System (fixed) + User (dynamic)
# =============================================================================
# System Prompt is cached by LLM (KV Cache), reducing token computation by ~50%

SYSTEM_PROMPT = """你是一个知识类型分析专家，擅长分析教学视频的动作单元并判断知识类型。

## 分析框架

### 核心主体 (Subject)
- **抽象知识/算法/机制**: 讲解某个概念、算法、数据结构的定义或工作原理
- **人/操作者**: 描述人如何操作软件、工具、界面
- **逻辑/公式/问题**: 讨论为什么、证明、推导、解释原因
- **概念/定义**: 单纯介绍某个概念是什么，无步骤/操作/推导
- **环境/配置/参数**: 环境搭建、配置文件修改、参数设置、依赖安装、命令行执行

### 核心描述 (Description) - 必须基于动作单元字幕判断
- **标准化步骤**: 必须包含 ≥2 个明确的步骤描述
- **动手操作动作**: 必须包含具体操作动词 + 操作对象
- **推理/演算/论证步骤**: 必须包含 ≥1 个因果/论证逻辑
- **配置/环境搭建步骤**: 必须包含 配置修改/命令执行/环境描述
- **解释/说明**: 无上述特征的纯概念介绍

### 核心目标 (Goal)
- **还原流程**: 让读者理解某个过程是怎么进行的
- **复刻操作**: 让读者能够照着做出来
- **展示思维**: 让读者理解背后的道理和逻辑
- **知晓概念**: 让读者知道某事是什么

## ⚠️ 严格判定标准 (防止伪阳性)

### 核心特征必填项 - 不满足则判定为讲解型
1. **过程性知识**: 必须包含 **≥2 个标准化步骤**
   - 有效步骤词: "第一步/第二步"、"首先/然后/接着/最后"、"按照顺序"、"从...到..."
   - 反例: "红黑树左旋是基本操作之一" → 无步骤 → 讲解型

2. **推演**: 必须包含 **≥1 个因果/论证逻辑**
   - 有效因果词: "因为/所以"、"因此/由此可得"、"等于/意味着"、"证明/说明了"
   - 反例: "步骤是A→B→C，能保持平衡" → 核心是步骤 → 过程性知识

3. **实操**: 必须包含 **具体操作动作 + 操作对象**
   - 有效操作词: "点击/右键/双击"、"输入/拖拽/选择"、"打开/关闭/保存"
   - 必须有对象: "右键代码文件"、"点击确定按钮"
   - 反例: "操作电脑执行左旋" → 无具体动作 → 讲解型

4. **环境/配置/参数**: 必须包含 **配置动作 + 配置对象** 或 **命令行操作**
   - 有效动作: 修改/添加/注释/指定/设置/启用/禁用
   - 有效对象: yml/properties/conf/json/端口/IP/参数/依赖
   - 命令行: cd/ls/mvn/docker/pip 等命令 + 回车
   - 典型: "修改 server.port 为 8080", "输入 docker-compose up"

### 负面关键词库 - 命中且无核心特征则排除
- 纯概念词: "是/属于/定义为/称为/叫做"
- 模糊描述词: "操作一下/执行一下/做一下/弄一下"

### 单特征否决规则
- 仅靠单个关键词（如"左旋""步骤""计算"）**不能判定**
- 必须满足 **主体 + 描述 + 目标** 三要素齐全

## ⚠️ 隐性特征挖掘 (防止伪阴性)

对隐晦文本进行语义补全:
- "红黑树进行左旋调整" → 隐含左旋的标准化步骤 → 可能是过程性知识
- "左旋后黑色高度不变" → 隐含证明左旋正确性的逻辑 → 可能是推演

### 边界判定规则
- **过程性知识 vs 实操**: 以核心目标为准
  - 目标是还原算法流程 → 过程性知识
  - 目标是展示操作行为 → 实操

- **过程性知识 vs 推演**: 以核心描述为准
  - 描述重点是执行步骤 → 过程性知识
  - 描述重点是因果推导 → 推演

## 输出格式 (JSON)
{{
  "subject": "抽象知识/算法/机制" | "人/操作者" | "逻辑/公式/问题" | "概念/定义",
  "description": "标准化步骤" | "动手操作动作" | "推理/演算/论证步骤" | "解释/说明",
  "goal": "还原流程" | "复刻操作" | "展示思维" | "知晓概念" | "完成配置",
  "knowledge_type": "过程性知识" | "实操" | "推演" | "讲解型" | "环境配置",
  "confidence": 0.0-1.0,
  "step_count": 0,
  "causal_count": 0,
  "has_specific_action": false,
  "key_evidence": "从字幕中提取的关键证据(30字以内)",
  "reasoning": "简要分析理由(50字以内)"
}}

请只输出JSON，不要有其他内容。"""

# Dynamic User Prompt (changes per request)
USER_PROMPT_TEMPLATE = """请分析以下动作单元:

## 语义单元上下文
**标题**: {title}
**完整文本**: {full_text}

## 动作单元
**时间范围**: {action_start:.1f}s - {action_end:.1f}s
**字幕文本**: 
{action_subtitles}"""



class KnowledgeClassifier:
    """类说明：KnowledgeClassifier 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        step2_path: Optional[str] = None,
        subtitle_repo: Optional[SubtitleRepository] = None,
    ):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        决策逻辑：
        - 条件：not self.api_key
        依据来源（证据链）：
        - 对象内部状态：self.api_key。
        输入参数：
        - api_key: 函数入参（类型：Optional[str]）。
        - base_url: 函数入参（类型：Optional[str]）。
        - step2_path: 文件路径（类型：Optional[str]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        self.base_url = base_url or "https://api.deepseek.com"
        self.step2_path = str(step2_path or "")
        self.subtitle_repo = subtitle_repo or SubtitleRepository(step2_path=self.step2_path)
        self._batch_system_prompt = get_prompt(
            PromptKeys.DEEPSEEK_KC_BATCH_SYSTEM,
            fallback=self.BATCH_SYSTEM_PROMPT,
        )
        self._batch_user_template = get_prompt(
            PromptKeys.DEEPSEEK_KC_BATCH_USER,
            fallback=self.BATCH_USER_TEMPLATE,
        )
        self._multi_unit_user_template = get_prompt(
            PromptKeys.DEEPSEEK_KC_MULTI_UNIT_USER,
            fallback=self.MULTI_UNIT_USER_TEMPLATE,
        )
        
        if not self.api_key:
            logger.warning("DEEPSEEK_API_KEY not set, classification will be disabled")
            self._enabled = False
            self._llm_client = None
            self._fast_llm_client = None
        else:
            self._enabled = True
            
            # Load config for model names
            self.smart_model = "deepseek-chat"
            self.fast_model = "deepseek-chat"
            try:
                import yaml
                config_path = self._resolve_config_path()

                if config_path and os.path.exists(config_path):
                    with open(config_path, "r", encoding="utf-8") as f:
                        config = yaml.safe_load(f)
                        ai_config = config.get("ai", {}).get("analysis", {})
                        self.smart_model = ai_config.get("model", "deepseek-chat")
                        self.fast_model = ai_config.get("fast_model") or "deepseek-chat"
                    logger.info(f"Loaded knowledge models from config: Smart='{self.smart_model}', Fast='{self.fast_model}'")
                else:
                    logger.warning("Config not found (checked unified config path), using defaults")
            except Exception as e:
                logger.warning(f"Failed to load config.yaml: {e}")

            # 统一入口：DeepSeek 客户端由网关管理，避免重复初始化
            self._llm_client = llm_gateway.get_deepseek_client(
                api_key=self.api_key,
                base_url=self.base_url + "/v1",
                model=self.smart_model,
            )
            self._fast_llm_client = llm_gateway.get_deepseek_client(
                api_key=self.api_key,
                base_url=self.base_url + "/v1",
                model=self.fast_model,
            )

    @staticmethod
    def _resolve_config_path() -> str:
        """方法说明：KnowledgeClassifier._resolve_config_path 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        resolved = resolve_video_config_path(anchor_file=__file__)
        return str(resolved) if resolved else ""
    
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
    
    BATCH_SYSTEM_PROMPT = """你是一个基于【第一性原理】的 AI 知识架构师。你的任务是透过表面的关键词（形式），洞察字幕产生的根本动机（语义）。

## 核心原则：去形式化，重语义
⚠️ **严禁** 仅凭“点击”、“因为”、“首先”等关键词进行机械分类。
✅ **必须** 结合上下文，问自己：如果把这句话删掉，用户失去的是什么？（是失去了一个概念？失去了一个操作步骤？还是失去了一个逻辑证明？）

## 一、五大本质公理 (Mutually Exclusive)

1. **【讲解型】 (Explanation)**
   - **失去它，用户失去了什么**：失去对事物定义的知晓，或看不到最终效果。
   - **本质**：静态信息传递、概念定义、或 **最终效果展示 (Demo)**。
   - **陷阱**：如果不涉及具体“怎么做”或“为什么”，仅仅是“看那里”，就是讲解型。

2. **【环境配置】 (Configuration)**
   - **失去它，用户失去了什么**：无法搭建起程序运行的舞台。
   - **本质**：对依赖、参数、系统的设置。
   - **特征**：对象通常是静态的文件、变量或系统服务。

3. **【过程性知识】 (Process)**
   - **失去它，用户失去了什么**：搞不懂事物内部是如何流转/运作的。
   - **本质**：揭示 **机制、算法或逻辑的动态执行流**。
   - **辨析**：它描述客观规律（如“数据包会经过路由器...”），而非主观操作（如“我去点击路由器的开关...”）。

4. **【实操】 (Practical)**
   - **失去它，用户失去了什么**：无法复刻具体的交互动作。
   - **本质**：人与计算机的直接交互指令集。
   - **特征**：必须包含明确的动作施加者（人）和操作对象。

5. **【推演】 (Deduction)**
   - **失去它，用户失去了什么**：只知其然，不知其所以然。
   - **本质**：逻辑闭环的构建、设计哲学的论证。
   - **辨析**：如果是在解释“为什么要这样设计”或“导致Bug的根本原因”，就是推演。

## 二、认知优先级 (Cognitive Hierarchy)
当内容混合时，按认知价值排序：
**推演 (Why) > 实操/配置 (How to do) > 过程性知识 (How it works) > 讲解型 (What is it)**

    ## 输出格式 (JSON Array)
    - 你必须返回 JSON 数组
    - 每个对象必须包含字段：id / knowledge_type / confidence / reasoning / key_evidence
    - 其中 id 必须严格等于输入中的 ID（用于回填映射）
    [
        {
            "id": 0,
            "knowledge_type": "过程性知识",
            "confidence": 0.95,
            "reasoning": "虽然含有'点击'一词，但核心意图是解释点击触发后的事件冒泡机制，而非教用户复刻点击动作。",
            "key_evidence": "事件会向上传递直到被捕获"
        }
    ]
    仅输出 JSON 数组，无其他内容。"""

    BATCH_USER_TEMPLATE = """请批量分析以下动作单元:

## 全局上下文
**标题**: {title}
**完整文本**: {full_text}

## 待分析动作单元列表
{batch_content}"""

    MULTI_UNIT_USER_TEMPLATE = """请批量分析以下【多个语义单元】的动作单元。

## 重要约束
- 你必须返回 JSON 数组
- 每个结果对象必须包含字段：id / knowledge_type / confidence / reasoning / key_evidence
- 其中 id 必须严格等于输入 actions[*].id（格式形如 "SU001:action_1"），用于回填映射

## 输入数据（JSON）
{units_json}
"""

    async def classify_batch(
        self,
        semantic_unit_title: str,
        semantic_unit_text: str,
        action_segments: list
    ) -> list:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、asyncio 异步调度实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not action_segments
        - 条件：len(items) > 0
        - 条件：avg_len < 30
        依据来源（证据链）：
        - 输入参数：action_segments。
        - 对象内部状态：self._enabled。
        输入参数：
        - semantic_unit_title: 函数入参（类型：str）。
        - semantic_unit_text: 函数入参（类型：str）。
        - action_segments: 函数入参（类型：list）。
        输出参数：
        - 列表结果（与输入或处理结果一一对应）。"""
        if not action_segments:
            return []

        # 1. Prepare items with unique IDs for mapping back
        items = []
        avg_len = 0
        for i, action in enumerate(action_segments):
            start = action.get("start_sec", 0)
            end = action.get("end_sec", 0)
            subs = self._get_subtitles_in_range(start, end)

            avg_len += len(subs)
            items.append({
                "id": i,
                "start": start,
                "end": end,
                "subtitles": subs
            })
        
        if len(items) > 0:
            avg_len /= len(items)
        
        # 2. Dynamic Batch Size Determination（按 token 与资源动态调整）
        # 估算 token：字符/4（用户确认）
        avg_tokens = max(1, int(avg_len / 4))
        token_budget = 3500  # 预留系统提示与结构化输出空间
        BATCH_SIZE = max(1, min(20, token_budget // avg_tokens))
        
        # 资源保护：CPU/内存高时下调 batch
        try:
            import psutil
            cpu_percent = psutil.cpu_percent(interval=None)
            mem_percent = psutil.virtual_memory().percent
            if cpu_percent > 85 or mem_percent > 85:
                BATCH_SIZE = max(1, int(BATCH_SIZE * 0.5))
            elif cpu_percent > 70 or mem_percent > 75:
                BATCH_SIZE = max(1, int(BATCH_SIZE * 0.7))
        except Exception as e:
            logger.debug(f"Resource check skipped: {e}")
        
        # 保底：确保单批 token 不超过 4k
        if avg_tokens * BATCH_SIZE > 4000:
            BATCH_SIZE = max(1, 4000 // avg_tokens)
            
        chunks = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
        
        # 🚀 Model Routing: Use Fast Model for simple/short tasks
        selected_client = self._llm_client
        model_name = self.smart_model
        if avg_len < 300 and self._fast_llm_client:
            selected_client = self._fast_llm_client
            model_name = self.fast_model
            logger.info(f"Routing to FAST model ({model_name}) | AvgLen: {avg_len:.0f} < 300")
        else:
            logger.info(f"Routing to SMART model ({model_name}) | AvgLen: {avg_len:.0f} >= 300")

        logger.info(f"Dynamic Batching: {len(items)} items, avg_len={avg_len:.0f} chars "
                    f"→ Batch Size {BATCH_SIZE}, {len(chunks)} chunks")

        # 3. Concurrent Execution
        results_map = {} # id -> result

        # 解析失败时的拆分重试深度（避免极端情况下递归过深导致请求风暴）
        max_split_depth = int(os.getenv("MODULE2_KC_BATCH_SPLIT_MAX_DEPTH", "6") or "6")
        
        async def _process_chunk(chunk_items, split_depth: int = 0, is_retry: bool = False):
            """
            执行逻辑：
            1) 准备必要上下文与参数。
            2) 执行核心处理并返回结果。
            实现方式：通过内部方法调用/状态更新、JSON 解析/序列化实现。
            核心价值：封装逻辑单元，提升复用与可维护性。
            决策逻辑：
            - 条件：not self._enabled
            - 条件：isinstance(data, dict) and 'items' in data
            - 条件：isinstance(data, list)
            依据来源（证据链）：
            - 对象内部状态：self._enabled。
            输入参数：
            - chunk_items: 函数入参（类型：未标注）。
            输出参数：
            - 列表结果（与输入或处理结果一一对应）。"""
            try:
                # Build Batch Prompt
                batch_content = ""
                for item in chunk_items:
                    batch_content += f"""
---
ID: {item['id']}
时间: {item['start']:.1f}-{item['end']:.1f}
字幕: {item['subtitles']}
"""
                # 🚀 V3: 使用拆分 Prompt，system_message 触发 KV Cache
                user_prompt = render_prompt(
                    PromptKeys.DEEPSEEK_KC_BATCH_USER,
                    context={
                        "title": semantic_unit_title,
                        "full_text": semantic_unit_text,
                        "batch_content": batch_content,
                    },
                    fallback=self._batch_user_template,
                )
                if is_retry:
                    user_prompt += "\n\n⚠️ 请严格输出可被 json.loads 解析的 JSON 数组；不要使用 ``` 代码围栏；字符串中禁止未转义换行/制表符。"
                
                if not self._enabled:
                    return []

                # 🚀 使用选定的 LLM Client 进行异步调用
                content, _, _ = await llm_gateway.deepseek_complete_text(
                    prompt=user_prompt,
                    system_message=self._batch_system_prompt,
                    client=selected_client,
                )
                
                # Parse JSON Array (兼容 Markdown 包裹/尾随文本)
                parsed_items = self._parse_batch_content(content)
                if parsed_items:
                    return parsed_items
                logger.warning(
                    f"Batch JSON parse failed (items={len(chunk_items)}, depth={split_depth}): {content[:120]}..."
                )

                # 兜底：常见原因是输出截断/格式漂移，尝试将 chunk 拆小后重试
                if len(chunk_items) > 1 and split_depth < max_split_depth:
                    mid = max(1, len(chunk_items) // 2)
                    left = await _process_chunk(chunk_items[:mid], split_depth=split_depth + 1, is_retry=True)
                    right = await _process_chunk(chunk_items[mid:], split_depth=split_depth + 1, is_retry=True)
                    return (left or []) + (right or [])

                return []
            except Exception as e:
                logger.error(f"Chunk processing failed: {e}")
                return []

        # Use asyncio.gather to process chunks in parallel
        tasks = [_process_chunk(chunk) for chunk in chunks]
        all_chunk_res = await asyncio.gather(*tasks)
        
        

        def _ingest_results(chunk_res: list) -> None:
            for res in chunk_res or []:
                if not isinstance(res, dict):
                    continue
                if "id" not in res:
                    continue
                idx = self._normalize_batch_index(res.get("id"))
                if idx is None or not (0 <= idx < len(items)):
                    logger.warning(f"Skip invalid batch result id: {res.get('id')!r}")
                    continue

                reasoning = str(res.get("reasoning", "") or "")
                key_evidence = str(res.get("key_evidence", "") or "") or reasoning[:30]
                results_map[idx] = {
                    "knowledge_type": res.get("knowledge_type", "过程性知识"),
                    "confidence": safe_float(res.get("confidence", 0.5), default=0.5),
                    "key_evidence": key_evidence,
                    "reasoning": reasoning,
                }

        for chunk_res in all_chunk_res:
            _ingest_results(chunk_res or [])

        # 兜底：若仍有缺失，尝试对缺失项做一次“缩小范围”的重试（避免整批回退默认值）
        missing_indices = [i for i in range(len(items)) if i not in results_map]
        if missing_indices and self._enabled:
            retry_items = [items[i] for i in missing_indices]
            retry_res = await _process_chunk(retry_items, split_depth=0, is_retry=True)
            _ingest_results(retry_res or [])

        # 4. Assemble final results in order
        final_results = []
        for i in range(len(items)):
            if i in results_map:
                final_results.append(results_map[i])
            else:
                logger.warning(f"Item {i} missing from batch results, using fallback default")
                final_results.append({
                    "knowledge_type": "过程性知识", 
                    "confidence": 0.5,
                    "key_evidence": "Batch Miss",
                    "reasoning": "批量分类缺失，已使用默认兜底。"
                })
                
        return final_results

    async def classify_units_batch(self, units: list, external_limiter: Optional[Any] = None) -> Dict[str, list]:
        """
        做什么：对多个语义单元的动作单元做“跨 unit”批量分类。
        为什么：单 unit 调一次 LLM 在大任务下会形成瓶颈；跨 unit 合并请求可显著降低调用次数与调度开销（参考 LLM调用优化.md）。
        权衡：单次 prompt 更长，需通过 token_budget 动态分块避免超过上下文窗口；解析失败时回退到缺省结果。
        输入：
        - units: 形如 [{unit_id,title/full_text,action_segments:[{id,start_sec,end_sec}, ...]}, ...]
        - external_limiter: 可选外部并发控制器（用于 gRPC 层 AIMD 探测），需支持 acquire/release/record_success/record_failure
        输出：
        - Dict[unit_id, list[classification]]（顺序与输入 action_segments 对齐）
        """
        if not units:
            return {}

        # 降级：未启用或无 client
        if not self._enabled or not self._llm_client:
            fallback: Dict[str, list] = {}
            for u in units:
                unit_id = str(u.get("unit_id", "") or "")
                segs = u.get("action_segments", []) or []
                fallback[unit_id] = [
                    {"knowledge_type": "过程性知识", "confidence": 0.5, "key_evidence": "LLM Disabled"}
                    for _ in segs
                ]
            return fallback

        token_budget = int(os.getenv("MODULE2_KC_MULTI_TOKEN_BUDGET", "3500") or "3500")
        max_units_per_chunk = int(os.getenv("MODULE2_KC_MULTI_MAX_UNITS_PER_CHUNK", "6") or "6")
        max_full_text_chars = int(os.getenv("MODULE2_KC_MULTI_FULL_TEXT_CHARS", "600") or "600")

        # 1) 预处理：构建每个 unit 的 payload（actions[*].id 用于回填）
        unit_payloads = []
        total_sub_chars = 0
        total_actions = 0

        for u in units:
            unit_id = str(u.get("unit_id", "") or "")
            title = str(u.get("title", "") or u.get("semantic_unit_title", "") or "")
            full_text = str(u.get("full_text", "") or u.get("semantic_unit_text", "") or u.get("text", "") or "")
            if max_full_text_chars > 0 and len(full_text) > max_full_text_chars:
                full_text = full_text[:max_full_text_chars] + "..."

            actions_in = u.get("action_segments", []) or []
            actions_payload = []

            for idx, action in enumerate(actions_in):
                start = action.get("start_sec", 0)
                end = action.get("end_sec", 0)
                action_id = action.get("id", idx)
                key = f"{unit_id}:{action_id}"
                subs = self._get_subtitles_in_range(start, end)

                total_sub_chars += len(subs)
                total_actions += 1
                actions_payload.append(
                    {
                        "id": key,
                        "start": start,
                        "end": end,
                        "subtitles": subs,
                    }
                )

            unit_payloads.append(
                {
                    "unit_id": unit_id,
                    "title": title,
                    "full_text": full_text,
                    "actions": actions_payload,
                }
            )

        avg_len = (total_sub_chars / total_actions) if total_actions else 0

        # 2) 动态分块：按 token_budget 近似装箱，减少 LLM 请求次数
        def est_unit_tokens(payload: dict) -> int:
            base_chars = len(payload.get("title", "")) + len(payload.get("full_text", ""))
            action_chars = 0
            for a in payload.get("actions", []) or []:
                action_chars += len(a.get("subtitles", ""))
            return max(1, int((base_chars + action_chars) / 4))

        chunks: List[List[dict]] = []
        cur: List[dict] = []
        cur_tokens = 0

        for payload in unit_payloads:
            u_tokens = est_unit_tokens(payload)
            if cur and (cur_tokens + u_tokens > token_budget or len(cur) >= max_units_per_chunk):
                chunks.append(cur)
                cur = []
                cur_tokens = 0
            cur.append(payload)
            cur_tokens += u_tokens
        if cur:
            chunks.append(cur)

        # 3) Model Routing（沿用单 unit 的快慢模型策略）
        selected_client = self._llm_client
        model_name = self.smart_model
        if avg_len < 300 and self._fast_llm_client:
            selected_client = self._fast_llm_client
            model_name = self.fast_model
            logger.info(f"[MultiUnit] Routing to FAST model ({model_name}) | AvgLen: {avg_len:.0f} < 300")
        else:
            logger.info(f"[MultiUnit] Routing to SMART model ({model_name}) | AvgLen: {avg_len:.0f} >= 300")

        logger.info(
            f"[MultiUnit] {len(units)} units / {total_actions} actions -> {len(chunks)} chunks "
            f"(token_budget={token_budget}, max_units_per_chunk={max_units_per_chunk})"
        )

        async def _process_chunk(chunk_units: List[dict]) -> list:
            acquired = 0
            try:
                if external_limiter is not None:
                    acquired = await external_limiter.acquire()

                units_json = json.dumps(chunk_units, ensure_ascii=False, separators=(",", ":"))
                user_prompt = render_prompt(
                    PromptKeys.DEEPSEEK_KC_MULTI_UNIT_USER,
                    context={"units_json": units_json},
                    fallback=self._multi_unit_user_template,
                )

                content, _, _ = await llm_gateway.deepseek_complete_text(
                    prompt=user_prompt,
                    system_message=self._batch_system_prompt,
                    client=selected_client,
                )
                parsed_items = self._parse_batch_content(content)
                if parsed_items:
                    if external_limiter is not None:
                        await external_limiter.record_success()
                    return parsed_items

                logger.warning(
                    f"[MultiUnit] Batch JSON parse failed (units={len(chunk_units)}): {content[:120]}..."
                )
                if external_limiter is not None:
                    await external_limiter.record_failure(is_rate_limit=False)
                return []
            except Exception as e:
                if external_limiter is not None:
                    error_msg = str(e)
                    is_rate = "429" in error_msg or "rate" in error_msg.lower() or "Too Many Requests" in error_msg
                    await external_limiter.record_failure(is_rate_limit=is_rate)
                logger.error(f"[MultiUnit] Chunk processing failed: {e}")
                return []
            finally:
                if external_limiter is not None and acquired:
                    await external_limiter.release(acquired)

        # 并行处理 chunks（chunk 数量通常远小于 unit 数，能显著减少 API 调用次数）
        all_chunk_res = await asyncio.gather(*[_process_chunk(c) for c in chunks])

        # 4) 归并：key -> classification
        

        results_map: Dict[str, Dict[str, Any]] = {}
        for chunk_res in all_chunk_res:
            for res in chunk_res or []:
                if not isinstance(res, dict):
                    continue
                res_id = res.get("id")
                if not res_id:
                    continue
                results_map[str(res_id)] = {
                    "knowledge_type": res.get("knowledge_type", "过程性知识"),
                    "confidence": safe_float(res.get("confidence", 0.5), default=0.5),
                    "key_evidence": res.get("key_evidence", "") or res.get("reasoning", "")[:30],
                    "reasoning": res.get("reasoning", ""),
                }

        # 5) 回填：按输入顺序组装 per-unit 结果
        final: Dict[str, list] = {}
        for u in units:
            unit_id = str(u.get("unit_id", "") or "")
            segs = u.get("action_segments", []) or []
            out: List[Dict[str, Any]] = []
            for idx, action in enumerate(segs):
                action_id = action.get("id", idx)
                key = f"{unit_id}:{action_id}"
                if key in results_map:
                    out.append(results_map[key])
                else:
                    out.append({"knowledge_type": "过程性知识", "confidence": 0.5, "key_evidence": "Batch Miss"})
            final[unit_id] = out

        return final

    def _parse_batch_content(self, content: str) -> list:
        """
        做什么：解析 LLM 批量返回的 JSON 列表。
        为什么：LLM 输出可能包含代码围栏或尾随文本，直接 json.loads 失败。
        权衡：容错解析可能忽略尾随非 JSON 信息。
        """
        if not content:
            return []

        text = content.strip()
        candidates: List[str] = []

        # 1) 原始文本
        candidates.append(text)

        # 2) Markdown code fence（```json 或 ```）
        for m in re.finditer(r"```(?:json)?\s*([\s\S]*?)```", text, flags=re.IGNORECASE):
            fenced = (m.group(1) or "").strip()
            if fenced:
                candidates.append(fenced)

        # 3) 提取第一个“括号配平”的数组/对象（避免简单 rfind 在截断时失效）
        balanced_array = self._extract_first_balanced_json(text, "[", "]")
        if balanced_array:
            candidates.append(balanced_array)
        balanced_obj = self._extract_first_balanced_json(text, "{", "}")
        if balanced_obj:
            candidates.append(balanced_obj)

        seen: set = set()
        single_obj: Optional[dict] = None
        for candidate in candidates:
            if not candidate:
                continue
            if candidate in seen:
                continue
            seen.add(candidate)
            data = self._loads_jsonish(candidate)
            if isinstance(data, dict):
                if "items" in data:
                    items = data.get("items")
                    return items if isinstance(items, list) else []
                # 单个对象先暂存：优先尝试解析出数组/多对象，避免“数组截断”只返回第一个对象
                if single_obj is None and ("knowledge_type" in data or ("id" in data and "confidence" in data)):
                    single_obj = data
            if isinstance(data, list):
                return data

        # 4) 最后兜底：尝试从原始文本中逐个抽取对象并解析（可救回“数组截断/逗号异常”导致的整体失败）
        obj_texts = self._extract_top_level_objects(text)
        if obj_texts:
            parsed: List[dict] = []
            for obj in obj_texts:
                data = self._loads_jsonish(obj)
                if isinstance(data, dict):
                    parsed.append(data)
            if parsed:
                return parsed

        if single_obj is not None:
            return [single_obj]

        return []

    @staticmethod
    def _extract_first_balanced_json(text: str, open_ch: str, close_ch: str) -> Optional[str]:
        """
        做什么：从文本中提取第一个“括号配平”的片段（数组或对象）。
        为什么：LLM 输出被截断或夹杂多余文本时，find/rfind 容易截取到不完整片段。
        权衡：只返回第一个闭合片段，若输出包含多个 JSON 片段会忽略后续。
        """
        if not text:
            return None
        depth = 0
        start_idx: Optional[int] = None
        in_str = False
        quote = ""
        escape = False
        for i, ch in enumerate(text):
            if in_str:
                if escape:
                    escape = False
                    continue
                if ch == "\\":
                    escape = True
                    continue
                if ch == quote:
                    in_str = False
                    quote = ""
                continue

            if ch in {"\"", "'"}:
                in_str = True
                quote = ch
                continue
            if ch == open_ch:
                if depth == 0:
                    start_idx = i
                depth += 1
                continue
            if ch == close_ch and depth > 0:
                depth -= 1
                if depth == 0 and start_idx is not None:
                    return text[start_idx : i + 1]
        return None

    @staticmethod
    def _extract_top_level_objects(text: str) -> List[str]:
        """
        做什么：从文本中抽取顶层 JSON 对象片段（{...}）。
        为什么：批量数组整体解析失败时，逐对象解析可最大化保留有效结果。
        权衡：只抽取顶层对象；若文本里包含与 JSON 无关的大括号，可能引入噪声对象。
        """
        if not text:
            return []
        objs: List[str] = []
        depth = 0
        start_idx: Optional[int] = None
        in_str = False
        quote = ""
        escape = False

        for i, ch in enumerate(text):
            if in_str:
                if escape:
                    escape = False
                    continue
                if ch == "\\":
                    escape = True
                    continue
                if ch == quote:
                    in_str = False
                    quote = ""
                continue

            if ch in {"\"", "'"}:
                in_str = True
                quote = ch
                continue
            if ch == "{":
                if depth == 0:
                    start_idx = i
                depth += 1
                continue
            if ch == "}" and depth > 0:
                depth -= 1
                if depth == 0 and start_idx is not None:
                    seg = text[start_idx : i + 1].strip()
                    if seg:
                        objs.append(seg)
                    start_idx = None
        return objs

    def _loads_jsonish(self, text: str) -> Any:
        """
        做什么：以“尽量不丢结果”为目标解析 JSON/类 JSON 文本。
        为什么：LLM 输出常见偏差包括：尾随逗号、中文标点、代码围栏、字符串中包含未转义换行等。
        权衡：会对文本做最小必要修复；若输出本身语义错误则仍会解析失败并回退。
        """
        if text is None:
            return None
        raw = str(text).strip().lstrip("\ufeff")
        if not raw:
            return None

        # 1) 先尝试严格 JSON
        try:
            return json.loads(raw)
        except Exception:
            pass

        # 2) 尝试最小修复后再用 json.loads
        normalized = self._normalize_jsonish_text(raw)
        if normalized and normalized != raw:
            try:
                return json.loads(normalized)
            except Exception:
                pass

        # 3) 再尝试 Python literal（兼容单引号/尾随逗号/True/False/None）
        for candidate in (raw, normalized):
            if not candidate:
                continue
            try:
                py_text = self._jsonish_to_python_literal(candidate)
                return ast.literal_eval(py_text)
            except Exception:
                continue

        return None

    @staticmethod
    def _jsonish_to_python_literal(text: str) -> str:
        """
        做什么：把 JSON 关键字（true/false/null）转换为 Python literal。
        为什么：ast.literal_eval 能容忍单引号与尾随逗号，比 json.loads 更宽容。
        权衡：只在字符串外替换关键字，避免污染 reasoning/key_evidence 文本内容。
        """
        if not text:
            return ""
        out: List[str] = []
        i = 0
        in_str = False
        quote = ""
        escape = False

        def _is_ident_char(ch: str) -> bool:
            return ch.isalpha() or ch == "_"

        while i < len(text):
            ch = text[i]
            if in_str:
                out.append(ch)
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == quote:
                    in_str = False
                    quote = ""
                i += 1
                continue

            if ch in {"\"", "'"}:
                in_str = True
                quote = ch
                out.append(ch)
                i += 1
                continue

            if _is_ident_char(ch):
                j = i + 1
                while j < len(text) and (_is_ident_char(text[j]) or text[j].isdigit()):
                    j += 1
                word = text[i:j]
                lower = word.lower()
                if lower == "true":
                    out.append("True")
                elif lower == "false":
                    out.append("False")
                elif lower == "null":
                    out.append("None")
                else:
                    out.append(word)
                i = j
                continue

            out.append(ch)
            i += 1

        return "".join(out)

    @staticmethod
    def _normalize_jsonish_text(text: str) -> str:
        """
        做什么：对“近似 JSON”做最小化修复，让 json.loads 更容易成功。
        为什么：JSON 解析失败往往是轻微格式问题（中文标点、尾随逗号、字符串控制字符）。
        权衡：只在字符串外替换结构符号；字符串内仅转义控制字符，尽量不改语义。
        """
        if not text:
            return ""

        # 1) 统一常见引号（全局替换对语义影响很小，能显著提升解析成功率）
        trans = str.maketrans(
            {
                "“": "\"",
                "”": "\"",
                "‘": "'",
                "’": "'",
            }
        )
        s = str(text).translate(trans).lstrip("\ufeff").strip()

        # 2) 字符串外替换中文标点（避免把结构逗号/冒号输出成全角）
        s = KnowledgeClassifier._replace_outside_strings(s, {"，": ",", "：": ":"})

        # 3) 字符串内转义控制字符（\\n/\\r/\\t 等），否则严格 JSON 会失败
        s = KnowledgeClassifier._escape_control_chars_in_strings(s)

        # 4) 去除尾随逗号（例如 {"a":1,} 或 [1,]）
        s = KnowledgeClassifier._remove_trailing_commas(s)

        return s

    @staticmethod
    def _replace_outside_strings(text: str, mapping: Dict[str, str]) -> str:
        """方法说明：KnowledgeClassifier._replace_outside_strings 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if not text or not mapping:
            return text
        out: List[str] = []
        in_str = False
        quote = ""
        escape = False
        for ch in text:
            if in_str:
                out.append(ch)
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == quote:
                    in_str = False
                    quote = ""
                continue

            if ch in {"\"", "'"}:
                in_str = True
                quote = ch
                out.append(ch)
                continue

            out.append(mapping.get(ch, ch))
        return "".join(out)

    @staticmethod
    def _escape_control_chars_in_strings(text: str) -> str:
        """方法说明：KnowledgeClassifier._escape_control_chars_in_strings 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if not text:
            return ""
        out: List[str] = []
        in_str = False
        quote = ""
        escape = False
        for ch in text:
            if in_str:
                if escape:
                    out.append(ch)
                    escape = False
                    continue
                if ch == "\\":
                    out.append(ch)
                    escape = True
                    continue
                if ch == quote:
                    out.append(ch)
                    in_str = False
                    quote = ""
                    continue
                code = ord(ch)
                if code < 0x20:
                    if ch == "\n":
                        out.append("\\n")
                    elif ch == "\r":
                        out.append("\\r")
                    elif ch == "\t":
                        out.append("\\t")
                    else:
                        out.append(f"\\u{code:04x}")
                else:
                    out.append(ch)
                continue

            if ch in {"\"", "'"}:
                in_str = True
                quote = ch
                out.append(ch)
                continue
            out.append(ch)
        return "".join(out)

    @staticmethod
    def _remove_trailing_commas(text: str) -> str:
        """方法说明：KnowledgeClassifier._remove_trailing_commas 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if not text:
            return ""
        out: List[str] = []
        i = 0
        in_str = False
        quote = ""
        escape = False

        while i < len(text):
            ch = text[i]
            if in_str:
                out.append(ch)
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == quote:
                    in_str = False
                    quote = ""
                i += 1
                continue

            if ch in {"\"", "'"}:
                in_str = True
                quote = ch
                out.append(ch)
                i += 1
                continue

            if ch == ",":
                j = i + 1
                while j < len(text) and text[j] in {" ", "\t", "\r", "\n"}:
                    j += 1
                if j < len(text) and text[j] in {"}", "]"}:
                    i += 1
                    continue

            out.append(ch)
            i += 1

        return "".join(out)

    @staticmethod
    def _normalize_batch_index(val: Any) -> Optional[int]:
        """
        做什么：将 LLM 返回的 id 归一为批量序号（0..N-1）。
        为什么：LLM 可能输出 "0" / 0 / "ID:0" 等非严格格式；直接 int() 容易失败或抛异常。
        权衡：只提取第一个整数；若输出完全不可解析则返回 None。
        """
        if val is None:
            return None
        if isinstance(val, int):
            return val
        if isinstance(val, float):
            try:
                return int(val)
            except Exception:
                return None
        s = str(val).strip()
        if not s:
            return None
        try:
            return int(s)
        except Exception:
            pass
        m = re.search(r"-?\d+", s)
        if not m:
            return None
        try:
            return int(m.group(0))
        except Exception:
            return None
    
    def _load_all_subtitles(self) -> list:
        """方法说明：KnowledgeClassifier._load_all_subtitles 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        self.subtitle_repo.set_paths(step2_path=self.step2_path, clear_cache=False)
        return self.subtitle_repo.list_subtitles()

    def _get_subtitles_in_range(self, start_sec: float, end_sec: float) -> str:
        """方法说明：KnowledgeClassifier._get_subtitles_in_range 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        self.subtitle_repo.set_paths(step2_path=self.step2_path, clear_cache=False)
        return self.subtitle_repo.get_subtitles_in_range(
            start_sec,
            end_sec,
            expand_to_sentence_boundary=True,
            include_ts_prefix=True,
            empty_fallback="(无字幕)",
        )

