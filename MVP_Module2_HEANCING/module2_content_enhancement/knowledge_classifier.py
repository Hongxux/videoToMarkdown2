"""
知识分类器 - 基于 LLM 三要素分析

对动作单元进行四分类:
- 过程性知识: 标准化步骤演示
- 实操: 软件/工具操作
- 推演: 数学推导/逻辑论证
- 讲解型: 纯概念解释/背景介绍

V1.0
"""

import os
import json
import logging
from typing import Dict, Optional
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

# Legacy template for backward compatibility (deprecated)
PROMPT_TEMPLATE = SYSTEM_PROMPT + "\n\n" + USER_PROMPT_TEMPLATE


class KnowledgeClassifier:
    """基于 LLM 的知识分类器"""
    
    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None, step2_path: Optional[str] = None):
        """
        初始化分类器
        
        🚀 V2: 使用集中式 LLMClient (连接池+HTTP/2+自适应并发)
        🚀 V3: 支持直接从 step2_correction_output.json 读取字幕
        
        Args:
            api_key: DeepSeek API Key (默认从环境变量 DEEPSEEK_API_KEY 获取)
            base_url: API Base URL (默认 https://api.deepseek.com)
            step2_path: Step 2 字幕文件路径 (可选，用于直接读取完整字幕)
        """
        self.api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        self.base_url = base_url or "https://api.deepseek.com"
        self.step2_path = step2_path
        self._all_subtitles_cache = None  # 缓存完整字幕列表
        
        if not self.api_key:
            logger.warning("DEEPSEEK_API_KEY not set, classification will be disabled")
            self._enabled = False
            self._llm_client = None
        else:
            self._enabled = True
            # 🚀 使用集中式 LLMClient
            from .llm_client import LLMClient
            self._llm_client = LLMClient(
                api_key=self.api_key,
                base_url=self.base_url + "/v1"
            )
    
    @property
    def enabled(self) -> bool:
        return self._enabled
    
    async def classify(
        self,
        title: str,
        full_text: str,
        action_start: float,
        action_end: float,
        action_subtitles: str
    ) -> Dict:
        """
        对动作单元进行知识分类
        
        Args:
            title: 语义单元标题
            full_text: 语义单元完整文本内容
            action_start: 动作单元开始时间 (秒)
            action_end: 动作单元结束时间 (秒)
            action_subtitles: 动作单元时间范围内的字幕文本
            
        Returns:
            dict: 分类结果，包含 knowledge_type, confidence, key_evidence 等
        """
        # 💥 1. 缓存键生成 (使用 MD5 避免过长)
        import hashlib
        cache_key = hashlib.md5(f"{title}|{action_subtitles}".encode('utf-8')).hexdigest()
        
        # 💥 2. 检查内存缓存
        if not hasattr(self, '_cache'):
            self._cache = {}
            # 尝试加载本地缓存文件 (可选，这里简化为内存级持久化，若需跨进程可读写文件)
            # self._load_cache() 
            
        if cache_key in self._cache:
            # logger.debug(f"Cache hit for {title[:10]}...")
            return self._cache[cache_key]

        if not self._enabled:
            # 未启用时返回默认值
            return {
                "knowledge_type": "过程性知识",
                "confidence": 0.5,
                "key_evidence": "API未配置，使用默认分类",
                "subject": "抽象知识/算法/机制",
                "description": "标准化步骤",
                "goal": "还原流程"
            }
        
        # 🚀 V3: 使用拆分的 Prompt 实现前缀缓存 (KV Cache)
        user_prompt = USER_PROMPT_TEMPLATE.format(
            title=title,
            full_text=full_text,
            action_start=action_start,
            action_end=action_end,
            action_subtitles=action_subtitles
        )
        
        try:
            # 🚀 使用集中式 LLMClient (自带连接池+自适应并发)
            # system_message 固定不变，触发 LLM KV Cache
            content, _, _ = await self._llm_client.complete_text(
                prompt=user_prompt,
                system_message=SYSTEM_PROMPT
            )
            
            # 提取 JSON
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
            
            result = json.loads(content)
            logger.debug(f"Classification result: {result.get('knowledge_type')} "
                        f"(conf={result.get('confidence', 0):.0%})")
            
            # 💥 3. 写入缓存
            if hasattr(self, '_cache'):
                self._cache[cache_key] = result
                
            return result
            
        except Exception as e:
            logger.error(f"Knowledge classification failed: {e}")
            return {
                "knowledge_type": "过程性知识",
                "confidence": 0.5,
                "key_evidence": f"分类失败: {str(e)[:20]}",
                "subject": "抽象知识/算法/机制",
                "description": "标准化步骤",
                "goal": "还原流程"
            }
    
    def classify_batch(
        self,
        semantic_unit_title: str,
        semantic_unit_text: str,
        action_segments: list,
        subtitles: list
    ) -> list:
        """
        批量分类多个动作单元
        
        Args:
            semantic_unit_title: 语义单元标题
            semantic_unit_text: 语义单元完整文本
            action_segments: 动作单元列表，每个包含 start, end
            subtitles: 字幕列表，每个包含 start_sec, end_sec, corrected_text
            
        Returns:
            list: 每个动作单元的分类结果
        """

    # 🚀 V3: Batch Prompt 也拆分为 System + User 实现前缀缓存
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
[
    {
        "id": "item_index",
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

    # Legacy template for backward compatibility
    BATCH_PROMPT_TEMPLATE = BATCH_SYSTEM_PROMPT + "\n\n" + BATCH_USER_TEMPLATE

    async def classify_batch(
        self,
        semantic_unit_title: str,
        semantic_unit_text: str,
        action_segments: list
    ) -> list:
        """
        批量并行分类 (Dynamic Batching Strategy)
        
        策略:
        1. 根据文本长度动态决定 Batch Size (Prompt-Level Batching)
        2. 使用 asyncio.gather 并发执行这些 Batch (Concurrent Execution)
        """
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
        
        # 2. Dynamic Batch Size Determination
        # 🚀 DeepSeek Optimization: More aggressive batching for short texts
        if avg_len < 30:
            BATCH_SIZE = 20
        elif avg_len < 100: 
            BATCH_SIZE = 15
        elif avg_len < 300:
            BATCH_SIZE = 10
        elif avg_len < 800:
            BATCH_SIZE = 5
        else:
            BATCH_SIZE = 2
            
        chunks = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
        logger.info(f"Dynamic Batching: {len(items)} items, avg_len={avg_len:.0f} chars "
                    f"→ Batch Size {BATCH_SIZE}, {len(chunks)} chunks")

        # 3. Concurrent Execution
        results_map = {} # id -> result
        
        async def _process_chunk(chunk_items):
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
                user_prompt = self.BATCH_USER_TEMPLATE.format(
                    title=semantic_unit_title,
                    full_text=semantic_unit_text,
                    batch_content=batch_content
                )
                
                if not self._enabled:
                    return []

                # 🚀 使用 LLMClient 进行异步调用，system_message 固定
                content, _, _ = await self._llm_client.complete_text(
                    prompt=user_prompt,
                    system_message=self.BATCH_SYSTEM_PROMPT
                )
                
                # Parse JSON Array
                try:
                    data = json.loads(content)
                    if isinstance(data, dict) and "items" in data:
                        return data["items"]
                    if isinstance(data, list):
                        return data
                    if "```json" in content:
                        parsed = json.loads(content.split("```json")[1].split("```")[0])
                        return parsed if isinstance(parsed, list) else []
                except:
                    logger.warning(f"Batch JSON parse failed: {content[:100]}...")
                    return []
                
                return []
            except Exception as e:
                logger.error(f"Chunk processing failed: {e}")
                return []

        # Use asyncio.gather to process chunks in parallel
        tasks = [_process_chunk(chunk) for chunk in chunks]
        all_chunk_res = await asyncio.gather(*tasks)
        
        for chunk_res in all_chunk_res:
            if chunk_res:
                for res in chunk_res:
                    if isinstance(res, dict) and "id" in res:
                        res_id = res["id"]
                        norm_res = {
                            "knowledge_type": res.get("knowledge_type", "过程性知识"),
                            "confidence": float(res.get("confidence", 0.5)),
                            "key_evidence": res.get("reasoning", "")[:30],
                            "reasoning": res.get("reasoning", ""),
                        }
                        results_map[int(res_id)] = norm_res

        # 4. Assemble final results in order
        final_results = []
        for i in range(len(items)):
            if i in results_map:
                final_results.append(results_map[i])
            else:
                logger.warning(f"Item {i} missing from batch results, doing fallback classify")
                final_results.append({
                    "knowledge_type": "过程性知识", 
                    "confidence": 0.5,
                    "key_evidence": "Batch Miss"
                })
                
        return final_results
    
    def _load_all_subtitles(self) -> list:
        """
        从 step2_correction_output.json 加载完整字幕列表（带缓存）
        
        Returns:
            list: [{"start_sec": float, "end_sec": float, "corrected_text": str}, ...]
        """
        if self._all_subtitles_cache is not None:
            return self._all_subtitles_cache
        
        if not self.step2_path or not os.path.exists(self.step2_path):
            logger.warning(f"Step 2 path not available: {self.step2_path}")
            return []
        
        try:
            with open(self.step2_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            subtitles = []
            corrected_subs = data.get("output", {}).get("corrected_subtitles", [])
            
            for sub in corrected_subs:
                subtitles.append({
                    "start_sec": sub["start_sec"],
                    "end_sec": sub["end_sec"],
                    "corrected_text": sub["corrected_text"]
                })
            
            self._all_subtitles_cache = subtitles
            logger.info(f"Loaded {len(subtitles)} subtitles from Step 2")
            return subtitles
            
        except Exception as e:
            logger.error(f"Failed to load Step 2 subtitles: {e}")
            return []
    
    def _get_subtitles_in_range(self, start_sec: float, end_sec: float) -> str:
        """
        从 Step 2 完整字幕中获取指定时间范围内的字幕文本（扩展到包含边界的完整字幕）
        
        Args:
            start_sec: 动作单元起始时间
            end_sec: 动作单元结束时间
        
        Returns:
            str: 格式化的字幕文本
        """
        # 从 Step 2 加载完整字幕
        all_subtitles = self._load_all_subtitles()
        
        if not all_subtitles:
            return "(无字幕)"
        
        # 第一遍：找到包含 start_sec 和 end_sec 的字幕边界
        effective_start = start_sec
        effective_end = end_sec
        
        for sub in all_subtitles:
            sub_start = sub["start_sec"]
            sub_end = sub["end_sec"]
            
            # 如果 start_sec 落在这个字幕区间内，向前扩展
            if sub_start <= start_sec < sub_end:
                effective_start = min(effective_start, sub_start)
            
            # 如果 end_sec 落在这个字幕区间内，向后扩展
            if sub_start < end_sec <= sub_end:
                effective_end = max(effective_end, sub_end)
        
        # 第二遍：收集扩展后范围内的所有字幕
        texts = []
        for sub in all_subtitles:
            sub_start = sub["start_sec"]
            sub_end = sub["end_sec"]
            text = sub["corrected_text"]
            
            # 字幕与扩展后的时间范围有重叠
            if sub_start < effective_end and sub_end > effective_start:
                texts.append(f"[{sub_start:.1f}s] {text}")
        
        return "\n".join(texts) if texts else "(无字幕)"
    
    def _parse_subtitle(self, sub) -> tuple:
        """解析字幕对象，返回 (start_sec, end_sec, text)"""
        if hasattr(sub, 'start_sec'):
            # CorrectedSubtitle dataclass
            sub_start = sub.start_sec
            sub_end = sub.end_sec
            text = getattr(sub, 'corrected_text', getattr(sub, 'text', ''))
        else:
            # dict 格式
            sub_start = sub.get("start_sec", 0)
            sub_end = sub.get("end_sec", 0)
            text = sub.get("corrected_text", sub.get("text", ""))
        return sub_start, sub_end, text
