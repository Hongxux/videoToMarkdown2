"""
模块说明：阶段 LLM 适配 vision 的实现。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import base64
import httpx
import json
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime
from pathlib import Path

from .client import LLMClient, LLMConfig, LLMResponse


class ERNIEVisionClient(LLMClient):
    """类说明：ERNIEVisionClient 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(self, config: LLMConfig):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、HTTP 调用实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - config: 配置对象/字典（类型：LLMConfig）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        super().__init__(config)
        self._client: Optional[httpx.AsyncClient] = None
        
    async def _get_client(self) -> httpx.AsyncClient:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、HTTP 调用实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self._client is None
        依据来源（证据链）：
        - 对象内部状态：self._client。
        输入参数：
        - 无。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        if self._client is None:
            self._client = httpx.AsyncClient(
                headers={
                    "Authorization": f"Bearer {self.config.api_key}",
                    "Content-Type": "application/json"
                },
                timeout=self.config.timeout
            )
        return self._client
    
    async def close(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self._client
        依据来源（证据链）：
        - 对象内部状态：self._client。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    def _encode_image(self, image_path: str) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not path.exists()
        依据来源（证据链）：
        输入参数：
        - image_path: 文件路径（类型：str）。
        输出参数：
        - 字符串结果。"""
        path = Path(image_path)
        if not path.exists():
            raise FileNotFoundError(f"Image not found: {image_path}")
        
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    
    async def complete(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> LLMResponse:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、HTTP 调用实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：system_prompt
        - 条件：temperature is not None
        依据来源（证据链）：
        - 输入参数：system_prompt, temperature。
        输入参数：
        - prompt: 文本内容（类型：str）。
        - system_prompt: 函数入参（类型：Optional[str]）。
        - temperature: 函数入参（类型：Optional[float]）。
        - **kwargs: 可变参数，含义由调用方决定。
        输出参数：
        - LLMResponse 响应对象。"""
        client = await self._get_client()
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        payload = {
            "model": self.config.model,
            "messages": messages,
            "temperature": temperature if temperature is not None else self.config.temperature,
        }
        
        start_time = datetime.now()
        
        try:
            response = await client.post(self.config.base_url, json=payload)
            response.raise_for_status()
            data = response.json()
            
            latency_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            content = data["choices"][0]["message"]["content"]
            usage = data.get("usage", {})
            
            self._last_prompt = prompt
            self._last_response = content
            self._last_token_count = usage.get("total_tokens", 0)
            
            return LLMResponse(
                content=content,
                prompt_tokens=usage.get("prompt_tokens", 0),
                completion_tokens=usage.get("completion_tokens", 0),
                total_tokens=usage.get("total_tokens", 0),
                model=self.config.model,
                latency_ms=latency_ms,
                raw_response=data
            )
            
        except httpx.HTTPStatusError as e:
            raise RuntimeError(f"ERNIE API error: {e.response.status_code} - {e.response.text}")
    
    async def complete_with_image(
        self,
        prompt: str,
        image_path: str,
        system_prompt: Optional[str] = None,
        **kwargs
    ) -> LLMResponse:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、HTTP 调用实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：system_prompt
        依据来源（证据链）：
        - 输入参数：system_prompt。
        输入参数：
        - prompt: 文本内容（类型：str）。
        - image_path: 文件路径（类型：str）。
        - system_prompt: 函数入参（类型：Optional[str]）。
        - **kwargs: 可变参数，含义由调用方决定。
        输出参数：
        - LLMResponse 响应对象。"""
        client = await self._get_client()
        
        # 编码图片
        image_base64 = self._encode_image(image_path)
        
        # 构建多模态消息
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        
        # ERNIE Vision 消息格式 (使用 image_url 类型)
        messages.append({
            "role": "user",
            "content": [
                {
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/png;base64,{image_base64}"
                    }
                },
                {
                    "type": "text",
                    "text": prompt
                }
            ]
        })
        
        payload = {
            "model": self.config.model,
            "messages": messages,
            "temperature": self.config.temperature,
        }
        
        start_time = datetime.now()
        
        try:
            response = await client.post(self.config.base_url, json=payload)
            response.raise_for_status()
            data = response.json()
            
            latency_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            content = data["choices"][0]["message"]["content"]
            usage = data.get("usage", {})
            
            self._last_prompt = prompt
            self._last_response = content
            self._last_token_count = usage.get("total_tokens", 0)
            
            return LLMResponse(
                content=content,
                prompt_tokens=usage.get("prompt_tokens", 0),
                completion_tokens=usage.get("completion_tokens", 0),
                total_tokens=usage.get("total_tokens", 0),
                model=self.config.model,
                latency_ms=latency_ms,
                raw_response=data
            )
            
        except httpx.HTTPStatusError as e:
            raise RuntimeError(f"ERNIE Vision API error: {e.response.status_code} - {e.response.text}")
    
    async def complete_with_images(
        self,
        prompt: str,
        image_paths: List[str],
        system_prompt: Optional[str] = None,
        **kwargs
    ) -> LLMResponse:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、HTTP 调用实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：system_prompt
        依据来源（证据链）：
        - 输入参数：system_prompt。
        输入参数：
        - prompt: 文本内容（类型：str）。
        - image_paths: 文件路径（类型：List[str]）。
        - system_prompt: 函数入参（类型：Optional[str]）。
        - **kwargs: 可变参数，含义由调用方决定。
        输出参数：
        - LLMResponse 响应对象。"""
        client = await self._get_client()
        
        # 构建多模态消息内容
        content_parts = []
        
        for image_path in image_paths:
            image_base64 = self._encode_image(image_path)
            content_parts.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/png;base64,{image_base64}"
                }
            })
        
        content_parts.append({
            "type": "text",
            "text": prompt
        })
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": content_parts})
        
        payload = {
            "model": self.config.model,
            "messages": messages,
            "temperature": self.config.temperature,
        }
        
        start_time = datetime.now()
        
        try:
            response = await client.post(self.config.base_url, json=payload)
            response.raise_for_status()
            data = response.json()
            
            latency_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            content = data["choices"][0]["message"]["content"]
            usage = data.get("usage", {})
            
            return LLMResponse(
                content=content,
                prompt_tokens=usage.get("prompt_tokens", 0),
                completion_tokens=usage.get("completion_tokens", 0),
                total_tokens=usage.get("total_tokens", 0),
                model=self.config.model,
                latency_ms=latency_ms,
                raw_response=data
            )
            
        except httpx.HTTPStatusError as e:
            raise RuntimeError(f"ERNIE Vision API error: {e.response.status_code} - {e.response.text}")
    
    async def complete_json(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        **kwargs
    ) -> Tuple[Dict, LLMResponse]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：'```json' in content
        - 条件：'```' in content
        - 条件：repaired
        依据来源（证据链）：
        输入参数：
        - prompt: 文本内容（类型：str）。
        - system_prompt: 函数入参（类型：Optional[str]）。
        - **kwargs: 可变参数，含义由调用方决定。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        response = await self.complete(prompt, system_prompt, **kwargs)
        
        content = response.content.strip()
        if "```json" in content:
            start = content.find("```json") + 7
            end = content.find("```", start)
            content = content[start:end].strip()
        elif "```" in content:
            start = content.find("```") + 3
            end = content.find("```", start)
            content = content[start:end].strip()
            
        try:
            parsed = json.loads(content)
            return parsed, response
        except json.JSONDecodeError as e:
            # 尝试修复 JSON
            repaired = self._repair_json(content)
            if repaired:
                try:
                    parsed = json.loads(repaired)
                    return parsed, response
                except json.JSONDecodeError:
                    pass
            
            raise ValueError(f"Failed to parse JSON: {e}\nContent: {content[:500]}")
    
    def _repair_json(self, content: str) -> Optional[str]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：open_braces > close_braces
        - 条件：open_brackets > close_brackets
        - 条件：content.endswith(',}')
        依据来源（证据链）：
        - 输入参数：content。
        输入参数：
        - content: 文本内容（类型：str）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        # 1. 修复缺失的结尾大括号
        open_braces = content.count('{')
        close_braces = content.count('}')
        if open_braces > close_braces:
            content = content + '}' * (open_braces - close_braces)
        
        # 2. 修复缺失的结尾方括号
        open_brackets = content.count('[')
        close_brackets = content.count(']')
        if open_brackets > close_brackets:
            content = content + ']' * (open_brackets - close_brackets)
        
        # 3. 移除末尾的逗号（在对象或数组结束前）
        content = content.rstrip()
        if content.endswith(',}'):
            content = content[:-2] + '}'
        if content.endswith(',]'):
            content = content[:-2] + ']'
        
        # 4. 修复未闭合的字符串（简单情况）
        # 检查引号是否配对
        quote_count = content.count('"') - content.count('\\"')
        if quote_count % 2 != 0:
            # 如果引号数量为奇数，尝试在末尾添加引号
            content = content + '"'
        
        return content
    
    async def validate_frame(
        self,
        image_path: str,
        questions: List[Dict[str, Any]],
        fault_type: str
    ) -> Dict[str, Any]:
        """
        执行逻辑：
        1) 整理待校验数据。
        2) 按规则逐项校验并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化实现。
        核心价值：提前发现数据/状态问题，降低运行风险。
        决策逻辑：
        - 条件：'```json' in content
        - 条件：core_total == 0
        - 条件：secondary_yes >= secondary_total * 0.6
        依据来源（证据链）：
        - 输入参数：fault_type。
        输入参数：
        - image_path: 文件路径（类型：str）。
        - questions: 函数入参（类型：List[Dict[str, Any]]）。
        - fault_type: 函数入参（类型：str）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        questions_text = "\n".join([
            f"{i+1}. [{q['question_id']}] {q['question']} (核心: {q.get('is_core', True)})"
            for i, q in enumerate(questions)
        ])
        
        prompt = f"""请根据以下图片回答问题，并提取相关内容。

【断层类型】{fault_type}

【校验问题】
{questions_text}

【回答要求】
对每个问题：
1. 回答"是"或"否"
2. 如果回答"是"，提取图中对应的具体内容
3. 如果回答"否"，说明缺失的原因

【输出格式】
{{
  "answers": [
    {{
      "question_id": "Q1",
      "answer": "是/否",
      "extracted_content": "从图中提取的具体内容",
      "missing_reason": "如果否，说明缺失原因"
    }}
  ]
}}"""

        response = await self.complete_with_image(prompt, image_path)
        
        # 解析响应
        content = response.content.strip()
        if "```json" in content:
            start = content.find("```json") + 7
            end = content.find("```", start)
            content = content[start:end].strip()
            
        try:
            result = json.loads(content)
        except json.JSONDecodeError:
            result = {"answers": [], "error": "Failed to parse response"}
        
        # 计算等级
        answers = result.get("answers", [])
        core_yes = sum(1 for a in answers if a.get("answer") == "是" and 
                      any(q.get("is_core", True) for q in questions if q["question_id"] == a["question_id"]))
        core_total = sum(1 for q in questions if q.get("is_core", True))
        secondary_yes = sum(1 for a in answers if a.get("answer") == "是" and
                          any(not q.get("is_core", True) for q in questions if q["question_id"] == a["question_id"]))
        secondary_total = sum(1 for q in questions if not q.get("is_core", True))
        
        # 改进：根据断层类型使用不同阈值
        fault_type = result.get("fault_type")
        
        import logging
        logger = logging.getLogger(__name__)
        logger.debug(f"[VisionQA] core={core_yes}/{core_total}, sec={secondary_yes}/{secondary_total}, fault={fault_type}")
        
        if core_total == 0:
            # 无核心问题（如指代模糊类）
            if secondary_yes >= secondary_total * 0.6:
                grade = "B"
            elif secondary_yes > 0:
                grade = "C"
            else:
                grade = "不合格"
        else:
            core_ratio = core_yes / core_total
            # 概念定义(3):100%, 实操/对比(4,10):70%, 其他:80%
            if fault_type == 3:
                threshold = 1.0
            elif fault_type in [4, 10]:
                threshold = 0.7
            else:
                threshold = 0.8
                
            if core_ratio >= threshold:
                grade = "A" if secondary_yes >= secondary_total * 0.5 else "B"
            elif core_ratio >= 0.6:
                grade = "C"
            else:
                grade = "不合格"
        
        logger.info(f"[VisionQA] Grade={grade} (fault_type={fault_type})")
            
        result["grade"] = grade
        result["extracted_content"] = {
            a["question_id"]: a.get("extracted_content", "")
            for a in answers if a.get("answer") == "是"
        }
        
        return result
