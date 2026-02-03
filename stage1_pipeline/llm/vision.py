"""
ERNIE Vision API 客户端
用于视觉问答校验
"""

import base64
import httpx
import json
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime
from pathlib import Path

from .client import LLMClient, LLMConfig, LLMResponse


class ERNIEVisionClient(LLMClient):
    """ERNIE Vision API 客户端"""
    
    def __init__(self, config: LLMConfig):
        super().__init__(config)
        self._client: Optional[httpx.AsyncClient] = None
        
    async def _get_client(self) -> httpx.AsyncClient:
        """获取 HTTP 客户端"""
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
        """关闭客户端"""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    def _encode_image(self, image_path: str) -> str:
        """将图片编码为 base64"""
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
        """完成文本生成（不含图片）"""
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
        带图片的完成
        
        Args:
            prompt: 提示词
            image_path: 图片路径
            system_prompt: 系统提示词
        """
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
        带多张图片的完成
        
        Args:
            prompt: 提示词
            image_paths: 图片路径列表
            system_prompt: 系统提示词
        """
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
        """完成 JSON 生成，带自动修复功能"""
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
        """尝试修复常见的 JSON 格式错误"""
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
        校验帧内容（步骤14专用）
        
        Args:
            image_path: 帧图片路径
            questions: 校验问题列表
            fault_type: 断层类型
            
        Returns:
            {
                "answers": [...],
                "grade": "A/B/C/不合格",
                "extracted_content": {...}
            }
        """
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
