"""Validation/VL 阶段职责：CV 校验与视觉语言分析入口。"""

from __future__ import annotations


class ValidationAndVLStageMixin:
    """Validation/VL 阶段 RPC 入口混入。"""

    async def ValidateCVBatch(self, request, context):
        """委托核心实现处理批量 CV 校验（流式）。"""
        async for response in self._validation_validate_cv_batch_impl(request, context):
            yield response

    async def AnalyzeWithVL(self, request, context):
        """委托核心实现处理 VL 分析。"""
        return await self._validation_analyze_with_vl_impl(request, context)

    async def ReleaseCVResources(self, request, context):
        """委托核心实现处理 CV 资源释放。"""
        return await self._validation_release_cv_resources_impl(request, context)
