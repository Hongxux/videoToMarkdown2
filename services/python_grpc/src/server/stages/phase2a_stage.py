"""Phase2A 阶段职责：素材请求生成入口。"""

from __future__ import annotations


class Phase2AMaterialStageMixin:
    """Phase2A 阶段 RPC 入口混入。"""

    async def GenerateMaterialRequests(self, request, context):
        """委托核心实现处理素材请求生成。"""
        return await self._phase2a_generate_material_requests_impl(request, context)
