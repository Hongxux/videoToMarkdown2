"""Server service export layer.

This module exposes the new server symbol location while delegating to
`grpc_service_impl.py`.
"""

from __future__ import annotations

from types import ModuleType
from typing import Any


def _impl_module() -> ModuleType:
    """Lazy-load server implementation module."""
    from . import grpc_service_impl as impl

    return impl


def __getattr__(name: str) -> Any:
    """Forward unknown symbols to implementation module."""
    return getattr(_impl_module(), name)


def __dir__() -> list[str]:
    """Expose merged symbol list."""
    symbols = set(globals().keys())
    symbols.update(dir(_impl_module()))
    return sorted(symbols)


async def serve(host: str = "0.0.0.0", port: int = 50051):
    """Forward startup call to implementation module."""
    return await _impl_module().serve(host=host, port=port)


__all__ = ["serve"]
