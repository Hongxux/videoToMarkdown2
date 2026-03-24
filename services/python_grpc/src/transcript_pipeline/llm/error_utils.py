from __future__ import annotations

import re
from typing import Any, Optional

import httpx


def normalize_error_text(value: Any, *, max_chars: int = 400) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if len(text) > max_chars:
        return text[: max_chars - 3] + "..."
    return text


def format_request_summary(request: Optional[httpx.Request]) -> str:
    if request is None:
        return ""
    method = normalize_error_text(getattr(request, "method", ""), max_chars=32)
    url = normalize_error_text(getattr(request, "url", ""), max_chars=300)
    if method and url:
        return f"{method} {url}"
    return method or url


def format_provider_error(
    error: Exception,
    *,
    base_url: str,
    model: str,
    timeout: Optional[float] = None,
) -> str:
    error_type = type(error).__name__
    details = []

    message = normalize_error_text(error)
    if not message:
        arg_messages = []
        for item in getattr(error, "args", ()) or ():
            normalized = normalize_error_text(item)
            if normalized:
                arg_messages.append(normalized)
        message = ", ".join(arg_messages)
    details.append(f"message={message or '<empty>'}")

    request: Optional[httpx.Request] = getattr(error, "request", None)
    if isinstance(error, httpx.HTTPStatusError):
        response = getattr(error, "response", None)
        request = request or getattr(response, "request", None)
        status_code = getattr(response, "status_code", None)
        if status_code is not None:
            details.append(f"status={int(status_code)}")
        reason_phrase = normalize_error_text(getattr(response, "reason_phrase", ""))
        if reason_phrase:
            details.append(f"reason={reason_phrase}")
        try:
            response_text = normalize_error_text(
                response.text if response is not None else "",
                max_chars=600,
            )
        except Exception:
            response_text = ""
        if response_text:
            details.append(f"body={response_text}")

    request_summary = format_request_summary(request)
    details.append(f"request={request_summary or '<unknown>'}")
    details.append(f"base_url={normalize_error_text(base_url, max_chars=200)}")
    details.append(f"model={normalize_error_text(model, max_chars=80)}")
    if isinstance(error, httpx.TimeoutException) and timeout is not None:
        details.append(f"timeout={float(timeout):.1f}s")
    return f"{error_type} ({'; '.join(details)})"
