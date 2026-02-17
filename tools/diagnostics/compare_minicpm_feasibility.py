import asyncio
import base64
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from services.python_grpc.src.content_pipeline.infra.llm.vision_ai_client import (  # noqa: E402
    VisionAIClient,
    VisionAIConfig,
)
from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import (  # noqa: E402
    load_module2_config,
)
from services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer import (  # noqa: E402
    VLVideoAnalyzer,
)
from services.python_grpc.src.config_paths import load_yaml_dict, resolve_video_config_path  # noqa: E402


VISION_PROMPT = """请判断截图是否包含可复用的具体知识信息（例如界面操作、配置步骤、图表、代码、流程图）。
仅输出 JSON：
{
  "has_concrete_knowledge": true/false,
  "concrete_type": "ui|code|chart|diagram|other|none",
  "confidence": 0.0-1.0,
  "reason": "一句话"
}
"""

VL_EXTRA_PROMPT = """请严格按既有 schema 输出 JSON 数组，不要任何解释文本。"""


def _now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def _find_dataset_root(root: Path) -> Path:
    if not root.exists():
        raise FileNotFoundError(f"数据目录不存在: {root}")
    subdirs = [p for p in root.iterdir() if p.is_dir()]
    if not subdirs:
        raise FileNotFoundError(f"数据目录下没有子目录: {root}")
    preferred = root / "20225626c2a19253c4121f684ecdff12"
    if preferred.exists():
        return preferred
    return sorted(subdirs, key=lambda p: p.name)[0]


def _pick_files(dataset_root: Path) -> Dict[str, List[Path]]:
    screenshots = sorted((dataset_root / "screenshots").glob("*.png"))[:3]
    clips = sorted((dataset_root / "clips").glob("*.mp4"))[:2]
    if not screenshots:
        raise FileNotFoundError(f"未找到截图: {dataset_root / 'screenshots'}")
    if not clips:
        raise FileNotFoundError(f"未找到视频片段: {dataset_root / 'clips'}")
    return {"screenshots": screenshots, "clips": clips}


def _load_video_vision_config() -> Dict[str, Any]:
    cfg_path = resolve_video_config_path(anchor_file=__file__)
    if not cfg_path or not cfg_path.exists():
        raise FileNotFoundError(f"video_config.yaml 未找到: {cfg_path}")
    return load_yaml_dict(cfg_path).get("vision_ai", {})


def _make_vision_cfg(raw: Dict[str, Any]) -> VisionAIConfig:
    batch_cfg = raw.get("batch", {}) if isinstance(raw.get("batch"), dict) else {}
    return VisionAIConfig(
        enabled=bool(raw.get("enabled", False)),
        bearer_token=str(raw.get("bearer_token", "")),
        base_url=str(raw.get("base_url", "")),
        model=str(raw.get("model", "ernie-4.5-turbo-vl-32k")),
        temperature=float(raw.get("temperature", 0.1)),
        timeout=float(raw.get("timeout", raw.get("timeout_seconds", 60.0))),
        rate_limit_per_minute=int(raw.get("rate_limit_per_minute", 60)),
        duplicate_detection_enabled=False,
        batch_enabled=bool(batch_cfg.get("enabled", False)),
        batch_max_size=int(batch_cfg.get("max_size", 4)),
        batch_flush_ms=int(batch_cfg.get("flush_ms", 20)),
        batch_max_inflight_batches=int(batch_cfg.get("max_inflight_batches", 2)),
    )


async def run_vision_test(image_paths: List[Path]) -> Dict[str, Any]:
    raw_cfg = _load_video_vision_config()
    cfg = _make_vision_cfg(raw_cfg)
    result_items: List[Dict[str, Any]] = []
    if not cfg.enabled:
        return {
            "enabled": False,
            "ok": False,
            "error": "vision_ai.enabled=false",
            "items": result_items,
        }
    client = VisionAIClient(cfg)
    try:
        for path in image_paths:
            t0 = time.perf_counter()
            item = await client.validate_image(
                image_path=str(path),
                prompt=VISION_PROMPT,
                skip_duplicate_check=True,
            )
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            result_items.append(
                {
                    "image": str(path),
                    "elapsed_ms": round(elapsed_ms, 2),
                    "result": item,
                    "ok": "error" not in item,
                }
            )
        return {
            "enabled": True,
            "ok": any(x["ok"] for x in result_items),
            "items": result_items,
            "stats": client.get_stats(),
        }
    finally:
        await client.close()


def _extract_su_id(filename: str) -> str:
    m = re.search(r"(SU\d+)", filename, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper()
    return "SU000"


async def run_vl_test(clips: List[Path]) -> Dict[str, Any]:
    m2_cfg = load_module2_config()
    vl_cfg = m2_cfg.get("vl_material_generation", {})
    analyzer = VLVideoAnalyzer(vl_cfg)
    items: List[Dict[str, Any]] = []
    try:
        for clip in clips:
            t0 = time.perf_counter()
            resp = await analyzer.analyze_clip(
                clip_path=str(clip),
                semantic_unit_start_sec=0.0,
                semantic_unit_id=_extract_su_id(clip.name),
                extra_prompt=VL_EXTRA_PROMPT,
                analysis_mode="default",
            )
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            items.append(
                {
                    "clip": str(clip),
                    "elapsed_ms": round(elapsed_ms, 2),
                    "success": bool(resp.success),
                    "error_msg": resp.error_msg,
                    "analysis_count": len(resp.analysis_results or []),
                    "clip_request_count": len(resp.clip_requests or []),
                    "screenshot_request_count": len(resp.screenshot_requests or []),
                    "token_usage": resp.token_usage,
                    "raw_response_json": resp.raw_response_json[:2] if resp.raw_response_json else [],
                }
            )
        return {
            "ok": any(x["success"] for x in items),
            "items": items,
        }
    finally:
        await analyzer.close()


def _build_minicpm_query(image_path: Path) -> Dict[str, str]:
    with image_path.open("rb") as f:
        image_b64 = base64.b64encode(f.read()).decode("utf-8")
    question = json.dumps(
        [
            {
                "role": "user",
                "content": [
                    {"type": "text", "pairs": VISION_PROMPT},
                    {"type": "image", "pairs": image_b64},
                ],
            }
        ],
        ensure_ascii=False,
    )
    params = json.dumps(
        {
            "max_new_tokens": 512,
            "temperature": 0.1,
            "enable_thinking": False,
            "stream": False,
        },
        ensure_ascii=False,
    )
    return {
        "image": "",
        "question": question,
        "params": params,
        "temporal_ids": None,
    }


def _extract_video_frames_b64(video_path: Path, max_frames: int = 8) -> List[str]:
    import cv2  # noqa: WPS433

    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        return []

    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    if frame_count <= 0:
        cap.release()
        return []

    picks: List[int] = []
    if frame_count <= max_frames:
        picks = list(range(frame_count))
    else:
        gap = frame_count / max_frames
        picks = [min(frame_count - 1, int(i * gap + gap / 2)) for i in range(max_frames)]

    out: List[str] = []
    for idx in picks:
        cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
        ok, frame = cap.read()
        if not ok or frame is None:
            continue
        ok2, enc = cv2.imencode(".jpg", frame)
        if not ok2:
            continue
        out.append(base64.b64encode(enc.tobytes()).decode("utf-8"))

    cap.release()
    return out


def _build_minicpm_video_query(video_path: Path) -> Dict[str, Any]:
    frame_b64_list = _extract_video_frames_b64(video_path, max_frames=8)
    content: List[Dict[str, str]] = [
        {"type": "text", "pairs": "请按 JSON 输出：[{id, knowledge_type, confidence, clip_start_sec, clip_end_sec, suggested_screenshoot_timestamps}]，不要解释文本。"}
    ]
    for frame_b64 in frame_b64_list:
        content.append({"type": "image", "pairs": frame_b64})

    msgs = [{"role": "user", "content": content}]
    params = {
        "max_new_tokens": 1024,
        "temperature": 0.1,
        "enable_thinking": False,
        "stream": False,
    }
    return {
        "image": "",
        "question": json.dumps(msgs, ensure_ascii=True),
        "params": json.dumps(params, ensure_ascii=True),
    }


def _normalize_server_url(raw_url: str) -> str:
    url = (raw_url or "").strip() or "http://127.0.0.1:9999/api"
    if not url.endswith("/api"):
        url = url.rstrip("/")
    return url


def run_minicpm_test(image_paths: List[Path]) -> Dict[str, Any]:
    server_url = _normalize_server_url(os.getenv("MINICPM_SERVER_URL", "http://127.0.0.1:9999/api"))
    health_url = server_url[:-4] if server_url.endswith("/api") else server_url
    detail: Dict[str, Any] = {
        "ok": False,
        "items": [],
        "blocked_reasons": [],
        "server_url": server_url,
        "health_url": health_url,
        "mode": "cookbook_gradio_server_api",
    }

    try:
        with httpx.Client(timeout=httpx.Timeout(10.0, connect=5.0)) as cli:
            try:
                hr = cli.get(health_url)
                detail["health_status_code"] = int(hr.status_code)
            except Exception as exc:
                detail["blocked_reasons"].append(f"MiniCPM 服务不可达: {type(exc).__name__}: {exc}")
                return detail

            if detail["health_status_code"] >= 500:
                detail["blocked_reasons"].append(f"MiniCPM 服务异常: HTTP {detail['health_status_code']}")
                return detail

            for path in image_paths:
                payload = _build_minicpm_query(path)
                t0 = time.perf_counter()
                resp = cli.post(server_url, json=payload)
                elapsed_ms = (time.perf_counter() - t0) * 1000.0

                if resp.status_code != 200:
                    detail["items"].append(
                        {
                            "image": str(path),
                            "elapsed_ms": round(elapsed_ms, 2),
                            "ok": False,
                            "error": f"http_{resp.status_code}",
                            "body_preview": resp.text[:300],
                        }
                    )
                    continue

                data = resp.json()
                body = data.get("data", {}) if isinstance(data, dict) else {}
                result_text = body.get("result", "")
                usage = body.get("usage", {})
                detail["items"].append(
                    {
                        "image": str(path),
                        "elapsed_ms": round(elapsed_ms, 2),
                        "output_tokens": int(usage.get("output_tokens", 0)),
                        "output_preview": str(result_text)[:600],
                        "ok": True,
                    }
                )

        detail["ok"] = any(x.get("ok") for x in detail["items"])
        return detail
    except Exception as exc:
        detail["blocked_reasons"].append(f"运行异常: {type(exc).__name__}: {exc}")
        return detail


def run_minicpm_video_test(clips: List[Path]) -> Dict[str, Any]:
    server_url = _normalize_server_url(os.getenv("MINICPM_SERVER_URL", "http://127.0.0.1:9999/api"))
    health_url = server_url[:-4] if server_url.endswith("/api") else server_url
    detail: Dict[str, Any] = {
        "ok": False,
        "items": [],
        "blocked_reasons": [],
        "server_url": server_url,
        "health_url": health_url,
        "mode": "cookbook_gradio_server_api_video_frames",
    }
    try:
        with httpx.Client(timeout=httpx.Timeout(120.0, connect=5.0)) as cli:
            try:
                hr = cli.get(health_url)
                detail["health_status_code"] = int(hr.status_code)
            except Exception as exc:
                detail["blocked_reasons"].append(f"MiniCPM 服务不可达: {type(exc).__name__}: {exc}")
                return detail
            if detail["health_status_code"] >= 500:
                detail["blocked_reasons"].append(f"MiniCPM 服务异常: HTTP {detail['health_status_code']}")
                return detail

            for clip in clips:
                payload = _build_minicpm_video_query(clip)
                if not payload:
                    detail["items"].append(
                        {"clip": str(clip), "ok": False, "error": "payload_empty"}
                    )
                    continue
                t0 = time.perf_counter()
                resp = cli.post(server_url, json=payload)
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                if resp.status_code != 200:
                    detail["items"].append(
                        {
                            "clip": str(clip),
                            "elapsed_ms": round(elapsed_ms, 2),
                            "ok": False,
                            "error": f"http_{resp.status_code}",
                            "body_preview": resp.text[:300],
                        }
                    )
                    continue
                data = resp.json()
                body = data.get("data", {}) if isinstance(data, dict) else {}
                result_text = body.get("result", "")
                usage = body.get("usage", {})
                detail["items"].append(
                    {
                        "clip": str(clip),
                        "elapsed_ms": round(elapsed_ms, 2),
                        "output_tokens": int(usage.get("output_tokens", 0)),
                        "output_preview": str(result_text)[:800],
                        "ok": True,
                    }
                )

        detail["ok"] = any(x.get("ok") for x in detail["items"])
        return detail
    except Exception as exc:
        detail["blocked_reasons"].append(f"运行异常: {type(exc).__name__}: {exc}")
        return detail


def run_minicpm_test_legacy(image_paths: List[Path]) -> Dict[str, Any]:
    # 兼容保留：旧的 direct-import 模式（默认不使用）
    server_dir = PROJECT_ROOT / "third_party" / "minicpm" / "MiniCPM-V-CookBook" / "demo" / "web_demo" / "gradio" / "server"
    if str(server_dir) not in sys.path:
        sys.path.insert(0, str(server_dir))
    detail: Dict[str, Any] = {
        "ok": False,
        "items": [],
        "blocked_reasons": [],
        "server_model_code_path": str(server_dir / "models"),
        "mode": "legacy_direct_import",
    }
    try:
        import importlib.util
        if importlib.util.find_spec("accelerate") is None:
            detail["blocked_reasons"].append("缺少 Python 依赖 accelerate")
        model_path = os.getenv("MINICPM_MODEL_PATH", "").strip()
        if not model_path:
            detail["blocked_reasons"].append("未设置 MINICPM_MODEL_PATH（本地模型权重路径）")
        elif not Path(model_path).exists():
            detail["blocked_reasons"].append(f"MINICPM_MODEL_PATH 不存在: {model_path}")
        import torch  # noqa: WPS433
        if not torch.cuda.is_available():
            detail["blocked_reasons"].append("当前环境无 CUDA GPU，可运行性和速度风险极高")
        if detail["blocked_reasons"]:
            return detail
        from models.minicpmv4_5 import ModelMiniCPMV4_5  # type: ignore  # noqa: E402

        t_load = time.perf_counter()
        model = ModelMiniCPMV4_5(model_path)
        detail["model_load_ms"] = round((time.perf_counter() - t_load) * 1000.0, 2)

        for path in image_paths:
            payload = _build_minicpm_query(path)
            t0 = time.perf_counter()
            text, out_tokens = model(payload)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            detail["items"].append(
                {
                    "image": str(path),
                    "elapsed_ms": round(elapsed_ms, 2),
                    "output_tokens": int(out_tokens),
                    "output_preview": str(text)[:600],
                    "ok": True,
                }
            )
        detail["ok"] = any(x.get("ok") for x in detail["items"])
        return detail
    except Exception as exc:
        detail["blocked_reasons"].append(f"运行异常: {type(exc).__name__}: {exc}")
        return detail


def summarize(ver: Dict[str, Any], vl: Dict[str, Any], mc: Dict[str, Any]) -> Dict[str, Any]:
    vision_ok = sum(1 for x in ver.get("items", []) if x.get("ok"))
    vision_total = len(ver.get("items", []))
    vl_ok = sum(1 for x in vl.get("items", []) if x.get("success"))
    vl_total = len(vl.get("items", []))
    return {
        "vision_pass_rate": f"{vision_ok}/{vision_total}",
        "vl_pass_rate": f"{vl_ok}/{vl_total}",
        "minicpm_runnable": bool(mc.get("ok")),
        "minicpm_blocked_reasons": mc.get("blocked_reasons", []),
        "replacement_feasible_now": bool(vision_ok == vision_total and vl_ok == vl_total and mc.get("ok")),
    }


async def main() -> None:
    storage_root = Path(r"d:\videoToMarkdownTest2\var\storage\storage")
    dataset_root = _find_dataset_root(storage_root)
    picked = _pick_files(dataset_root)

    print(f"[{_now()}] 数据集: {dataset_root}")
    print(f"[{_now()}] 截图样本数: {len(picked['screenshots'])}, 视频样本数: {len(picked['clips'])}")

    vision_result = await run_vision_test(picked["screenshots"])
    vl_result = await run_vl_test(picked["clips"])
    minicpm_result = run_minicpm_test(picked["screenshots"])
    minicpm_video_result = run_minicpm_video_test(picked["clips"])

    final = {
        "time": _now(),
        "dataset_root": str(dataset_root),
        "samples": {
            "screenshots": [str(p) for p in picked["screenshots"]],
            "clips": [str(p) for p in picked["clips"]],
        },
        "vision": vision_result,
        "vl": vl_result,
        "minicpm_v4_5": minicpm_result,
        "minicpm_v4_5_video": minicpm_video_result,
    }
    final["summary"] = summarize(vision_result, vl_result, minicpm_result)

    out_dir = PROJECT_ROOT / "var" / "artifacts" / "benchmarks" / "minicpm_feasibility"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    out_path = out_dir / f"compare_{ts}.json"
    out_path.write_text(json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[{_now()}] 测试完成，结果文件: {out_path}")
    print(json.dumps(final["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
