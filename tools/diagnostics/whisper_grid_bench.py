import argparse
import copy
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from services.python_grpc.src.media_engine.knowledge_engine.core.parallel_transcription import (  # noqa: E402
    get_video_duration,
    transcribe_parallel,
)


def parse_int_list(text: str) -> list[int]:
    values = []
    for token in (text or "").split(","):
        token = token.strip()
        if not token:
            continue
        values.append(int(token))
    if not values:
        raise ValueError("empty integer list")
    return values


def extract_clip(src_video: str, clip_seconds: int) -> str:
    fd, clip_path = tempfile.mkstemp(prefix="whisper_bench_clip_", suffix=".mp4")
    os.close(fd)
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        "0",
        "-i",
        src_video,
        "-t",
        str(max(1, int(clip_seconds))),
        "-c",
        "copy",
        clip_path,
    ]
    subprocess.run(cmd, check=True)
    if (not os.path.exists(clip_path)) or os.path.getsize(clip_path) <= 0:
        raise RuntimeError(f"clip extract failed: {clip_path}")
    return clip_path


def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"invalid config: {config_path}")
    return data


def with_overrides(
    base_config: dict,
    workers: int,
    segment_duration: int,
    split_threshold: int,
    min_segment_duration: int,
    beam_size: int,
    vad_filter: bool,
    probe_sec: int,
) -> dict:
    cfg = copy.deepcopy(base_config)
    whisper_cfg = cfg.setdefault("whisper", {})
    parallel_cfg = whisper_cfg.setdefault("parallel", {})
    parallel_cfg["num_workers"] = max(1, int(workers))
    parallel_cfg["segment_duration"] = max(1, int(segment_duration))
    parallel_cfg["segment_split_threshold_sec"] = max(1, int(split_threshold))
    parallel_cfg["min_segment_duration_sec"] = max(1, int(min_segment_duration))
    whisper_cfg["beam_size"] = max(1, int(beam_size))
    whisper_cfg["vad_filter"] = bool(vad_filter)
    whisper_cfg["language_detect_probe_sec"] = max(30, int(probe_sec))
    return cfg


def main() -> int:
    parser = argparse.ArgumentParser(description="Whisper parallel grid benchmark")
    parser.add_argument("--video", required=True, help="source video path")
    parser.add_argument("--config", default="config/video_config.yaml", help="config yaml")
    parser.add_argument("--clip-seconds", type=int, default=60, help="clip first N seconds")
    parser.add_argument("--workers", default="2,3,4", help="comma list, e.g. 2,3,4")
    parser.add_argument("--segment-durations", default="20,30,45", help="comma list, e.g. 20,30,45")
    parser.add_argument("--split-threshold", type=int, default=30, help="segment split threshold sec")
    parser.add_argument("--min-segment-duration", type=int, default=10, help="min segment duration sec")
    parser.add_argument("--model-size", default="base", help="whisper model size")
    parser.add_argument("--device", default="cpu", help="cpu/cuda")
    parser.add_argument("--compute-type", default="int8", help="compute type")
    parser.add_argument("--language", default="auto", help="language or auto")
    parser.add_argument("--beam-size", type=int, default=4, help="beam size")
    parser.add_argument("--vad-filter", action="store_true", help="enable vad filter")
    parser.add_argument("--probe-sec", type=int, default=120, help="language detect probe length")
    args = parser.parse_args()

    source_video = os.path.abspath(args.video)
    config_path = os.path.abspath(args.config)
    if not os.path.exists(source_video):
        raise FileNotFoundError(source_video)
    if not os.path.exists(config_path):
        raise FileNotFoundError(config_path)

    worker_grid = parse_int_list(args.workers)
    seg_grid = parse_int_list(args.segment_durations)
    base_config = load_config(config_path)

    bench_video = source_video
    cleanup_clip = None
    if args.clip_seconds > 0:
        bench_video = extract_clip(source_video, args.clip_seconds)
        cleanup_clip = bench_video

    try:
        video_duration = max(0.1, float(get_video_duration(bench_video)))
        print(f"[bench] video={bench_video}")
        print(f"[bench] duration={video_duration:.2f}s, workers={worker_grid}, segment_duration={seg_grid}")
        print(
            f"[bench] fixed params: model={args.model_size}, device={args.device}, compute={args.compute_type}, "
            f"language={args.language}, beam={args.beam_size}, vad={args.vad_filter}, probe_sec={args.probe_sec}"
        )

        results = []
        for workers in worker_grid:
            for seg_dur in seg_grid:
                cfg = with_overrides(
                    base_config=base_config,
                    workers=workers,
                    segment_duration=seg_dur,
                    split_threshold=args.split_threshold,
                    min_segment_duration=args.min_segment_duration,
                    beam_size=args.beam_size,
                    vad_filter=args.vad_filter,
                    probe_sec=args.probe_sec,
                )
                label = f"workers={workers}, seg={seg_dur}s"
                print(f"\n[bench] run {label}")
                started = time.perf_counter()
                error = None
                text_len = 0
                try:
                    subtitle_text = transcribe_parallel(
                        video_path=bench_video,
                        model_size=args.model_size,
                        device=args.device,
                        compute_type=args.compute_type,
                        language=args.language,
                        segment_duration=seg_dur,
                        num_workers=workers,
                        config=cfg,
                    )
                    text_len = len(subtitle_text or "")
                except Exception as exc:
                    error = str(exc)
                elapsed = time.perf_counter() - started
                rtf = elapsed / video_duration
                results.append(
                    {
                        "workers": workers,
                        "segment_duration": seg_dur,
                        "elapsed": elapsed,
                        "rtf": rtf,
                        "chars": text_len,
                        "error": error,
                    }
                )
                if error:
                    print(f"[bench] FAIL {label}: {error} (elapsed={elapsed:.2f}s, rtf={rtf:.3f})")
                else:
                    print(f"[bench] OK   {label}: elapsed={elapsed:.2f}s, rtf={rtf:.3f}, chars={text_len}")

        ok_results = [r for r in results if not r["error"]]
        ok_results.sort(key=lambda item: item["elapsed"])
        print("\n=== GRID RESULT (sorted by elapsed) ===")
        for idx, item in enumerate(ok_results, 1):
            print(
                f"{idx:02d}. workers={item['workers']}, seg={item['segment_duration']}s, "
                f"elapsed={item['elapsed']:.2f}s, rtf={item['rtf']:.3f}, chars={item['chars']}"
            )
        fail_results = [r for r in results if r["error"]]
        if fail_results:
            print("\n=== FAILURES ===")
            for item in fail_results:
                print(
                    f"- workers={item['workers']}, seg={item['segment_duration']}s, "
                    f"elapsed={item['elapsed']:.2f}s, error={item['error']}"
                )
        return 0 if ok_results else 2
    finally:
        if cleanup_clip and os.path.exists(cleanup_clip):
            os.remove(cleanup_clip)


if __name__ == "__main__":
    raise SystemExit(main())
