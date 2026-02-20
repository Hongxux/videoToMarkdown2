from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict


def _load_recommendation(path: Path) -> Dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("recommendation.json 必须是对象")
    return payload


def _resolve_recommendation_path(args: argparse.Namespace) -> Path:
    if args.recommendation_json:
        path = Path(args.recommendation_json).resolve()
        if not path.exists():
            raise FileNotFoundError(f"recommendation.json 不存在: {path}")
        return path

    if not args.bench_output_dir:
        raise ValueError("需要提供 --recommendation-json 或 --bench-output-dir")
    candidate = Path(args.bench_output_dir).resolve() / "raw" / "recommendation.json"
    if not candidate.exists():
        raise FileNotFoundError(f"未找到 recommendation.json: {candidate}")
    return candidate


def _build_env_map(recommendation: Dict[str, object], structure_mode: str) -> Dict[str, str]:
    try:
        structure_workers = int(recommendation.get("structure_workers", 0) or 0)
        ocr_workers = int(recommendation.get("ocr_workers", 0) or 0)
    except Exception as exc:
        raise ValueError(f"recommendation 并发字段无效: {exc}") from exc

    if structure_workers <= 0 or ocr_workers <= 0:
        raise ValueError("recommendation 中 structure_workers / ocr_workers 必须大于 0")

    return {
        "PHASE2B_STRUCTURE_PREPROCESS_MODE": str(structure_mode or "process").strip() or "process",
        "PHASE2B_STRUCTURE_PREPROCESS_WORKERS": str(structure_workers),
        "PHASE2B_OCR_VALIDATE_WORKERS": str(ocr_workers),
    }


def _write_dotenv(path: Path, env_map: Dict[str, str]) -> None:
    lines = [f"{key}={value}" for key, value in env_map.items()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_powershell(path: Path, env_map: Dict[str, str]) -> None:
    lines = []
    lines.append("param([switch]$Persist)")
    lines.append("")
    for key, value in env_map.items():
        lines.append(f"$env:{key} = '{value}'")
    lines.append("")
    lines.append("if ($Persist) {")
    for key, value in env_map.items():
        lines.append(f"    setx {key} '{value}' | Out-Null")
    lines.append("    Write-Host '已使用 setx 持久化到用户环境变量。'")
    lines.append("}")
    lines.append("Write-Host 'Phase2B 并发环境变量已应用到当前 PowerShell 会话。'")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_cmd(path: Path, env_map: Dict[str, str]) -> None:
    lines = ["@echo off"]
    for key, value in env_map.items():
        lines.append(f"set {key}={value}")
    lines.append("echo Phase2B 并发环境变量已应用到当前 CMD 会话。")
    lines.append("echo 如需持久化，请执行: apply_phase2b_concurrency_recommendation.py --persist-user-env")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _persist_user_env(env_map: Dict[str, str]) -> None:
    if os.name != "nt":
        raise RuntimeError("--persist-user-env 仅支持 Windows")
    for key, value in env_map.items():
        subprocess.run(
            ["setx", key, value],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="将 Phase2B 并发压测推荐结果应用到运行配置")
    parser.add_argument("--recommendation-json", default="", help="raw/recommendation.json 路径")
    parser.add_argument("--bench-output-dir", default="", help="压测输出目录（自动读取 raw/recommendation.json）")
    parser.add_argument("--structure-mode", default="process", help="PHASE2B_STRUCTURE_PREPROCESS_MODE 值")
    parser.add_argument(
        "--output-dir",
        default="",
        help="落地脚本目录，默认使用 recommendation 所在压测目录",
    )
    parser.add_argument(
        "--persist-user-env",
        action="store_true",
        help="立即调用 setx 持久化到用户环境变量（Windows）",
    )
    args = parser.parse_args()

    recommendation_path = _resolve_recommendation_path(args)
    recommendation = _load_recommendation(recommendation_path)
    env_map = _build_env_map(recommendation, args.structure_mode)

    if args.output_dir:
        output_dir = Path(args.output_dir).resolve()
    else:
        output_dir = recommendation_path.parent.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    dotenv_path = output_dir / "phase2b_concurrency.env"
    ps1_path = output_dir / "apply_phase2b_concurrency.ps1"
    cmd_path = output_dir / "apply_phase2b_concurrency.cmd"
    _write_dotenv(dotenv_path, env_map)
    _write_powershell(ps1_path, env_map)
    _write_cmd(cmd_path, env_map)

    if args.persist_user_env:
        _persist_user_env(env_map)

    print("=== Phase2B 推荐并发已落地 ===")
    print(f"recommendation: {recommendation_path}")
    print(f"dotenv: {dotenv_path}")
    print(f"powershell: {ps1_path}")
    print(f"cmd: {cmd_path}")
    print("")
    print("PowerShell 当前会话应用:")
    print(f"  . '{ps1_path}'")
    print("PowerShell 持久化应用:")
    print(f"  . '{ps1_path}' -Persist")
    print("CMD 当前会话应用:")
    print(f"  call \"{cmd_path}\"")
    if args.persist_user_env:
        print("已执行 setx 持久化。新开终端后生效。")
    else:
        print("如需立即持久化，可追加参数: --persist-user-env")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise
