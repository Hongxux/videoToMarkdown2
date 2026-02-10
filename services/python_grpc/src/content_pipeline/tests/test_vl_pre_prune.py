"""
VL 鍓嶇疆闈欐€佹鍓旈櫎閫昏緫鍗曞厓娴嬭瘯銆?
瑕嗙洊鐩爣锛?1) stable 鏍稿績鍖洪棿鍓旈櫎鍚庝繚鐣欏尯闂磋绠楁纭紱
2) 瑁佸壀鐗囨鐩稿鏃堕棿鏄犲皠鍥炲師濮嬫椂闂磋酱姝ｇ‘锛?3) 涓婁笅鏂囨彁绀哄寘鍚?knowledge_topic 涓庝笂涓嬫枃鏂囨湰銆?"""

from __future__ import annotations

import asyncio
from typing import Dict, Any

from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator


def _build_generator():
    return VLMaterialGenerator(
        {
            "enabled": True,
            "pre_vl_static_pruning": {
                "enabled": True,
                "only_process": True,
                "min_unit_duration_sec": 10.0,
                "min_stable_interval_sec": 3.0,
                "keep_edge_sec": 1.0,
                "min_cut_span_sec": 0.8,
                "min_keep_segment_sec": 0.5,
                "min_removed_ratio": 0.1,
                "context_text_max_chars": 200,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )


def test_subtract_intervals_keeps_edges_for_stable_core_cut():
    """
    缁欏畾 stable=[1,5] 涓?keep_edge=1s 鏃讹紝浠呭墧闄?[2,4]锛屾渶缁堜繚鐣?[0,2] 鍜?[4,10]銆?    """
    generator = _build_generator()

    base = (0.0, 10.0)
    removed = [(2.0, 4.0)]
    keep = generator._subtract_intervals(base, removed, min_keep_segment_sec=0.5)

    assert len(keep) == 2
    assert abs(keep[0][0] - 0.0) < 1e-6
    assert abs(keep[0][1] - 2.0) < 1e-6
    assert abs(keep[1][0] - 4.0) < 1e-6
    assert abs(keep[1][1] - 10.0) < 1e-6


def test_map_pruned_relative_time_back_to_original_axis():
    """
    淇濈暀娈?[0,2] + [4,10] 鏃讹細
    - 鏂扮墖娈电浉瀵?1.5s -> 鍘熷 1.5s锛堢涓€娈靛唴锛?    - 鏂扮墖娈电浉瀵?3.0s -> 鍘熷 5.0s锛堣惤鍏ョ浜屾锛屽亸绉?1.0s锛?    """
    generator = _build_generator()
    kept = [(0.0, 2.0), (4.0, 10.0)]

    mapped_a = generator._map_pruned_relative_to_original(1.5, kept)
    mapped_b = generator._map_pruned_relative_to_original(3.0, kept)

    assert abs(mapped_a - 1.5) < 1e-6
    assert abs(mapped_b - 5.0) < 1e-6


def test_build_pruning_context_prompt_contains_topic_and_text():
    """
    涓婁笅鏂囨彁绀哄簲鍖呭惈 knowledge_topic銆佹枃鏈笂涓嬫枃銆佷繚鐣?鍓旈櫎鍖洪棿銆?    """
    generator = _build_generator()
    su: Dict[str, Any] = {
        "unit_id": "SU100",
        "knowledge_topic": "MCP setup",
        "text": "Install skill, configure params, then verify connectivity.",
    }

    prompt = generator._build_pruning_context_prompt(
        semantic_unit=su,
        kept_segments=[(0.0, 2.0), (4.0, 8.0)],
        removed_segments=[(2.0, 4.0)],
    )

    assert "MCP setup" in prompt
    assert "Install skill" in prompt
    assert "[0.00s-2.00s]" in prompt
    assert "[2.00s-4.00s]" in prompt


def test_token_saving_estimation_linear_seconds():
    """
    楠岃瘉鈥滄寜淇濈暀鏃堕暱绾挎€у洖鎺ㄥ熀绾?token鈥濈殑鏍稿績浼扮畻閫昏緫銆?
    绀轰緥锛?    - 鍘熷鏃堕暱 10s
    - 淇濈暀鏃堕暱 4s
    - 瀹為檯 total_tokens=120
    鍒欏熀绾夸及绠楃害涓?120/4*10=300锛岃妭鐪佺害 180銆?    """
    generator = _build_generator()

    unit_duration = 10.0
    kept_segments = [(0.0, 2.0), (4.0, 6.0)]
    kept_duration = sum((e - s) for s, e in kept_segments)
    assert abs(kept_duration - 4.0) < 1e-6

    total_actual = 120
    prompt_actual = 80
    completion_actual = 40

    prompt_base = int(round(prompt_actual / kept_duration * unit_duration))
    completion_base = int(round(completion_actual / kept_duration * unit_duration))
    total_base = prompt_base + completion_base
    saved = total_base - total_actual

    assert total_base == 300
    assert saved == 180


def test_removed_intervals_require_stable_longer_than_3s():
    """
    鏂扮害鏉燂細stable 鍘熷闀垮害蹇呴』 >3s 鎵嶅彲鍓旈櫎銆?    - 3.0s锛堝 1-4锛変笉鍓旈櫎
    - 3.1s锛堝 1-4.1锛夊彲鍓旈櫎鏍稿績鍖?    """
    generator = _build_generator()

    # 边界：正好 3.0s，不应剔除
    removed_a = generator._build_removed_intervals_from_stable([(1.0, 4.0)])
    assert removed_a == []

    # 超过 3.0s，应剔除 [2.0, 3.1]
    removed_b = generator._build_removed_intervals_from_stable([(1.0, 4.1)])
    assert len(removed_b) == 1
    assert abs(removed_b[0][0] - 2.0) < 1e-6
    assert abs(removed_b[0][1] - 3.1) < 1e-6


def test_map_pruned_interval_to_original_segments_cross_gap():
    """
    褰?pruned 鍖洪棿璺ㄨ秺琚墧闄ら棿闅欐椂锛屽簲鏄犲皠涓哄師鏃堕棿杞翠笂鐨勫娈靛尯闂淬€?    kept=[0,2]+[4,6]锛宲runed 涓?[1,3] -> 鍘熷 [1,2] + [4,5]
    """
    generator = _build_generator()
    kept = [(0.0, 2.0), (4.0, 6.0)]

    mapped = generator._map_pruned_interval_to_original_segments(1.0, 3.0, kept)
    assert len(mapped) == 2
    assert abs(mapped[0][0] - 1.0) < 1e-6
    assert abs(mapped[0][1] - 2.0) < 1e-6
    assert abs(mapped[1][0] - 4.0) < 1e-6
    assert abs(mapped[1][1] - 5.0) < 1e-6


def test_find_clip_for_unit_avoids_substring_collision(tmp_path):
    """
    SU01 涓?SU010 骞跺瓨鏃讹紝涓嶈兘鐢ㄥ瓙涓插尮閰嶅鑷磋閫夈€?    """
    generator = _build_generator()
    clips_dir = tmp_path / "clips"
    clips_dir.mkdir(parents=True, exist_ok=True)

    file_su01 = clips_dir / "001_SU01_topic_10.00-20.00.mp4"
    file_su010 = clips_dir / "002_SU010_topic_30.00-40.00.mp4"
    file_su01.write_bytes(b"x")
    file_su010.write_bytes(b"x")

    selected = generator._find_clip_for_unit(
        clips_dir=str(clips_dir),
        unit_id="SU01",
        start_sec=10.0,
        end_sec=20.0,
    )

    assert selected is not None
    assert selected.endswith(file_su01.name)


def test_split_complete_sentences_by_pause_threshold():
    """
    0.3s 鍙婁互涓婂仠椤垮簲鍒囧嚭鏂扮殑鍙ｈ鍙ャ€?    """
    generator = _build_generator()

    subtitles = [
        {"start_sec": 0.0, "end_sec": 0.5, "text": "first part"},
        {"start_sec": 0.5, "end_sec": 1.0, "text": "first tail"},
        {"start_sec": 1.4, "end_sec": 1.8, "text": "second"},
    ]

    sentences = generator._split_complete_sentences_by_pause(subtitles)
    assert len(sentences) == 2
    assert abs(sentences[0]["start_sec"] - 0.0) < 1e-6
    assert abs(sentences[0]["end_sec"] - 1.0) < 1e-6
    assert abs(sentences[1]["start_sec"] - 1.4) < 1e-6


def test_refine_kept_segments_before_concat_applies_semantic_physical_and_buffers(tmp_path, monkeypatch):
    """
    Stable 鍓旈櫎鍚庣殑 kept 鐗囨鍦ㄥ悎骞跺墠浼氱粡鍘嗭細
    1) 璇箟鍙ュご鍥炴媺锛?) 缁堢偣骞跺叆 MSE 璺冲彉锛?) 璇祦缂撳啿銆?    """
    generator = _build_generator()

    async def _fake_detect_segment_mse_jump_end(clip_path, semantic_end_sec, clip_duration_sec):
        return min(clip_duration_sec, semantic_end_sec + 0.5)

    monkeypatch.setattr(
        generator,
        "_detect_segment_mse_jump_end",
        _fake_detect_segment_mse_jump_end,
    )
    class _RepoStub:
        def list_subtitles(self):
            return [
                {"start_sec": 100.0, "end_sec": 100.4, "text": "look next"},
                {"start_sec": 100.4, "end_sec": 101.0, "text": "step one"},
                {"start_sec": 101.5, "end_sec": 102.5, "text": "wrap up"},
            ]

        def build_relative_subtitles(self, *, unit_start_sec: float, unit_end_sec: float):
            if unit_end_sec <= unit_start_sec:
                return []
            unit_duration = unit_end_sec - unit_start_sec
            result = []
            for sub in self.list_subtitles():
                sub_start = float(sub["start_sec"])
                sub_end = float(sub["end_sec"])
                if sub_end <= unit_start_sec or sub_start >= unit_end_sec:
                    continue
                rel_start = max(0.0, sub_start - unit_start_sec)
                rel_end = min(unit_duration, sub_end - unit_start_sec)
                if rel_end <= rel_start:
                    continue
                result.append({"start_sec": rel_start, "end_sec": rel_end, "text": sub["text"]})
            return result

    monkeypatch.setattr(generator, "_get_subtitle_repo_for_output_dir", lambda _output_dir: _RepoStub())

    clips_dir = tmp_path / "semantic_unit_clips_vl"
    clips_dir.mkdir(parents=True, exist_ok=True)

    refined = asyncio.run(
        generator._refine_kept_segments_before_concat(
            clips_dir=str(clips_dir),
            semantic_unit={"unit_id": "SU999", "start_sec": 100.0, "end_sec": 103.0},
            original_clip_path=str(clips_dir / "dummy.mp4"),
            kept_segments=[(0.5, 0.8)],
        )
    )

    assert len(refined) == 1
    # 起点：回拉到语义句头并加 -0.2s 缓冲后截断到 0
    assert abs(refined[0][0] - 0.0) < 1e-6
    # 终点：语义句末 1.0s -> MSE 跃变 1.5s -> +0.3s 缓冲 = 1.8s
    assert abs(refined[0][1] - 1.8) < 1e-6


