"""
模块说明：测试用例与断言集合。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import json
import pytest
import tempfile
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from module2_content_enhancement.data_loader import (
    load_corrected_subtitles,
    load_merged_segments,
    create_module2_input,
    validate_input_consistency
)


# Sample test data matching existing implementation format

SAMPLE_CORRECTED_SUBTITLES = {
    "corrected_subtitles": [
        {
            "subtitle_id": "SUB001",
            "corrected_text": "今天我们来讲解快速排序算法",
            "start_sec": 0.0,
            "end_sec": 2.5,
            "corrections": []
        },
        {
            "subtitle_id": "SUB002",
            "corrected_text": "快速排序的核心是分治思想",
            "start_sec": 2.5,
            "end_sec": 5.0,
            "corrections": [
                {"original": "核型", "corrected": "核心", "reason": "同音字"}
            ]
        },
        {
            "subtitle_id": "SUB003",
            "corrected_text": "首先选择一个基准值",
            "start_sec": 5.0,
            "end_sec": 7.5,
            "corrections": []
        }
    ]
}

SAMPLE_MERGED_SEGMENTS = {
    "pure_text_script": [
        {
            "paragraph_id": "P001",
            "text": "今天我们来讲解快速排序算法,快速排序的核心是分治思想",
            "source_sentence_ids": ["S001", "S002"],
            "merge_type": "同义转述"
        },
        {
            "paragraph_id": "P002",
            "text": "首先选择一个基准值",
            "source_sentence_ids": ["S003"],
            "merge_type": "无合并"
        }
    ]
}


class TestLoadCorrectedSubtitles:
    """
    类说明：封装 TestLoadCorrectedSubtitles 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    
    def test_load_valid_json(self, tmp_path):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - tmp_path: 文件路径（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        json_file = tmp_path / "corrected_subtitles.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(SAMPLE_CORRECTED_SUBTITLES, f)
        
        subtitles = load_corrected_subtitles(str(json_file))
        
        assert len(subtitles) == 3
        assert subtitles[0].subtitle_id == "SUB001"
        assert subtitles[0].text == "今天我们来讲解快速排序算法"
        assert subtitles[0].start_sec == 0.0
        assert subtitles[0].end_sec == 2.5
        
        assert len(subtitles[1].corrections) == 1
        assert subtitles[1].corrections[0]["corrected"] == "核心"
    
    def test_load_missing_file(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        with pytest.raises(FileNotFoundError):
            load_corrected_subtitles("/nonexistent/file.json")
    
    def test_load_invalid_json(self, tmp_path):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - tmp_path: 文件路径（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        json_file = tmp_path / "invalid.json"
        with open(json_file, 'w') as f:
            f.write("not valid json{")
        
        with pytest.raises(ValueError, match="Invalid JSON"):
            load_corrected_subtitles(str(json_file))
    
    def test_load_missing_required_field(self, tmp_path):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - tmp_path: 文件路径（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        invalid_data = {
            "corrected_subtitles": [
                {
                    "subtitle_id": "SUB001",
                    # Missing start_sec, end_sec
                }
            ]
        }
        
        json_file = tmp_path / "missing_fields.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(invalid_data, f)
        
        with pytest.raises(ValueError, match="Missing required fields"):
            load_corrected_subtitles(str(json_file))
    
    def test_load_negative_timestamp(self, tmp_path):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - tmp_path: 文件路径（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        invalid_data = {
            "corrected_subtitles": [
                {
                    "subtitle_id": "SUB001",
                    "corrected_text": "test",
                    "start_sec": -1.0,  # Invalid
                    "end_sec": 2.0
                }
            ]
        }
        
        json_file = tmp_path / "negative_time.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(invalid_data, f)
        
        with pytest.raises(ValueError, match="Negative timestamp"):
            load_corrected_subtitles(str(json_file))


class TestLoadMergedSegments:
    """
    类说明：封装 TestLoadMergedSegments 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    
    def test_load_valid_json(self, tmp_path):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - tmp_path: 文件路径（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        json_file = tmp_path / "merged_segments.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(SAMPLE_MERGED_SEGMENTS, f)
        
        segments = load_merged_segments(str(json_file))
        
        assert len(segments) == 2
        assert segments[0].segment_id == "P001"
        assert "快速排序的核心是分治思想" in segments[0].full_text
        assert len(segments[0].source_sentence_ids) == 2
        assert segments[0].merge_type == "同义转述"
        
        assert segments[1].segment_id == "P002"
        assert len(segments[1].source_sentence_ids) == 1
    
    def test_load_missing_file(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        with pytest.raises(FileNotFoundError):
            load_merged_segments("/nonexistent/file.json")
    
    def test_load_missing_required_field(self, tmp_path):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - tmp_path: 文件路径（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        invalid_data = {
            "pure_text_script": [
                {
                    "paragraph_id": "P001",
                    # Missing text, source_sentence_ids
                }
            ]
        }
        
        json_file = tmp_path / "missing_fields.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(invalid_data, f)
        
        with pytest.raises(ValueError, match="Missing required fields"):
            load_merged_segments(str(json_file))


class TestCreateModule2Input:
    """
    类说明：封装 TestCreateModule2Input 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    
    def test_create_valid_input(self, tmp_path):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - tmp_path: 文件路径（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        # Create test files
        sub_file = tmp_path / "corrected_subtitles.json"
        with open(sub_file, 'w', encoding='utf-8') as f:
            json.dump(SAMPLE_CORRECTED_SUBTITLES, f)
        
        seg_file = tmp_path / "merged_segments.json"
        with open(seg_file, 'w', encoding='utf-8') as f:
            json.dump(SAMPLE_MERGED_SEGMENTS, f)
        
        video_file = tmp_path / "test_video.mp4"
        video_file.touch()  # Create empty file
        
        output_dir = tmp_path / "output"
        
        # Create input
        module_input = create_module2_input(
            corrected_subtitles_path=str(sub_file),
            merged_segments_path=str(seg_file),
            video_path=str(video_file),
            output_dir=str(output_dir),
            domain="算法",
            main_topic="快速排序"
        )
        
        assert len(module_input.corrected_subtitles) == 3
        assert len(module_input.merged_segments) == 2
        assert module_input.domain == "算法"
        assert module_input.main_topic == "快速排序"
        assert Path(output_dir).exists()
    
    def test_create_missing_video(self, tmp_path):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - tmp_path: 文件路径（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        sub_file = tmp_path / "corrected_subtitles.json"
        with open(sub_file, 'w', encoding='utf-8') as f:
            json.dump(SAMPLE_CORRECTED_SUBTITLES, f)
        
        seg_file = tmp_path / "merged_segments.json"
        with open(seg_file, 'w', encoding='utf-8') as f:
            json.dump(SAMPLE_MERGED_SEGMENTS, f)
        
        with pytest.raises(FileNotFoundError, match="Video file not found"):
            create_module2_input(
                corrected_subtitles_path=str(sub_file),
                merged_segments_path=str(seg_file),
                video_path="/nonexistent/video.mp4",
                output_dir=str(tmp_path / "output"),
                domain="算法"
            )


class TestValidateInputConsistency:
    """
    类说明：封装 TestValidateInputConsistency 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    
    def test_validate_valid_input(self, tmp_path):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - tmp_path: 文件路径（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        sub_file = tmp_path / "corrected_subtitles.json"
        with open(sub_file, 'w', encoding='utf-8') as f:
            json.dump(SAMPLE_CORRECTED_SUBTITLES, f)
        
        seg_file = tmp_path / "merged_segments.json"
        with open(seg_file, 'w', encoding='utf-8') as f:
            json.dump(SAMPLE_MERGED_SEGMENTS, f)
        
        video_file = tmp_path / "test_video.mp4"
        video_file.touch()
        
        module_input = create_module2_input(
            corrected_subtitles_path=str(sub_file),
            merged_segments_path=str(seg_file),
            video_path=str(video_file),
            output_dir=str(tmp_path / "output"),
            domain="算法"
        )
        
        report = validate_input_consistency(module_input)
        
        assert report["valid"] == True
        assert report["subtitle_count"] == 3
        assert report["segment_count"] == 2

