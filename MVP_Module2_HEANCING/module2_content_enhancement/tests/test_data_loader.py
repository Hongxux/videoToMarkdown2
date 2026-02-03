"""
Unit tests for data_loader module

Tests JSON parsing and validation logic.
"""

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
    """Test corrected_subtitles loading"""
    
    def test_load_valid_json(self, tmp_path):
        """Test loading valid corrected_subtitles JSON"""
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
        """Test loading non-existent file"""
        with pytest.raises(FileNotFoundError):
            load_corrected_subtitles("/nonexistent/file.json")
    
    def test_load_invalid_json(self, tmp_path):
        """Test loading malformed JSON"""
        json_file = tmp_path / "invalid.json"
        with open(json_file, 'w') as f:
            f.write("not valid json{")
        
        with pytest.raises(ValueError, match="Invalid JSON"):
            load_corrected_subtitles(str(json_file))
    
    def test_load_missing_required_field(self, tmp_path):
        """Test loading JSON with missing required fields"""
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
        """Test validation catches negative timestamps"""
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
    """Test merged_segments loading"""
    
    def test_load_valid_json(self, tmp_path):
        """Test loading valid merged_segments JSON"""
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
        """Test loading non-existent file"""
        with pytest.raises(FileNotFoundError):
            load_merged_segments("/nonexistent/file.json")
    
    def test_load_missing_required_field(self, tmp_path):
        """Test loading JSON with missing fields"""
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
    """Test complete Module2Input creation"""
    
    def test_create_valid_input(self, tmp_path):
        """Test creating Module2Input from valid files"""
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
        """Test error when video file doesn't exist"""
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
    """Test input validation"""
    
    def test_validate_valid_input(self, tmp_path):
        """Test validation of valid input"""
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
