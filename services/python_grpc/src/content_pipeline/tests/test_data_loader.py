"""
妯″潡璇存槑锛氭祴璇曠敤渚嬩笌鏂█闆嗗悎銆?
鎵ц閫昏緫锛?
1) 鑱氬悎鏈ā鍧楃殑绫?鍑芥暟锛屽澶栨彁渚涙牳蹇冭兘鍔涖€?
2) 閫氳繃鍐呴儴璋冪敤涓庡閮ㄤ緷璧栧畬鎴愬叿浣撳鐞嗐€?
瀹炵幇鏂瑰紡锛氶€氳繃妯″潡鍐呭嚱鏁扮粍鍚堜笌澶栭儴渚濊禆璋冪敤瀹炵幇銆?
鏍稿績浠峰€硷細缁熶竴妯″潡鑱岃矗杈圭晫锛岄檷浣庤法鏂囦欢鑰﹀悎鎴愭湰銆?
杈撳叆锛?
- 璋冪敤鏂逛紶鍏ョ殑鍙傛暟涓庢暟鎹矾寰勩€?
杈撳嚭锛?
- 鍚勫嚱鏁?绫昏繑鍥炵殑缁撴瀯鍖栫粨鏋滄垨鍓綔鐢ㄣ€?""

import json
import pytest
import tempfile
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.python_grpc.src.content_pipeline.shared.subtitle.data_loader import (
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
            "corrected_text": "浠婂ぉ鎴戜滑鏉ヨ瑙ｅ揩閫熸帓搴忕畻娉?,
            "start_sec": 0.0,
            "end_sec": 2.5,
            "corrections": []
        },
        {
            "subtitle_id": "SUB002",
            "corrected_text": "蹇€熸帓搴忕殑鏍稿績鏄垎娌绘€濇兂",
            "start_sec": 2.5,
            "end_sec": 5.0,
            "corrections": [
                {"original": "鏍稿瀷", "corrected": "鏍稿績", "reason": "鍚岄煶瀛?}
            ]
        },
        {
            "subtitle_id": "SUB003",
            "corrected_text": "棣栧厛閫夋嫨涓€涓熀鍑嗗€?,
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
            "text": "浠婂ぉ鎴戜滑鏉ヨ瑙ｅ揩閫熸帓搴忕畻娉?蹇€熸帓搴忕殑鏍稿績鏄垎娌绘€濇兂",
            "source_sentence_ids": ["S001", "S002"],
            "merge_type": "鍚屼箟杞堪"
        },
        {
            "paragraph_id": "P002",
            "text": "棣栧厛閫夋嫨涓€涓熀鍑嗗€?,
            "source_sentence_ids": ["S003"],
            "merge_type": "鏃犲悎骞?
        }
    ]
}


class TestLoadCorrectedSubtitles:
    """
    绫昏鏄庯細灏佽 TestLoadCorrectedSubtitles 鐨勮亴璐ｄ笌琛屼负銆?
    鎵ц閫昏緫锛?
    1) 缁存姢绫诲唴鐘舵€佷笌渚濊禆銆?
    2) 閫氳繃鏂规硶缁勫悎瀵瑰鎻愪緵鑳藉姏銆?
    瀹炵幇鏂瑰紡锛氶€氳繃鎴愬憳鍙橀噺涓庢柟娉曡皟鐢ㄥ疄鐜般€?
    鏍稿績浠峰€硷細闆嗕腑鐘舵€佷笌鏂规硶锛岄檷浣庡垎鏁ｅ疄鐜扮殑澶嶆潅搴︺€?
    杈撳叆锛?
    - 鏋勯€犲嚱鏁颁笌涓氬姟鏂规硶鐨勫叆鍙傘€?
    杈撳嚭锛?
    - 鏂规硶杩斿洖缁撴灉鎴栧唴閮ㄧ姸鎬佹洿鏂般€?""
    
    def test_load_valid_json(self, tmp_path):
        """
        鎵ц閫昏緫锛?
        1) 鍑嗗蹇呰涓婁笅鏂囦笌鍙傛暟銆?
        2) 鎵ц鏍稿績澶勭悊骞惰繑鍥炵粨鏋溿€?
        瀹炵幇鏂瑰紡锛氶€氳繃JSON 瑙ｆ瀽/搴忓垪鍖栥€佹枃浠剁郴缁熻鍐欏疄鐜般€?
        鏍稿績浠峰€硷細灏佽閫昏緫鍗曞厓锛屾彁鍗囧鐢ㄤ笌鍙淮鎶ゆ€с€?
        杈撳叆鍙傛暟锛?
        - tmp_path: 鏂囦欢璺緞锛堢被鍨嬶細鏈爣娉級銆?
        杈撳嚭鍙傛暟锛?
        - 鏃狅紙浠呬骇鐢熷壇浣滅敤锛屽鏃ュ織/鍐欑洏/鐘舵€佹洿鏂帮級銆?""
        json_file = tmp_path / "corrected_subtitles.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(SAMPLE_CORRECTED_SUBTITLES, f)
        
        subtitles = load_corrected_subtitles(str(json_file))
        
        assert len(subtitles) == 3
        assert subtitles[0].subtitle_id == "SUB001"
        assert subtitles[0].text == "浠婂ぉ鎴戜滑鏉ヨ瑙ｅ揩閫熸帓搴忕畻娉?
        assert subtitles[0].start_sec == 0.0
        assert subtitles[0].end_sec == 2.5
        
        assert len(subtitles[1].corrections) == 1
        assert subtitles[1].corrections[0]["corrected"] == "鏍稿績"
    
    def test_load_missing_file(self):
        """
        鎵ц閫昏緫锛?
        1) 鍑嗗蹇呰涓婁笅鏂囦笌鍙傛暟銆?
        2) 鎵ц鏍稿績澶勭悊骞惰繑鍥炵粨鏋溿€?
        瀹炵幇鏂瑰紡锛氶€氳繃JSON 瑙ｆ瀽/搴忓垪鍖栧疄鐜般€?
        鏍稿績浠峰€硷細灏佽閫昏緫鍗曞厓锛屾彁鍗囧鐢ㄤ笌鍙淮鎶ゆ€с€?
        杈撳叆鍙傛暟锛?
        - 鏃犮€?
        杈撳嚭鍙傛暟锛?
        - 鏃狅紙浠呬骇鐢熷壇浣滅敤锛屽鏃ュ織/鍐欑洏/鐘舵€佹洿鏂帮級銆?""
        with pytest.raises(FileNotFoundError):
            load_corrected_subtitles("/nonexistent/file.json")
    
    def test_load_invalid_json(self, tmp_path):
        """
        鎵ц閫昏緫锛?
        1) 鍑嗗蹇呰涓婁笅鏂囦笌鍙傛暟銆?
        2) 鎵ц鏍稿績澶勭悊骞惰繑鍥炵粨鏋溿€?
        瀹炵幇鏂瑰紡锛氶€氳繃JSON 瑙ｆ瀽/搴忓垪鍖栥€佹枃浠剁郴缁熻鍐欏疄鐜般€?
        鏍稿績浠峰€硷細灏佽閫昏緫鍗曞厓锛屾彁鍗囧鐢ㄤ笌鍙淮鎶ゆ€с€?
        杈撳叆鍙傛暟锛?
        - tmp_path: 鏂囦欢璺緞锛堢被鍨嬶細鏈爣娉級銆?
        杈撳嚭鍙傛暟锛?
        - 鏃狅紙浠呬骇鐢熷壇浣滅敤锛屽鏃ュ織/鍐欑洏/鐘舵€佹洿鏂帮級銆?""
        json_file = tmp_path / "invalid.json"
        with open(json_file, 'w') as f:
            f.write("not valid json{")
        
        with pytest.raises(ValueError, match="Invalid JSON"):
            load_corrected_subtitles(str(json_file))
    
    def test_load_missing_required_field(self, tmp_path):
        """
        鎵ц閫昏緫锛?
        1) 鍑嗗蹇呰涓婁笅鏂囦笌鍙傛暟銆?
        2) 鎵ц鏍稿績澶勭悊骞惰繑鍥炵粨鏋溿€?
        瀹炵幇鏂瑰紡锛氶€氳繃JSON 瑙ｆ瀽/搴忓垪鍖栥€佹枃浠剁郴缁熻鍐欏疄鐜般€?
        鏍稿績浠峰€硷細灏佽閫昏緫鍗曞厓锛屾彁鍗囧鐢ㄤ笌鍙淮鎶ゆ€с€?
        杈撳叆鍙傛暟锛?
        - tmp_path: 鏂囦欢璺緞锛堢被鍨嬶細鏈爣娉級銆?
        杈撳嚭鍙傛暟锛?
        - 鏃狅紙浠呬骇鐢熷壇浣滅敤锛屽鏃ュ織/鍐欑洏/鐘舵€佹洿鏂帮級銆?""
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
        鎵ц閫昏緫锛?
        1) 鍑嗗蹇呰涓婁笅鏂囦笌鍙傛暟銆?
        2) 鎵ц鏍稿績澶勭悊骞惰繑鍥炵粨鏋溿€?
        瀹炵幇鏂瑰紡锛氶€氳繃JSON 瑙ｆ瀽/搴忓垪鍖栥€佹枃浠剁郴缁熻鍐欏疄鐜般€?
        鏍稿績浠峰€硷細灏佽閫昏緫鍗曞厓锛屾彁鍗囧鐢ㄤ笌鍙淮鎶ゆ€с€?
        杈撳叆鍙傛暟锛?
        - tmp_path: 鏂囦欢璺緞锛堢被鍨嬶細鏈爣娉級銆?
        杈撳嚭鍙傛暟锛?
        - 鏃狅紙浠呬骇鐢熷壇浣滅敤锛屽鏃ュ織/鍐欑洏/鐘舵€佹洿鏂帮級銆?""
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
    绫昏鏄庯細灏佽 TestLoadMergedSegments 鐨勮亴璐ｄ笌琛屼负銆?
    鎵ц閫昏緫锛?
    1) 缁存姢绫诲唴鐘舵€佷笌渚濊禆銆?
    2) 閫氳繃鏂规硶缁勫悎瀵瑰鎻愪緵鑳藉姏銆?
    瀹炵幇鏂瑰紡锛氶€氳繃鎴愬憳鍙橀噺涓庢柟娉曡皟鐢ㄥ疄鐜般€?
    鏍稿績浠峰€硷細闆嗕腑鐘舵€佷笌鏂规硶锛岄檷浣庡垎鏁ｅ疄鐜扮殑澶嶆潅搴︺€?
    杈撳叆锛?
    - 鏋勯€犲嚱鏁颁笌涓氬姟鏂规硶鐨勫叆鍙傘€?
    杈撳嚭锛?
    - 鏂规硶杩斿洖缁撴灉鎴栧唴閮ㄧ姸鎬佹洿鏂般€?""
    
    def test_load_valid_json(self, tmp_path):
        """
        鎵ц閫昏緫锛?
        1) 鍑嗗蹇呰涓婁笅鏂囦笌鍙傛暟銆?
        2) 鎵ц鏍稿績澶勭悊骞惰繑鍥炵粨鏋溿€?
        瀹炵幇鏂瑰紡锛氶€氳繃JSON 瑙ｆ瀽/搴忓垪鍖栥€佹枃浠剁郴缁熻鍐欏疄鐜般€?
        鏍稿績浠峰€硷細灏佽閫昏緫鍗曞厓锛屾彁鍗囧鐢ㄤ笌鍙淮鎶ゆ€с€?
        杈撳叆鍙傛暟锛?
        - tmp_path: 鏂囦欢璺緞锛堢被鍨嬶細鏈爣娉級銆?
        杈撳嚭鍙傛暟锛?
        - 鏃狅紙浠呬骇鐢熷壇浣滅敤锛屽鏃ュ織/鍐欑洏/鐘舵€佹洿鏂帮級銆?""
        json_file = tmp_path / "merged_segments.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(SAMPLE_MERGED_SEGMENTS, f)
        
        segments = load_merged_segments(str(json_file))
        
        assert len(segments) == 2
        assert segments[0].segment_id == "P001"
        assert "蹇€熸帓搴忕殑鏍稿績鏄垎娌绘€濇兂" in segments[0].full_text
        assert len(segments[0].source_sentence_ids) == 2
        assert segments[0].merge_type == "鍚屼箟杞堪"
        
        assert segments[1].segment_id == "P002"
        assert len(segments[1].source_sentence_ids) == 1
    
    def test_load_missing_file(self):
        """
        鎵ц閫昏緫锛?
        1) 鍑嗗蹇呰涓婁笅鏂囦笌鍙傛暟銆?
        2) 鎵ц鏍稿績澶勭悊骞惰繑鍥炵粨鏋溿€?
        瀹炵幇鏂瑰紡锛氶€氳繃JSON 瑙ｆ瀽/搴忓垪鍖栧疄鐜般€?
        鏍稿績浠峰€硷細灏佽閫昏緫鍗曞厓锛屾彁鍗囧鐢ㄤ笌鍙淮鎶ゆ€с€?
        杈撳叆鍙傛暟锛?
        - 鏃犮€?
        杈撳嚭鍙傛暟锛?
        - 鏃狅紙浠呬骇鐢熷壇浣滅敤锛屽鏃ュ織/鍐欑洏/鐘舵€佹洿鏂帮級銆?""
        with pytest.raises(FileNotFoundError):
            load_merged_segments("/nonexistent/file.json")
    
    def test_load_missing_required_field(self, tmp_path):
        """
        鎵ц閫昏緫锛?
        1) 鍑嗗蹇呰涓婁笅鏂囦笌鍙傛暟銆?
        2) 鎵ц鏍稿績澶勭悊骞惰繑鍥炵粨鏋溿€?
        瀹炵幇鏂瑰紡锛氶€氳繃JSON 瑙ｆ瀽/搴忓垪鍖栥€佹枃浠剁郴缁熻鍐欏疄鐜般€?
        鏍稿績浠峰€硷細灏佽閫昏緫鍗曞厓锛屾彁鍗囧鐢ㄤ笌鍙淮鎶ゆ€с€?
        杈撳叆鍙傛暟锛?
        - tmp_path: 鏂囦欢璺緞锛堢被鍨嬶細鏈爣娉級銆?
        杈撳嚭鍙傛暟锛?
        - 鏃狅紙浠呬骇鐢熷壇浣滅敤锛屽鏃ュ織/鍐欑洏/鐘舵€佹洿鏂帮級銆?""
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
    绫昏鏄庯細灏佽 TestCreateModule2Input 鐨勮亴璐ｄ笌琛屼负銆?
    鎵ц閫昏緫锛?
    1) 缁存姢绫诲唴鐘舵€佷笌渚濊禆銆?
    2) 閫氳繃鏂规硶缁勫悎瀵瑰鎻愪緵鑳藉姏銆?
    瀹炵幇鏂瑰紡锛氶€氳繃鎴愬憳鍙橀噺涓庢柟娉曡皟鐢ㄥ疄鐜般€?
    鏍稿績浠峰€硷細闆嗕腑鐘舵€佷笌鏂规硶锛岄檷浣庡垎鏁ｅ疄鐜扮殑澶嶆潅搴︺€?
    杈撳叆锛?
    - 鏋勯€犲嚱鏁颁笌涓氬姟鏂规硶鐨勫叆鍙傘€?
    杈撳嚭锛?
    - 鏂规硶杩斿洖缁撴灉鎴栧唴閮ㄧ姸鎬佹洿鏂般€?""
    
    def test_create_valid_input(self, tmp_path):
        """
        鎵ц閫昏緫锛?
        1) 鍑嗗蹇呰涓婁笅鏂囦笌鍙傛暟銆?
        2) 鎵ц鏍稿績澶勭悊骞惰繑鍥炵粨鏋溿€?
        瀹炵幇鏂瑰紡锛氶€氳繃JSON 瑙ｆ瀽/搴忓垪鍖栥€佹枃浠剁郴缁熻鍐欏疄鐜般€?
        鏍稿績浠峰€硷細灏佽閫昏緫鍗曞厓锛屾彁鍗囧鐢ㄤ笌鍙淮鎶ゆ€с€?
        杈撳叆鍙傛暟锛?
        - tmp_path: 鏂囦欢璺緞锛堢被鍨嬶細鏈爣娉級銆?
        杈撳嚭鍙傛暟锛?
        - 鏃狅紙浠呬骇鐢熷壇浣滅敤锛屽鏃ュ織/鍐欑洏/鐘舵€佹洿鏂帮級銆?""
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
            domain="绠楁硶",
            main_topic="蹇€熸帓搴?
        )
        
        assert len(module_input.corrected_subtitles) == 3
        assert len(module_input.merged_segments) == 2
        assert module_input.domain == "绠楁硶"
        assert module_input.main_topic == "蹇€熸帓搴?
        assert Path(output_dir).exists()
    
    def test_create_missing_video(self, tmp_path):
        """
        鎵ц閫昏緫锛?
        1) 鍑嗗蹇呰涓婁笅鏂囦笌鍙傛暟銆?
        2) 鎵ц鏍稿績澶勭悊骞惰繑鍥炵粨鏋溿€?
        瀹炵幇鏂瑰紡锛氶€氳繃JSON 瑙ｆ瀽/搴忓垪鍖栥€佹枃浠剁郴缁熻鍐欏疄鐜般€?
        鏍稿績浠峰€硷細灏佽閫昏緫鍗曞厓锛屾彁鍗囧鐢ㄤ笌鍙淮鎶ゆ€с€?
        杈撳叆鍙傛暟锛?
        - tmp_path: 鏂囦欢璺緞锛堢被鍨嬶細鏈爣娉級銆?
        杈撳嚭鍙傛暟锛?
        - 鏃狅紙浠呬骇鐢熷壇浣滅敤锛屽鏃ュ織/鍐欑洏/鐘舵€佹洿鏂帮級銆?""
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
                domain="绠楁硶"
            )


class TestValidateInputConsistency:
    """
    绫昏鏄庯細灏佽 TestValidateInputConsistency 鐨勮亴璐ｄ笌琛屼负銆?
    鎵ц閫昏緫锛?
    1) 缁存姢绫诲唴鐘舵€佷笌渚濊禆銆?
    2) 閫氳繃鏂规硶缁勫悎瀵瑰鎻愪緵鑳藉姏銆?
    瀹炵幇鏂瑰紡锛氶€氳繃鎴愬憳鍙橀噺涓庢柟娉曡皟鐢ㄥ疄鐜般€?
    鏍稿績浠峰€硷細闆嗕腑鐘舵€佷笌鏂规硶锛岄檷浣庡垎鏁ｅ疄鐜扮殑澶嶆潅搴︺€?
    杈撳叆锛?
    - 鏋勯€犲嚱鏁颁笌涓氬姟鏂规硶鐨勫叆鍙傘€?
    杈撳嚭锛?
    - 鏂规硶杩斿洖缁撴灉鎴栧唴閮ㄧ姸鎬佹洿鏂般€?""
    
    def test_validate_valid_input(self, tmp_path):
        """
        鎵ц閫昏緫锛?
        1) 鍑嗗蹇呰涓婁笅鏂囦笌鍙傛暟銆?
        2) 鎵ц鏍稿績澶勭悊骞惰繑鍥炵粨鏋溿€?
        瀹炵幇鏂瑰紡锛氶€氳繃JSON 瑙ｆ瀽/搴忓垪鍖栥€佹枃浠剁郴缁熻鍐欏疄鐜般€?
        鏍稿績浠峰€硷細灏佽閫昏緫鍗曞厓锛屾彁鍗囧鐢ㄤ笌鍙淮鎶ゆ€с€?
        杈撳叆鍙傛暟锛?
        - tmp_path: 鏂囦欢璺緞锛堢被鍨嬶細鏈爣娉級銆?
        杈撳嚭鍙傛暟锛?
        - 鏃狅紙浠呬骇鐢熷壇浣滅敤锛屽鏃ュ織/鍐欑洏/鐘舵€佹洿鏂帮級銆?""
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
            domain="绠楁硶"
        )
        
        report = validate_input_consistency(module_input)
        
        assert report["valid"] == True
        assert report["subtitle_count"] == 3
        assert report["segment_count"] == 2



