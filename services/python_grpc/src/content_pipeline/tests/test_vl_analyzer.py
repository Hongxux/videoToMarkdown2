"""
VL Material Generator 鍗曞厓娴嬭瘯

娴嬭瘯 Qwen3-VL-Plus 瑙嗛鍒嗘瀽鍜岀礌鏉愮敓鎴愭ā鍧?

浣跨敤鏂规硶锛?
    cd D:\videoToMarkdownTest2
    python MVP_Module2_HEANCING/module2_content_enhancement/tests/test_vl_analyzer.py
"""

import os
import sys
import json
import asyncio
import logging
from pathlib import Path

# 设置项目路径
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("VLAnalyzerTest")

# 测试数据路径
TEST_STORAGE = project_root / "storage" / "20225626c2a19253c4121f684ecdff12"
TEST_VIDEO = TEST_STORAGE / "video.mp4"
TEST_CLIPS_DIR = TEST_STORAGE / "semantic_unit_clips"
TEST_SEMANTIC_UNITS = TEST_STORAGE / "semantic_units_phase2a.json"


class TestVLVideoAnalyzer:
    """娴嬭瘯 VLVideoAnalyzer 绫?""
    
    def __init__(self):
        self.results = []
    
    def test_config_loading(self):
        """娴嬭瘯 1: 閰嶇疆鍔犺浇"""
        logger.info("=" * 60)
        logger.info("娴嬭瘯 1: 閰嶇疆鍔犺浇")
        logger.info("=" * 60)
        
        try:
            from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import load_module2_config
            config = load_module2_config()
            vl_config = config.get("vl_material_generation", {})
            
            assert "enabled" in vl_config, "閰嶇疆缂哄皯 enabled 瀛楁"
            assert "api" in vl_config, "閰嶇疆缂哄皯 api 瀛楁"
            assert "api_key" in vl_config.get("api", {}), "閰嶇疆缂哄皯 api_key 瀛楁"
            
            logger.info(f"鉁?閰嶇疆鍔犺浇鎴愬姛")
            logger.info(f"   - enabled: {vl_config.get('enabled')}")
            logger.info(f"   - model: {vl_config.get('api', {}).get('model')}")
            logger.info(f"   - api_key: {vl_config.get('api', {}).get('api_key', '')[:10]}...")
            
            self.results.append(("閰嶇疆鍔犺浇", True, ""))
            return True
            
        except Exception as e:
            logger.error(f"鉂?閰嶇疆鍔犺浇澶辫触: {e}")
            self.results.append(("閰嶇疆鍔犺浇", False, str(e)))
            return False
    
    def test_timestamp_conversion(self):
        """娴嬭瘯 2: 鏃堕棿鎴宠浆鎹?""
        logger.info("=" * 60)
        logger.info("娴嬭瘯 2: 鏃堕棿鎴宠浆鎹?)
        logger.info("=" * 60)
        
        try:
            from services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer import VLVideoAnalyzer
            
            # 妯℃嫙閰嶇疆锛堜笉闇€瑕佸疄闄?API 璋冪敤锛?
            mock_config = {
                "api": {"api_key": "test", "model": "test"},
                "screenshot_optimization": {"enabled": False},
                "fallback": {"enabled": True}
            }
            
            analyzer = VLVideoAnalyzer(mock_config)
            
            # 测试用例
            relative_timestamps = [0.0, 5.5, 10.2]
            semantic_unit_start = 100.0
            
            absolute_timestamps = analyzer.convert_timestamps(
                relative_timestamps, 
                semantic_unit_start
            )
            
            expected = [100.0, 105.5, 110.2]
            
            for i, (actual, exp) in enumerate(zip(absolute_timestamps, expected)):
                assert abs(actual - exp) < 0.001, f"鏃堕棿鎴?{i} 涓嶅尮閰? {actual} != {exp}"
            
            logger.info(f"鉁?鏃堕棿鎴宠浆鎹㈡纭?)
            logger.info(f"   - 鐩稿鏃堕棿: {relative_timestamps}")
            logger.info(f"   - 璇箟鍗曞厓璧风偣: {semantic_unit_start}s")
            logger.info(f"   - 缁濆鏃堕棿: {absolute_timestamps}")
            
            self.results.append(("鏃堕棿鎴宠浆鎹?, True, ""))
            return True
            
        except Exception as e:
            logger.error(f"鉂?鏃堕棿鎴宠浆鎹㈡祴璇曞け璐? {e}")
            self.results.append(("鏃堕棿鎴宠浆鎹?, False, str(e)))
            return False
    
    def test_json_parsing(self):
        """娴嬭瘯 3: JSON 瑙ｆ瀽"""
        logger.info("=" * 60)
        logger.info("娴嬭瘯 3: JSON 瑙ｆ瀽")
        logger.info("=" * 60)
        
        try:
            from services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer import VLVideoAnalyzer
            
            mock_config = {
                "api": {"api_key": "test", "model": "test"},
                "screenshot_optimization": {"enabled": False},
                "fallback": {"enabled": True}
            }
            
            analyzer = VLVideoAnalyzer(mock_config)
            
            # Case 1: 妯℃嫙 AI 杩斿洖鐨?JSON锛堝寘鍚?markdown 浠ｇ爜鍧楋級
            mock_response = '''
鏍规嵁瑙嗛鍐呭鍒嗘瀽锛屾垜璇嗗埆鍑轰互涓嬬煡璇嗙墖娈碉細

```json
[
  {
    "id": 0,
    "knowledge_type": "瀹炴搷",
    "confidence": 0.9,
    "reasoning": "瑙嗛灞曠ず浜嗗叿浣撶殑鎿嶄綔姝ラ",
    "key_evidence": ["榧犳爣鐐瑰嚮", "閿洏杈撳叆"],
    "clip_start_sec": 5.0,
    "clip_end_sec": 25.0,
    "suggested_screenshoot_timestamps": [8.0, 15.0, 22.0]
  }
]
```

浠ヤ笂鏄垜鐨勫垎鏋愮粨鏋溿€?'''
            
            results = analyzer._parse_response(mock_response)
            
            assert len(results) == 1, f"棰勬湡 1 涓粨鏋滐紝瀹為檯 {len(results)}"
            assert results[0].knowledge_type == "瀹炴搷", f"鐭ヨ瘑绫诲瀷涓嶅尮閰?
            assert results[0].clip_start_sec == 5.0, f"璧峰鏃堕棿涓嶅尮閰?
            assert len(results[0].suggested_screenshoot_timestamps) == 3, f"鎴浘鏃堕棿鎴虫暟閲忎笉鍖归厤"
            assert "榧犳爣鐐瑰嚮" in results[0].key_evidence, "key_evidence 瑙ｆ瀽澶辫触锛堟暟缁?-> 瀛楃涓插吋瀹癸級"
            
            logger.info(f"鉁?JSON 瑙ｆ瀽鎴愬姛")
            logger.info(f"   - 鐭ヨ瘑绫诲瀷: {results[0].knowledge_type}")
            logger.info(f"   - 鐗囨鏃堕棿: {results[0].clip_start_sec}s - {results[0].clip_end_sec}s")
            logger.info(f"   - 鎴浘鏃堕棿鎴? {results[0].suggested_screenshoot_timestamps}")
            logger.info(f"   - 鍏抽敭璇佹嵁: {results[0].key_evidence}")

            # Encoding fixed: Case note.
            ```json
            [
              {
                "id": 0,
                "knowledge_type": "鐜閰嶇疆",
                "confidence": 0.98,
                "reasoning": "婕旂ず鍒濆鍖栭厤缃祦绋?,
                "key_evidence": "Install missing skill dependencies", "Set GOOGLE_PLACES_API_KEY for goplaces? No", "Enable hooks? Skip for now",
                "clip_start_sec": -1,
                "clip_end_sec": -1,
                "suggested_screenshoot_timestamps": []
              }
            ]
            ```
            '''
            results2 = analyzer._parse_response(broken_key_evidence_response)
            assert len(results2) == 1, f"棰勬湡 1 涓粨鏋滐紝瀹為檯 {len(results2)}"
            assert "Install missing skill dependencies" in results2[0].key_evidence, "key_evidence 淇澶辫触"
            assert "Enable hooks" in results2[0].key_evidence, "key_evidence 淇澶辫触"

            # Encoding fixed: JSON / Case note.
                "涓嬮潰鏄粨鏋滐細\\n"
                "[{\"id\":0,\"knowledge_type\":\"璁茶В鍨媆",\"confidence\":0.8,"
                "\"reasoning\":\"闈欐€佷粙缁峔",\"key_evidence\":[\"showcase\"],"
                "\"clip_start_sec\":-1,\"clip_end_sec\":-1,\"suggested_screenshoot_timestamps\":[]}]"
                "\\n锛堝畬锛?
            )
            results3 = analyzer._parse_response(wrapped_response)
            assert len(results3) == 1, f"棰勬湡 1 涓粨鏋滐紝瀹為檯 {len(results3)}"
            assert results3[0].knowledge_type == "璁茶В鍨?, "鎷彿鎻愬彇瑙ｆ瀽澶辫触"
            
            self.results.append(("JSON 瑙ｆ瀽", True, ""))
            return True
            
        except Exception as e:
            logger.error(f"鉂?JSON 瑙ｆ瀽娴嬭瘯澶辫触: {e}")
            import traceback
            traceback.print_exc()
            self.results.append(("JSON 瑙ｆ瀽", False, str(e)))
            return False
    
    async def test_single_clip_analysis(self):
        """娴嬭瘯 4: 鍗曚釜瑙嗛鐗囨 VL 鍒嗘瀽锛堥渶瑕?API 璋冪敤锛?""
        logger.info("=" * 60)
        logger.info("娴嬭瘯 4: 鍗曚釜瑙嗛鐗囨 VL 鍒嗘瀽")
        logger.info("=" * 60)
        
        try:
            from services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer import VLVideoAnalyzer
            from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import load_module2_config
            
            config = load_module2_config().get("vl_material_generation", {})
            
            if not config.get("api", {}).get("api_key"):
                logger.warning("鈿狅笍 璺宠繃: API Key 鏈厤缃?)
                self.results.append(("鍗曠墖娈靛垎鏋?, None, "API Key 鏈厤缃?))
                return None
            
            # 使用第一个测试视频片?
            clip_files = list(TEST_CLIPS_DIR.glob("*.mp4"))
            if not clip_files:
                logger.warning("鈿狅笍 璺宠繃: 鏈壘鍒版祴璇曡棰戠墖娈?)
                self.results.append(("鍗曠墖娈靛垎鏋?, None, "鏈壘鍒版祴璇曡棰戠墖娈?))
                return None
            
            # Encoding fixed: corrupted comment cleaned.
            clip_files.sort(key=lambda x: x.stat().st_size)
            test_clip = clip_files[0]
            
            logger.info(f"   娴嬭瘯鐗囨: {test_clip.name}")
            logger.info(f"   鏂囦欢澶у皬: {test_clip.stat().st_size / 1024 / 1024:.2f} MB")
            
            analyzer = VLVideoAnalyzer(config)
            
            # 从文件名解析起始时间 (格式: 001_SU001_topic_0.00-24.00.mp4)
            filename = test_clip.stem
            parts = filename.split("_")
            time_range = parts[-1]  # 0.00-24.00
            start_sec = float(time_range.split("-")[0])
            
            logger.info(f"   璇箟鍗曞厓璧峰鏃堕棿: {start_sec}s")
            logger.info("   姝ｅ湪璋冪敤 VL API...")
            
            result = await analyzer.analyze_clip(
                clip_path=str(test_clip),
                semantic_unit_start_sec=start_sec,
                semantic_unit_id="TEST_SU001"
            )
            
            if result.success:
                logger.info(f"鉁?VL 鍒嗘瀽鎴愬姛")
                logger.info(f"   - 鍒嗘瀽缁撴灉鏁? {len(result.analysis_results)}")
                logger.info(f"   - 瑙嗛鐗囨璇锋眰: {len(result.clip_requests)}")
                logger.info(f"   - 鎴浘璇锋眰: {len(result.screenshot_requests)}")
                
                if result.analysis_results:
                    ar = result.analysis_results[0]
                    logger.info(f"   - 绗竴涓粨鏋? {ar.knowledge_type}, {ar.clip_start_sec}s-{ar.clip_end_sec}s")
                    logger.info(f"   - 缁濆鏃堕棿: {ar.absolute_clip_start_sec}s-{ar.absolute_clip_end_sec}s")
                
                self.results.append(("鍗曠墖娈靛垎鏋?, True, ""))
                return True
            else:
                logger.error(f"鉂?VL 鍒嗘瀽澶辫触: {result.error_msg}")
                self.results.append(("鍗曠墖娈靛垎鏋?, False, result.error_msg))
                return False
                
        except Exception as e:
            logger.error(f"鉂?鍗曠墖娈靛垎鏋愭祴璇曞け璐? {e}")
            import traceback
            traceback.print_exc()
            self.results.append(("鍗曠墖娈靛垎鏋?, False, str(e)))
            return False
    
    def print_summary(self):
        """鎵撳嵃娴嬭瘯鎽樿"""
        logger.info("=" * 60)
        logger.info("娴嬭瘯鎽樿")
        logger.info("=" * 60)
        
        passed = 0
        failed = 0
        skipped = 0
        
        for name, success, error in self.results:
            if success is True:
                passed += 1
                logger.info(f"  鉁?{name}: 閫氳繃")
            elif success is False:
                failed += 1
                logger.info(f"  鉂?{name}: 澶辫触 - {error}")
            else:
                skipped += 1
                logger.info(f"  鈿狅笍 {name}: 璺宠繃 - {error}")
        
        logger.info("")
        logger.info(f"鎬昏: {passed} 閫氳繃, {failed} 澶辫触, {skipped} 璺宠繃")
        
        return failed == 0


async def run_tests():
    """杩愯鎵€鏈夋祴璇?""
    tester = TestVLVideoAnalyzer()
    
    # 运行同步测试
    tester.test_config_loading()
    tester.test_timestamp_conversion()
    tester.test_json_parsing()
    
    # 运行异步测试
    await tester.test_single_clip_analysis()
    
    # 打印摘要
    return tester.print_summary()


if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)


