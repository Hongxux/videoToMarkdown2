"""
VL Material Generator 单元测试

测试 Qwen3-VL-Plus 视频分析和素材生成模块

使用方法：
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
    """测试 VLVideoAnalyzer 类"""
    
    def __init__(self):
        self.results = []
    
    def test_config_loading(self):
        """测试 1: 配置加载"""
        logger.info("=" * 60)
        logger.info("测试 1: 配置加载")
        logger.info("=" * 60)
        
        try:
            from MVP_Module2_HEANCING.module2_content_enhancement.config_loader import load_module2_config
            config = load_module2_config()
            vl_config = config.get("vl_material_generation", {})
            
            assert "enabled" in vl_config, "配置缺少 enabled 字段"
            assert "api" in vl_config, "配置缺少 api 字段"
            assert "api_key" in vl_config.get("api", {}), "配置缺少 api_key 字段"
            
            logger.info(f"✅ 配置加载成功")
            logger.info(f"   - enabled: {vl_config.get('enabled')}")
            logger.info(f"   - model: {vl_config.get('api', {}).get('model')}")
            logger.info(f"   - api_key: {vl_config.get('api', {}).get('api_key', '')[:10]}...")
            
            self.results.append(("配置加载", True, ""))
            return True
            
        except Exception as e:
            logger.error(f"❌ 配置加载失败: {e}")
            self.results.append(("配置加载", False, str(e)))
            return False
    
    def test_timestamp_conversion(self):
        """测试 2: 时间戳转换"""
        logger.info("=" * 60)
        logger.info("测试 2: 时间戳转换")
        logger.info("=" * 60)
        
        try:
            from MVP_Module2_HEANCING.module2_content_enhancement.vl_video_analyzer import VLVideoAnalyzer
            
            # 模拟配置（不需要实际 API 调用）
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
                assert abs(actual - exp) < 0.001, f"时间戳 {i} 不匹配: {actual} != {exp}"
            
            logger.info(f"✅ 时间戳转换正确")
            logger.info(f"   - 相对时间: {relative_timestamps}")
            logger.info(f"   - 语义单元起点: {semantic_unit_start}s")
            logger.info(f"   - 绝对时间: {absolute_timestamps}")
            
            self.results.append(("时间戳转换", True, ""))
            return True
            
        except Exception as e:
            logger.error(f"❌ 时间戳转换测试失败: {e}")
            self.results.append(("时间戳转换", False, str(e)))
            return False
    
    def test_json_parsing(self):
        """测试 3: JSON 解析"""
        logger.info("=" * 60)
        logger.info("测试 3: JSON 解析")
        logger.info("=" * 60)
        
        try:
            from MVP_Module2_HEANCING.module2_content_enhancement.vl_video_analyzer import VLVideoAnalyzer
            
            mock_config = {
                "api": {"api_key": "test", "model": "test"},
                "screenshot_optimization": {"enabled": False},
                "fallback": {"enabled": True}
            }
            
            analyzer = VLVideoAnalyzer(mock_config)
            
            # Case 1: 模拟 AI 返回的 JSON（包含 markdown 代码块）
            mock_response = '''
根据视频内容分析，我识别出以下知识片段：

```json
[
  {
    "id": 0,
    "knowledge_type": "实操",
    "confidence": 0.9,
    "reasoning": "视频展示了具体的操作步骤",
    "key_evidence": ["鼠标点击", "键盘输入"],
    "clip_start_sec": 5.0,
    "clip_end_sec": 25.0,
    "suggested_screenshoot_timestamps": [8.0, 15.0, 22.0]
  }
]
```

以上是我的分析结果。
'''
            
            results = analyzer._parse_response(mock_response)
            
            assert len(results) == 1, f"预期 1 个结果，实际 {len(results)}"
            assert results[0].knowledge_type == "实操", f"知识类型不匹配"
            assert results[0].clip_start_sec == 5.0, f"起始时间不匹配"
            assert len(results[0].suggested_screenshoot_timestamps) == 3, f"截图时间戳数量不匹配"
            assert "鼠标点击" in results[0].key_evidence, "key_evidence 解析失败（数组 -> 字符串兼容）"
            
            logger.info(f"✅ JSON 解析成功")
            logger.info(f"   - 知识类型: {results[0].knowledge_type}")
            logger.info(f"   - 片段时间: {results[0].clip_start_sec}s - {results[0].clip_end_sec}s")
            logger.info(f"   - 截图时间戳: {results[0].suggested_screenshoot_timestamps}")
            logger.info(f"   - 关键证据: {results[0].key_evidence}")

            # Case 2: 模型常见错误：key_evidence 写成多个独立字符串（应修复为数组）
            broken_key_evidence_response = '''
            ```json
            [
              {
                "id": 0,
                "knowledge_type": "环境配置",
                "confidence": 0.98,
                "reasoning": "演示初始化配置流程",
                "key_evidence": "Install missing skill dependencies", "Set GOOGLE_PLACES_API_KEY for goplaces? No", "Enable hooks? Skip for now",
                "clip_start_sec": -1,
                "clip_end_sec": -1,
                "suggested_screenshoot_timestamps": []
              }
            ]
            ```
            '''
            results2 = analyzer._parse_response(broken_key_evidence_response)
            assert len(results2) == 1, f"预期 1 个结果，实际 {len(results2)}"
            assert "Install missing skill dependencies" in results2[0].key_evidence, "key_evidence 修复失败"
            assert "Enable hooks" in results2[0].key_evidence, "key_evidence 修复失败"

            # Case 3: 无代码块，仅自然语言包裹 JSON，验证括号配对提取
            wrapped_response = (
                "下面是结果：\\n"
                "[{\"id\":0,\"knowledge_type\":\"讲解型\",\"confidence\":0.8,"
                "\"reasoning\":\"静态介绍\",\"key_evidence\":[\"showcase\"],"
                "\"clip_start_sec\":-1,\"clip_end_sec\":-1,\"suggested_screenshoot_timestamps\":[]}]"
                "\\n（完）"
            )
            results3 = analyzer._parse_response(wrapped_response)
            assert len(results3) == 1, f"预期 1 个结果，实际 {len(results3)}"
            assert results3[0].knowledge_type == "讲解型", "括号提取解析失败"
            
            self.results.append(("JSON 解析", True, ""))
            return True
            
        except Exception as e:
            logger.error(f"❌ JSON 解析测试失败: {e}")
            import traceback
            traceback.print_exc()
            self.results.append(("JSON 解析", False, str(e)))
            return False
    
    async def test_single_clip_analysis(self):
        """测试 4: 单个视频片段 VL 分析（需要 API 调用）"""
        logger.info("=" * 60)
        logger.info("测试 4: 单个视频片段 VL 分析")
        logger.info("=" * 60)
        
        try:
            from MVP_Module2_HEANCING.module2_content_enhancement.vl_video_analyzer import VLVideoAnalyzer
            from MVP_Module2_HEANCING.module2_content_enhancement.config_loader import load_module2_config
            
            config = load_module2_config().get("vl_material_generation", {})
            
            if not config.get("api", {}).get("api_key"):
                logger.warning("⚠️ 跳过: API Key 未配置")
                self.results.append(("单片段分析", None, "API Key 未配置"))
                return None
            
            # 使用第一个测试视频片段
            clip_files = list(TEST_CLIPS_DIR.glob("*.mp4"))
            if not clip_files:
                logger.warning("⚠️ 跳过: 未找到测试视频片段")
                self.results.append(("单片段分析", None, "未找到测试视频片段"))
                return None
            
            # 选择一个较短的片段（文件大小较小）
            clip_files.sort(key=lambda x: x.stat().st_size)
            test_clip = clip_files[0]
            
            logger.info(f"   测试片段: {test_clip.name}")
            logger.info(f"   文件大小: {test_clip.stat().st_size / 1024 / 1024:.2f} MB")
            
            analyzer = VLVideoAnalyzer(config)
            
            # 从文件名解析起始时间 (格式: 001_SU001_topic_0.00-24.00.mp4)
            filename = test_clip.stem
            parts = filename.split("_")
            time_range = parts[-1]  # 0.00-24.00
            start_sec = float(time_range.split("-")[0])
            
            logger.info(f"   语义单元起始时间: {start_sec}s")
            logger.info("   正在调用 VL API...")
            
            result = await analyzer.analyze_clip(
                clip_path=str(test_clip),
                semantic_unit_start_sec=start_sec,
                semantic_unit_id="TEST_SU001"
            )
            
            if result.success:
                logger.info(f"✅ VL 分析成功")
                logger.info(f"   - 分析结果数: {len(result.analysis_results)}")
                logger.info(f"   - 视频片段请求: {len(result.clip_requests)}")
                logger.info(f"   - 截图请求: {len(result.screenshot_requests)}")
                
                if result.analysis_results:
                    ar = result.analysis_results[0]
                    logger.info(f"   - 第一个结果: {ar.knowledge_type}, {ar.clip_start_sec}s-{ar.clip_end_sec}s")
                    logger.info(f"   - 绝对时间: {ar.absolute_clip_start_sec}s-{ar.absolute_clip_end_sec}s")
                
                self.results.append(("单片段分析", True, ""))
                return True
            else:
                logger.error(f"❌ VL 分析失败: {result.error_msg}")
                self.results.append(("单片段分析", False, result.error_msg))
                return False
                
        except Exception as e:
            logger.error(f"❌ 单片段分析测试失败: {e}")
            import traceback
            traceback.print_exc()
            self.results.append(("单片段分析", False, str(e)))
            return False
    
    def print_summary(self):
        """打印测试摘要"""
        logger.info("=" * 60)
        logger.info("测试摘要")
        logger.info("=" * 60)
        
        passed = 0
        failed = 0
        skipped = 0
        
        for name, success, error in self.results:
            if success is True:
                passed += 1
                logger.info(f"  ✅ {name}: 通过")
            elif success is False:
                failed += 1
                logger.info(f"  ❌ {name}: 失败 - {error}")
            else:
                skipped += 1
                logger.info(f"  ⚠️ {name}: 跳过 - {error}")
        
        logger.info("")
        logger.info(f"总计: {passed} 通过, {failed} 失败, {skipped} 跳过")
        
        return failed == 0


async def run_tests():
    """运行所有测试"""
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
