"""
V7.x 模态分类 gRPC 集成测试

测试流程:
1. 直接调用 Python adapter (单元测试)
2. 模拟 gRPC 调用流程 (集成测试)

运行方式:
  cd enterprise_services/python_worker
  python test_modality_grpc.py
"""

import sys
import os
import time

# 添加路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# 导入生成的 proto
try:
    import fusion_service_pb2
    import fusion_service_pb2_grpc
    print("✅ Proto files loaded successfully")
except ImportError as e:
    print(f"❌ Proto import failed: {e}")
    print("   Run: python -m grpc_tools.protoc -I./protos --python_out=./python_worker --grpc_python_out=./python_worker ./protos/fusion_service.proto")
    sys.exit(1)

from service_adapter import FeatureExtractionAdapter


class MockRedis:
    def set(self, key, value): pass
    def get(self, key): return None


def test_modality_classification_direct():
    """
    测试1: 直接调用 adapter (绕过 gRPC)
    """
    print("\n" + "="*60)
    print("测试1: 直接调用 adapter (单元测试)")
    print("="*60)
    
    video_path = 'd:/videoToMarkdownTest2/mvp_test_outputs/downloads/video.mp4'
    
    if not os.path.exists(video_path):
        print(f"⚠️ 视频文件不存在: {video_path}")
        print("   跳过测试")
        return False
    
    # 创建 adapter
    adapter = FeatureExtractionAdapter(MockRedis())
    
    # 创建模拟请求
    request = fusion_service_pb2.ModalityClassificationRequest(
        request_id="test-001",
        video_path=video_path,
        start_sec=10.0,
        end_sec=20.0
    )
    
    print(f"\n请求: {video_path} [{request.start_sec:.1f}s - {request.end_sec:.1f}s]")
    
    start_time = time.time()
    response = adapter.get_modality_classification(request)
    elapsed = time.time() - start_time
    
    print(f"\n响应 (耗时 {elapsed:.2f}s):")
    print(f"  success: {response.success}")
    print(f"  modality: {response.modality}")
    print(f"  knowledge_subtype: {response.knowledge_subtype}")
    print(f"  screenshot_times: {list(response.screenshot_times)}")
    print(f"  stable_island_count: {response.stable_island_count}")
    print(f"  action_unit_count: {response.action_unit_count}")
    
    if response.action_segments:
        print(f"\n  动作详情 ({len(response.action_segments)} segments):")
        for i, seg in enumerate(response.action_segments):
            print(f"    [{i+1}] {seg.start_sec:.1f}s-{seg.end_sec:.1f}s | "
                  f"modality={seg.modality} | subtype={seg.subtype} | "
                  f"duration={seg.duration_ms}ms")
    
    if response.success:
        print("\n✅ 测试1 通过")
        return True
    else:
        print(f"\n❌ 测试1 失败: {response.error_message}")
        return False


def test_modality_classification_multiple_segments():
    """
    测试2: 多段落分类测试
    """
    print("\n" + "="*60)
    print("测试2: 多段落分类 (模拟批处理)")
    print("="*60)
    
    video_path = 'd:/videoToMarkdownTest2/mvp_test_outputs/downloads/video.mp4'
    
    if not os.path.exists(video_path):
        print(f"⚠️ 视频文件不存在, 跳过测试")
        return False
    
    adapter = FeatureExtractionAdapter(MockRedis())
    
    # 模拟多个语义单元
    segments = [
        (5.0, 10.0, "概念讲解"),
        (15.0, 25.0, "动态演示"),
        (30.0, 40.0, "公式推导"),
    ]
    
    results = []
    for start, end, desc in segments:
        request = fusion_service_pb2.ModalityClassificationRequest(
            request_id=f"test-{start}-{end}",
            video_path=video_path,
            start_sec=start,
            end_sec=end
        )
        
        print(f"\n  [{start:.0f}s-{end:.0f}s] {desc}...")
        response = adapter.get_modality_classification(request)
        
        results.append({
            'segment': f"{start:.0f}-{end:.0f}s",
            'desc': desc,
            'modality': response.modality,
            'subtype': response.knowledge_subtype,
            'success': response.success
        })
        
        print(f"    → modality: {response.modality} | subtype: {response.knowledge_subtype}")
    
    print("\n汇总:")
    print("-" * 50)
    for r in results:
        status = "✅" if r['success'] else "❌"
        print(f"  {status} {r['segment']} | {r['modality']:15} | {r['subtype'] or 'N/A'}")
    
    all_success = all(r['success'] for r in results)
    print(f"\n{'✅' if all_success else '❌'} 测试2 {'通过' if all_success else '部分失败'}")
    return all_success


def test_java_workflow_simulation():
    """
    测试3: 模拟 Java 调用流程
    
    模拟 FusionDecisionService.processVideoWithModalityClassification() 的逻辑
    """
    print("\n" + "="*60)
    print("测试3: 模拟 Java 调用流程")
    print("="*60)
    
    video_path = 'd:/videoToMarkdownTest2/mvp_test_outputs/downloads/video.mp4'
    
    if not os.path.exists(video_path):
        print(f"⚠️ 视频文件不存在, 跳过测试")
        return False
    
    adapter = FeatureExtractionAdapter(MockRedis())
    
    # Step 1: 模拟 Java 调用 getModalityClassification
    request = fusion_service_pb2.ModalityClassificationRequest(
        request_id="java-sim-001",
        video_path=video_path,
        start_sec=10.0,
        end_sec=25.0
    )
    
    print("\n[Step 1] gRPC: getModalityClassification")
    response = adapter.get_modality_classification(request)
    
    if not response.success:
        print(f"  ❌ 分类失败: {response.error_message}")
        return False
    
    modality = response.modality
    screenshot_times = list(response.screenshot_times)
    
    print(f"  modality = {modality}")
    print(f"  subtype = {response.knowledge_subtype}")
    print(f"  screenshot_times = {screenshot_times}")
    
    # Step 2: 模拟 Java switch(modality) 调度
    print("\n[Step 2] Java: switch(modality) 调度")
    
    generated_paths = []
    
    if modality == "screenshot":
        print("  → 调用 selectBestFrame()")
        # 模拟截图生成
        ss_time = screenshot_times[0] if screenshot_times else 25.0
        generated_paths.append(f"screenshot_{ss_time:.1f}s.png")
        
    elif modality == "video_only":
        print("  → 调用 extractVideoClip()")
        # 模拟视频生成
        generated_paths.append(f"clip_10.0s-25.0s.mp4")
        
    elif modality == "video_screenshot":
        print("  → 调用 extractVideoClip()")
        generated_paths.append(f"clip_10.0s-25.0s.mp4")
        
        print(f"  → 调用 selectBestFrame() × {len(screenshot_times)}")
        for t in screenshot_times:
            generated_paths.append(f"screenshot_{t:.1f}s.png")
            
    else:  # discard / text_only
        print("  → 跳过素材生成")
    
    # Step 3: 输出结果
    print("\n[Step 3] 生成结果:")
    for path in generated_paths:
        print(f"  📄 {path}")
    
    print("\n✅ 测试3 通过 (Java调用流程模拟完成)")
    return True


def main():
    print("="*60)
    print("V7.x 模态分类 gRPC 集成测试")
    print("="*60)
    
    results = []
    
    # 测试1: 直接调用
    results.append(("直接调用adapter", test_modality_classification_direct()))
    
    # 测试2: 多段落
    results.append(("多段落分类", test_modality_classification_multiple_segments()))
    
    # 测试3: Java流程模拟
    results.append(("Java流程模拟", test_java_workflow_simulation()))
    
    # 汇总
    print("\n" + "="*60)
    print("测试汇总")
    print("="*60)
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status} - {name}")
    
    all_passed = all(r[1] for r in results)
    print(f"\n{'🎉 所有测试通过!' if all_passed else '⚠️ 部分测试失败'}")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    exit(main())
