import grpc
import sys
import os
import time

# 添加项目根目录和 proto 目录
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
proto_dir = os.path.join(current_dir, "proto")
sys.path.insert(0, proto_dir)

# 尝试导入，处理路径问题
try:
    from proto import video_processing_pb2
    from proto import video_processing_pb2_grpc
except ImportError:
    # 如果作为脚本运行，可能需要调整路径
    sys.path.append(os.path.join(current_dir, "MVP_Module2_HEANCING"))
    from proto import video_processing_pb2
    from proto import video_processing_pb2_grpc

def test_validate_cv_batch():
    options = [
        ('grpc.max_send_message_length', 50 * 1024 * 1024),
        ('grpc.max_receive_message_length', 50 * 1024 * 1024)
    ]
    # Local server port 50051 (python_grpc_server.py default)
    print("Connecting to gRPC server at 127.0.0.1:50051...")
    channel = grpc.insecure_channel('127.0.0.1:50051', options=options)
    
    try:
        grpc.channel_ready_future(channel).result(timeout=5)
        print("✅ Channel Connect!")
    except grpc.FutureTimeoutError:
        print("❌ Connect Timeout")
        return

    stub = video_processing_pb2_grpc.VideoProcessingServiceStub(channel)
    
    # 使用存在的视频文件
    video_path = r"d:\videoToMarkdownTest2\find_alg\video_01\downloads\video.mp4"
    if not os.path.exists(video_path):
        print(f"Warning: Video file not found: {video_path}")
        # 尝试使用 sample
        video_path = r"d:\videoToMarkdownTest2\storage\20225626c2a19253c4121f684ecdff12\video.mp4"
    
    print(f"Using video: {video_path}")
    
    # 构造 SemanticUnit
    units = [
        video_processing_pb2.SemanticUnitInput(
            unit_id="test_unit_1",
            start_sec=10.0,
            end_sec=15.0, # Short duration for quick test
            knowledge_type="过程性知识"
        ),
        video_processing_pb2.SemanticUnitInput(
            unit_id="test_unit_2",
            start_sec=30.0,
            end_sec=35.0,
            knowledge_type="概念性知识"
        )
    ]
    
    request = video_processing_pb2.CVValidationRequest(
        task_id="test_task_001",
        video_path=video_path,
        semantic_units=units
    )
    
    print(f"Sending ValidateCVBatch request with {len(units)} units...")
    start_time = time.time()
    try:
        response = stub.ValidateCVBatch(request, timeout=300)
        elapsed = time.time() - start_time
        print(f"✅ ValidateCVBatch completed in {elapsed:.2f}s")
        print(f"Success: {response.success}")
        if not response.success:
            print(f"Error: {response.error_msg}")
        else:
            print(f"Received {len(response.results)} results")
            for res in response.results:
                print(f"  Unit: {res.unit_id}")
                print(f"    Modality: {res.modality}")
                print(f"    Knowledge Subtype: {res.knowledge_subtype}")
                print(f"    Stable Islands: {len(res.stable_islands)}")
                print(f"    Action Segments: {len(res.action_segments)}")
                
    except grpc.RpcError as e:
        print(f"❌ RPC Failed: {e.code()}: {e.details()}")

if __name__ == "__main__":
    test_validate_cv_batch()
