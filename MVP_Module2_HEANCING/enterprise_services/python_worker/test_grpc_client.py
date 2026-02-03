#!/usr/bin/env python
"""Simple gRPC client to test the Python Worker directly."""
import grpc
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

import fusion_service_pb2
import fusion_service_pb2_grpc

def test_ping():
    """Test the Ping RPC to verify gRPC connectivity."""
    channel = grpc.insecure_channel('127.0.0.1:50060')
    stub = fusion_service_pb2_grpc.FusionComputeServiceStub(channel)
    
    try:
        response = stub.Ping(fusion_service_pb2.Empty())
        print(f"✅ Ping successful! Alive: {response.alive}, CPU Load: {response.cpu_load}")
        return True
    except grpc.RpcError as e:
        print(f"❌ Ping failed: {e.code()}: {e.details()}")
        return False

def test_feature_extraction(video_path):
    """Test the ExtractFeatures RPC."""
    channel = grpc.insecure_channel('127.0.0.1:50060')
    stub = fusion_service_pb2_grpc.FusionComputeServiceStub(channel)
    
    request = fusion_service_pb2.FeatureRequest(
        request_id="test-001",
        video_path=video_path,
        time_range=fusion_service_pb2.TimeRange(start_sec=0.0, end_sec=5.0),
        config=fusion_service_pb2.AnalysisConfig(enable_ocr=False),
        segment_text="Test transcription",
        context_config=fusion_service_pb2.ContextConfig(
            subtitles_path="",
            merge_data_path="",
            main_topic="Algorithm"
        )
    )
    
    try:
        response = stub.ExtractFeatures(request, timeout=60)
        print(f"✅ ExtractFeatures successful!")
        print(f"   Success: {response.success}")
        print(f"   Duration: {response.duration_sec:.2f}s")
        if response.visual_features:
            print(f"   AvgMSE: {response.visual_features.avg_mse:.2f}")
            print(f"   IsDynamic: {response.visual_features.is_potential_dynamic}")
        return True
    except grpc.RpcError as e:
        print(f"❌ ExtractFeatures failed: {e.code()}: {e.details()}")
        return False

if __name__ == "__main__":
    print("🔍 Testing gRPC connection to Python Worker...\n")
    
    # Test 1: Ping
    print("Test 1: Ping")
    if not test_ping():
        print("Ping failed, aborting further tests.")
        sys.exit(1)
    
    # Test 2: Feature Extraction
    print("\nTest 2: Feature Extraction")
    video_path = r"d:\videoToMarkdownTest2\find_alg\video_01\downloads\video.mp4"
    if os.path.exists(video_path):
        test_feature_extraction(video_path)
    else:
        print(f"Video not found: {video_path}")
    
    print("\n✅ All tests completed!")
