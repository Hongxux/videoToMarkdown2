#!/usr/bin/env python
"""Detailed diagnostic for Java-Python gRPC issues."""
import grpc
import sys
import os
import time

sys.path.insert(0, os.path.dirname(__file__))

import fusion_service_pb2
import fusion_service_pb2_grpc

def test_with_details():
    """Test with detailed connection info."""
    print(f"Testing gRPC connection at 127.0.0.1:50060")
    
    # Use same options that Java might use
    options = [
        ('grpc.keepalive_time_ms', 10000),
        ('grpc.keepalive_timeout_ms', 5000),
        ('grpc.keepalive_permit_without_calls', 1),
        ('grpc.http2.min_time_between_pings_ms', 10000),
        ('grpc.http2.max_pings_without_data', 0),
    ]
    
    channel = grpc.insecure_channel('127.0.0.1:50060', options=options)
    
    # Wait for channel to be ready
    print("Waiting for channel ready state...")
    try:
        grpc.channel_ready_future(channel).result(timeout=10)
        print("✅ Channel is READY!")
    except grpc.FutureTimeoutError:
        print("❌ Channel failed to become ready within 10 seconds")
        return False
    
    stub = fusion_service_pb2_grpc.FusionComputeServiceStub(channel)
    
    # Test Ping
    print("\nTest 1: Ping")
    try:
        response = stub.Ping(fusion_service_pb2.Empty(), timeout=10)
        print(f"✅ Ping: alive={response.alive}")
    except grpc.RpcError as e:
        print(f"❌ Ping failed: {e.code()}: {e.details()}")
        return False
    
    # Test with minimal feature request
    print("\nTest 2: Minimal Feature Request")
    video_path = r"d:\videoToMarkdownTest2\find_alg\video_01\downloads\video.mp4"
    
    request = fusion_service_pb2.FeatureRequest(
        request_id="diag-001",
        video_path=video_path,
        time_range=fusion_service_pb2.TimeRange(start_sec=0.0, end_sec=2.0),
        config=fusion_service_pb2.AnalysisConfig(enable_ocr=False, enable_asr=False),
        segment_text="Test",
    )
    
    start_time = time.time()
    try:
        response = stub.ExtractFeatures(request, timeout=120)
        elapsed = time.time() - start_time
        print(f"✅ ExtractFeatures completed in {elapsed:.2f}s")
        print(f"   Success: {response.success}")
        if not response.success:
            print(f"   Error: {response.error_message}")
        else:
            print(f"   Duration: {response.duration_sec:.2f}s")
    except grpc.RpcError as e:
        elapsed = time.time() - start_time
        print(f"❌ ExtractFeatures failed after {elapsed:.2f}s: {e.code()}: {e.details()}")
        return False
    
    print("\n✅ All diagnostics passed!")
    return True

if __name__ == "__main__":
    test_with_details()
