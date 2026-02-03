import grpc
from concurrent import futures
import time
import logging
import sys
import os

# Adjust path to find protos
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

try:
    import fusion_service_pb2
    import fusion_service_pb2_grpc
except ImportError:
    print("Error: Proto files not found. Please run 'python -m grpc_tools.protoc ...' first.")
    # For now, we mock them to avoid runtime crash during file creation, 
    # but user needs to generate them.
    fusion_service_pb2 = None
    fusion_service_pb2_grpc = None

from config import config
from service_adapter import FeatureExtractionAdapter

# Mock Redis for Demo if not available
class MockRedis:
    def set(self, key, value): print(f"[Redis Mock] SET {key} = <{len(value)} bytes>")
    def get(self, key): return None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FusionWorker")

class FusionServiceServicer(fusion_service_pb2_grpc.FusionComputeServiceServicer if fusion_service_pb2_grpc else object):
    def __init__(self):
        # Initialize Redis
        try:
            import redis
            self.redis = redis.from_url(config.REDIS_URL)
        except ImportError:
            logger.warning("redis-py not installed, using MockRedis")
            self.redis = MockRedis()
            
        self.adapter = FeatureExtractionAdapter(self.redis)

    def ExtractFeatures(self, request, context):
        logger.info(f"Received Request: {request.request_id} for {request.video_path}")
        start_time = time.time()
        
        response = self.adapter.process(request)
        
        cost = time.time() - start_time
        logger.info(f"Processed Request: {request.request_id} in {cost:.2f}s | Success: {response.success}")
        return response

    def ExtractFeaturesBatch(self, request, context):
        """
        🚀 Batch Processing Strategy:
        Receive multiple segment requests in one call.
        Process them sequentially (to maintain stability) but reuse resources via VFEResourceManager.
        """
        batch_id = f"Batch-{int(time.time())}"
        count = len(request.requests)
        logger.info(f"📦 Received Batch Request: {batch_id} with {count} segments")
        
        responses = []
        start_time = time.time()
        
        for i, req in enumerate(request.requests):
            logger.info(f"  > Processing Batch Item {i+1}/{count}: {req.request_id}")
            try:
                # Reuse the existing adapter logic which uses VFEResourceManager
                res = self.adapter.process(req)
                responses.append(res)
            except Exception as e:
                logger.error(f"  ❌ Batch Item {i+1} Failed: {e}")
                # Create a failure response for this item
                err_res = fusion_service_pb2.FeatureResponse(
                    request_id=req.request_id,
                    success=False,
                    error_message=str(e)
                )
                responses.append(err_res)
                
        total_cost = time.time() - start_time
        logger.info(f"✅ Batch {batch_id} Completed in {total_cost:.2f}s")
        return fusion_service_pb2.BatchFeatureResponse(responses=responses)

    def Ping(self, request, context):
        return fusion_service_pb2.HealthStatus(
            alive=True,
            cpu_load="Low",
            gpu_memory_usage="0MB" # Placeholder
        )

    def SelectBestFrame(self, request, context):
        return self.adapter.select_best_frame(request)

    def ComputeFrameHash(self, request, context):
        return self.adapter.compute_frame_hash(request)

    def DetectFaults(self, request, context):
        return self.adapter.detect_faults(request)

    def GenerateEnhancementText(self, request, context):
        return self.adapter.generate_enhancement_text(request)

    def OptimizeMaterials(self, request, context):
        return self.adapter.optimize_materials(request)

    def ExtractVideoClip(self, request, context):
        return self.adapter.extract_video_clip(request)

    # =========================================================================
    # V7.x: Modality Classification RPC Handlers
    # =========================================================================
    
    def GetModalityClassification(self, request, context):
        """
        V7.x 模态分类 RPC
        
        调用 CVKnowledgeValidator 进行视觉状态检测和模态分类。
        Java Orchestrator 根据返回的 modality 决定生成哪种素材。
        """
        logger.info(f"V7.x ModalityClassification: {request.video_path} [{request.start_sec:.1f}s-{request.end_sec:.1f}s]")
        start_time = time.time()
        
        try:
            response = self.adapter.get_modality_classification(request)
            cost = time.time() - start_time
            logger.info(f"ModalityClassification completed in {cost:.2f}s: modality={response.modality}")
            return response
        except Exception as e:
            logger.error(f"ModalityClassification failed: {e}")
            return fusion_service_pb2.ModalityClassificationResponse(
                success=False,
                error_message=str(e)
            )
    
    def GenerateScreenshot(self, request, context):
        """
        生成指定时间点的截图 - 复用 SelectBestFrame 逻辑
        """
        logger.info(f"GenerateScreenshot: {request.video_path} @ {request.timestamp:.2f}s")
        
        # 转换为 FrameSelectionRequest 格式
        frame_request = fusion_service_pb2.FrameSelectionRequest(
            request_id=request.request_id,
            video_path=request.video_path,
            start_sec=request.timestamp - 0.1,  # 微小窗口
            end_sec=request.timestamp + 0.1
        )
        
        frame_response = self.adapter.select_best_frame(frame_request)
        
        return fusion_service_pb2.GenerateScreenshotResponse(
            success=frame_response.success,
            error_message=frame_response.error_message,
            screenshot_path=frame_response.best_frame_path
        )



def serve():
    if not fusion_service_pb2_grpc:
        logger.error("Protobufs missing. Aborting.")
        return

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=config.MAX_WORKERS))
    fusion_service_pb2_grpc.add_FusionComputeServiceServicer_to_server(FusionServiceServicer(), server)
    
    address = f'0.0.0.0:{config.GRPC_PORT}'
    server.add_insecure_port(address)
    logger.info(f"🚀 Python Worker started on {address}")
    logger.info(f"   Max Workers: {config.MAX_WORKERS}")
    logger.info(f"   Redis: {config.REDIS_URL}")
    
    server.start()
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
