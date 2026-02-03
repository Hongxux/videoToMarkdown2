import sys
import os
import pickle
import logging
import json
import asyncio
import time
import psutil
import cv2
# Add project root to sys.path to allow importing existing modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

logger = logging.getLogger(__name__)

from collections import OrderedDict
import threading
from typing import Dict, Tuple

class VFEResourceManager:
    """
    Manages VisualFeatureExtractor instances with LRU Caching and Auto-Cleanup.
    Implements Strategy 1 (Stateful) & Strategy 2 (Resource Governance).
    """
    def __init__(self, max_size=20, idle_timeout=300):
        self._cache: OrderedDict[str, VisualFeatureExtractor] = OrderedDict()
        self._last_access: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._max_size = max_size
        self._idle_timeout = idle_timeout
        self._running = True
        
        # Start cleanup daemon
        self._cleaner_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self._cleaner_thread.start()
        logger.info(f"🚀 [Resource] VFE Manager started (Max={max_size}, Timeout={idle_timeout}s)")

    def get_extractor(self, video_path: str):
        with self._lock:
            now = time.time()
            if video_path in self._cache:
                self._cache.move_to_end(video_path)
                self._last_access[video_path] = now
                logger.info(f"♻️ [Resource] VFE Cache HIT: {os.path.basename(video_path)}")
                return self._cache[video_path]
            
            # Evict if full
            if len(self._cache) >= self._max_size:
                dump_path, dump_vfe = self._cache.popitem(last=False)
                del self._last_access[dump_path]
                self._close_vfe(dump_vfe)
                logger.info(f"🧹 [Resource] LRU Evicted: {os.path.basename(dump_path)}")
            
            # Create new
            logger.info(f"🆕 [Resource] VFE Cache MISS: Loading {os.path.basename(video_path)}...")
            vfe = VisualFeatureExtractor(video_path)
            self._cache[video_path] = vfe
            self._last_access[video_path] = now
            return vfe

    def _cleanup_loop(self):
        while self._running:
            time.sleep(60)
            self._prune_idle()

    def _prune_idle(self):
        with self._lock:
            now = time.time()
            to_remove = []
            for path, last_ts in self._last_access.items():
                if now - last_ts > self._idle_timeout:
                    to_remove.append(path)
            
            for path in to_remove:
                vfe = self._cache.pop(path)
                del self._last_access[path]
                self._close_vfe(vfe)
                logger.info(f"🕰️ [Resource] Idle Cleanup: {os.path.basename(path)}")

    def _close_vfe(self, vfe):
        try:
            # Force cleanup if possible. VFE has __del__ but implicit call is better.
            # Explicitly releasing cap if accessible
            if hasattr(vfe, 'cap') and vfe.cap and vfe.cap.isOpened():
                vfe.cap.release()
            
            # If VFE has shared memory registry, maybe clean that too?
            # shm_registry is global, so it handles itself.
        except Exception as e:
            logger.warning(f"Error closing VFE: {e}")

    def shutdown(self):
        self._running = False
        with self._lock:
            for vfe in self._cache.values():
                self._close_vfe(vfe)
            self._cache.clear()
            self._last_access.clear()

from module2_content_enhancement.visual_feature_extractor import VisualFeatureExtractor
from module2_content_enhancement.screenshot_selector import ScreenshotSelector
from module2_content_enhancement.semantic_feature_extractor import SemanticFeatureExtractor
try:
    from module2_content_enhancement.ocr_utils import OCRExtractor
    from module2_content_enhancement.asr_utils import ASRExtractor
except ImportError:
    OCRExtractor = None
    ASRExtractor = None
import fusion_service_pb2
import torch

from module2_content_enhancement.llm_client import LLMClient

class FeatureExtractionAdapter:
    """
    Adapts the legacy VisualFeatureExtractor to the new gRPC Interface.
    Acts as the 'Anti-Corruption Layer' between Proto and Legacy Code.
    """
    
    def __init__(self, redis_client):
        self.redis = redis_client
        self.semantic_extractor = SemanticFeatureExtractor()
        from config import config
        self.config = config
        from module2_content_enhancement.fault_detector import FaultDetector
        self.llm_client = LLMClient()
        self.fault_detector = FaultDetector(llm_client=self.llm_client)
        
        from module2_content_enhancement.confidence_calculator import ConfidenceCalculator
        self.confidence_calculator = ConfidenceCalculator(semantic_extractor=self.semantic_extractor)
        
        from module2_content_enhancement.text_generator import TextGenerator
        self.text_generator = TextGenerator(llm_client=self.llm_client, confidence_calculator=self.confidence_calculator)
        
        from module2_content_enhancement.material_optimizer import GlobalMaterialOptimizer
        self.material_optimizer = GlobalMaterialOptimizer(semantic_extractor=self.semantic_extractor)
        
        from module2_content_enhancement.video_clip_extractor import VideoClipExtractor
        self.video_extractor = VideoClipExtractor(
            visual_extractor=VisualFeatureExtractor, # Pass class or placeholder if needed, usually initialized per video
            llm_client=self.llm_client,
            config=None, # Will load default
            semantic_extractor=self.semantic_extractor
        )
        
        # Initialize OCR/ASR
        self.ocr_extractor = OCRExtractor() if OCRExtractor else None
        self.asr_extractor = ASRExtractor() if ASRExtractor else None
        
        # 💥 Performance: Global JSON Cache to avoid repeated disk I/O in batch tasks
        self._json_cache = {} # path -> (mtime, data)
        self.vfe_manager = VFEResourceManager(max_size=3, idle_timeout=120)


        
        # 💥 Performance: Enforce torch threading limit for parallel microservice stability
        try:
            import torch
            torch.set_num_threads(1)
            logger.info("📡 [PERF] Enforced torch.set_num_threads(1) for Sidecar stability.")
        except:
            pass

    def _load_context(self, config_proto, current_start, current_end):
        """Helper to load context from JSON files specified in the request."""
        # Defensive check for missing context_config
        if not config_proto:
            return {
                "subtitles": [],
                "merged_segments": [],
                "context_before": "",
                "context_after": "",
                "main_topic": "Algorithm"
            }
        
        ctx = {
            "subtitles": [],
            "merged_segments": [],
            "context_before": "",
            "context_after": "",
            "main_topic": getattr(config_proto, 'main_topic', None) or "Algorithm"
        }
        
        # 1. Load Subtitles (with Cache)
        subtitles_data = self._get_cached_json(config_proto.subtitles_path)
        if subtitles_data:
            try:
                from module2_content_enhancement.data_structures import CorrectedSubtitle
                items = subtitles_data.get("output", {}).get("corrected_subtitles", [])
                if not items and isinstance(subtitles_data, list):
                    items = subtitles_data
                    
                for item in items:
                    sub = CorrectedSubtitle(
                        subtitle_id=item.get('subtitle_id', '0'),
                        text=item.get('corrected_text', item.get('text', "")),
                        start_sec=item.get('start_sec', 0.0),
                        end_sec=item.get('end_sec', 0.0)
                    )
                    ctx["subtitles"].append(sub)
                    
                    if sub.end_sec < current_start:
                        ctx["context_before"] += sub.text + " "
                    elif sub.start_sec > current_end:
                        ctx["context_after"] += sub.text + " "
            except Exception as e:
                logger.warning(f"Failed to parse context subtitles: {e}")

        # 2. Load Merged Segments (with Cache)
        merge_data = self._get_cached_json(config_proto.merge_data_path)
        if merge_data:
            try:
                segments_list = merge_data.get("output", {}).get("pure_text_script", [])
                from module2_content_enhancement.data_structures import CrossSentenceMergedSegment
                for s in segments_list:
                    ctx["merged_segments"].append(CrossSentenceMergedSegment(
                        segment_id=s.get("paragraph_id", "unknown"),
                        full_text=s.get("text", ""),
                        source_sentence_ids=s.get("source_sentence_ids", []),
                        merge_type=s.get("merge_type", "无合并")
                    ))
            except Exception as e:
                logger.warning(f"Failed to parse merge data: {e}")
        
        return ctx

    def _get_cached_json(self, path):
        """Helper to get cached JSON if mtime hasn't changed."""
        if not path or not os.path.exists(path):
            return None
        
        mtime = os.path.getmtime(path)
        if path in self._json_cache:
            cached_mtime, data = self._json_cache[path]
            if cached_mtime == mtime:
                return data
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self._json_cache[path] = (mtime, data)
                logger.info(f"📁 [PERF] Cached JSON: {os.path.basename(path)}")
                return data
        except Exception as e:
            logger.warning(f"Cache load failed for {path}: {e}")
            return None

    def process(self, request) -> fusion_service_pb2.FeatureResponse:
        video_path = request.video_path
        
        # 1. Validation
        if not os.path.exists(video_path):
            return fusion_service_pb2.FeatureResponse(
                request_id=request.request_id,
                success=False,
                error_message=f"File not found: {video_path}"
            )

        t0 = time.time()
        try:
            # 2. Initialize Legacy Extractor
            # Note: In a real prod env, we might pool these extractors if initialization is heavy (loading models)
            # 🚀 OPTIMIZATION: Use cached instance via Manager
            extractor = self.vfe_manager.get_extractor(video_path)
            
            # 3. Determine Time Range
            start = 0.0
            end = extractor.duration
            if request.HasField("time_range"):
                start = request.time_range.start_sec
                end = request.time_range.end_sec
                
            # 4. Execute Logic (The "Soldier" works)
            features = asyncio.run(extractor.extract_visual_features(start, end))
            
            elapsed_vis = (time.time() - t0) * 1000
            logger.info(f"🚀 [PERF] extract_visual_features for {start:.1f}s-{end:.1f}s completed in {elapsed_vis:.1f}ms")
            
            # 4.5. Extract Text (Real implementation replacing mocks)
            ocr_text = ""
            asr_text = ""
            if request.config.enable_ocr and self.ocr_extractor:
                 # Extract a representative frame (middle of segment) for OCR
                 mid_time = (start + end) / 2
                 cap = cv2.VideoCapture(video_path)
                 cap.set(cv2.CAP_PROP_POS_MSEC, mid_time * 1000)
                 ret, frame = cap.read()
                 if ret:
                      ocr_text = self.ocr_extractor.extract_text_from_frame(frame)
                 cap.release()
            
            if request.config.enable_asr and self.asr_extractor:
                 # ASRExtractor.extract_audio_text is async
                 asr_text = asyncio.run(self.asr_extractor.extract_audio_text(video_path, start, end))

            # 4.6 Extract Semantic Features
            # 🚀 ASR Integration: Use transcribed text from request if available
            transcription = getattr(request, 'segment_text', "")
            if not transcription:
                 transcription = asr_text if asr_text else "General technical explanation"

            # Parse Indicators
            class1_inds = {}
            class2_inds = {}
            if request.config.indicator_config_json:
                try:
                    inds = json.loads(request.config.indicator_config_json)
                    class1_inds = inds.get('class1', {})
                    class2_inds = inds.get('class2', {})
                except Exception as e:
                    logger.warning(f"Failed to parse indicator config: {e}")

            # Load Context from Files if specified
            context_config = getattr(request, 'context_config', None)
            file_ctx = self._load_context(context_config, start, end)
            context_before = request.context_before if request.context_before else file_ctx["context_before"]
            context_after = request.context_after if request.context_after else file_ctx["context_after"]

            sem_feat = asyncio.run(self.semantic_extractor.extract_semantic_features(
                text=transcription,
                context_before=context_before,
                context_after=context_after,
                domain="algorithm",
                class1_indicators=class1_inds, 
                class2_indicators=class2_inds
            ))

            proto_semantic = fusion_service_pb2.SemanticFeatures(
                knowledge_type=sem_feat.knowledge_type,
                context_similarity=sem_feat.context_similarity,
                domain_consistency=sem_feat.domain_consistency,
                matched_keywords=sem_feat.matched_keywords,
                has_sequence_pattern=sem_feat.has_sequence_pattern,
                has_hierarchy_pattern=sem_feat.has_hierarchy_pattern,
                has_process_words=sem_feat.has_process_words,
                has_spatial_words=sem_feat.has_spatial_words,
                confidence=sem_feat.confidence
            )

            # 5. Map Result to Proto (Strict Parity)
            proto_visual = fusion_service_pb2.VisualFeatures(
                avg_mse=features.avg_mse,
                avg_ssim=getattr(features, 'avg_ssim', 0.0),
                is_potential_dynamic=features.is_dynamic,
                element_count=features.element_count,
                # New fields for Strict Parity
                visual_type=getattr(features, 'visual_type', 'mixed'),
                action_density=getattr(features, 'action_density', 0.0),
                has_math_formula=getattr(features, 'has_math_formula', False),
                has_static_visual_structure=getattr(features, 'has_static_visual_structure', False),
                visual_confidence=getattr(features, 'confidence', 0.8),
                segment_start=start,
                segment_end=end,
                ocr_full_text=ocr_text,
                asr_segment_text=asr_text,
                
                # 🚀 Phase 7.0: Advanced Metrics
                animation_end_time=getattr(features, 'animation_end_time', 0.0),
                ssim_seq=getattr(features, 'ssim_seq', []),
                avg_edge_flux=getattr(features, 'avg_edge_flux', 0.0)
            )
            
            # 6. Store Heavy Data to Redis (The "Reference Passing")
            redis_key = f"task:{request.request_id}:features"
            try:
                if self.redis:
                    self.redis.set(redis_key, pickle.dumps(features))
            except Exception as e:
                logger.warning(f"Redis save failed: {e}. Proceeding without persistence.")
            
            # 7. Construct Response
            return fusion_service_pb2.FeatureResponse(
                request_id=request.request_id,
                success=True,
                duration_sec=extractor.duration,
                visual_features=proto_visual,
                semantic_features=proto_semantic,
                result_redis_key=redis_key
            )
            
        except Exception as e:
            logger.error(f"Extraction failed: {e}", exc_info=True)
            return fusion_service_pb2.FeatureResponse(
                request_id=request.request_id,
                success=False,
                error_message=str(e)
            )

    def detect_faults(self, request) -> fusion_service_pb2.DetectFaultsResponse:
        """
        Adapts the complex FaultDetector to gRPC.
        Constructs minimal data structures required by FaultDetector logic.
        """
        try:
             # 1. Reconstruct Data Objects from Proto
             # We need to trick FaultDetector into thinking it has full Segments
             # 1. Reconstruct Subtitles
             # 2. Reconstruct Segment
             from module2_content_enhancement.data_structures import CorrectedSubtitle, CrossSentenceMergedSegment, FaultClass, EnhancementType
             from module2_content_enhancement.fault_detector import FaultCandidate
             
             # Load Context from Files
             context_config = getattr(request, 'context_config', None)
             file_ctx = self._load_context(context_config, 
                                          request.visual_features.segment_start, 
                                          request.visual_features.segment_end)
             subtitles = file_ctx["subtitles"]
             
             if not subtitles:
                 # Fallback to proto context if file failed
                 from module2_content_enhancement.data_structures import CorrectedSubtitle
                 for item in request.subtitle_context:
                     subtitles.append(CorrectedSubtitle(
                         subtitle_id=str(item.index),
                         text=item.text,
                         start_sec=item.start,
                         end_sec=item.end
                     ))
             
             # Reconstruct segment properly
             segment = CrossSentenceMergedSegment(
                 segment_id=request.request_id,
                 full_text=request.fault_text,
                 source_sentence_ids=[]
             )
             
              # Now using REAL subtitles and topic
             main_topic = request.main_topic if request.main_topic else file_ctx["main_topic"]
             faults = asyncio.run(self.fault_detector.detect_faults(
                 corrected_subtitles=subtitles, 
                 merged_segments=file_ctx["merged_segments"] if file_ctx["merged_segments"] else [segment],
                 main_topic=main_topic
             ))
             
             if faults:
                 f = faults[0]
                 return fusion_service_pb2.DetectFaultsResponse(
                     has_fault=True,
                     fault_type=str(f.fault_class),
                     detection_reason=f.detection_reason,
                     suggested_enhancement_text=str(f.suggested_enhancement)
                 )
             
             return fusion_service_pb2.DetectFaultsResponse(has_fault=False)

        except Exception as e:
            logger.error(f"Fault Detection Failed: {e}", exc_info=True)
            return fusion_service_pb2.DetectFaultsResponse(has_fault=False, detection_reason=str(e))

    def select_best_frame(self, request) -> fusion_service_pb2.FrameSelectionResponse:
        """
        Delegates to ScreenshotSelector logic
        """
        try:
            # 🚀 Feature Parity: ScreenshotSelector requires VisualFeatureExtractor and config
            # Create a localized VFE for this specific video
            from module2_content_enhancement.visual_feature_extractor import VisualFeatureExtractor
            from module2_content_enhancement.screenshot_selector import ScreenshotSelector
            
            # Use cached local VFE or create new
            vfe = self.vfe_manager.get_extractor(request.video_path)
            
            selector = ScreenshotSelector(vfe, self.config)
            
            # Execute logic
            selection = asyncio.run(selector.select_screenshot(
                request.video_path,
                request.start_sec,
                request.end_sec,
                output_dir="output/screenshots" # 🚩 Ensure output dir is passed
            ))
            
            return fusion_service_pb2.FrameSelectionResponse(
                success=True,
                best_frame_path=selection.screenshot_path if selection else "",
                timestamp=selection.selected_timestamp if selection else 0.0,
                score=selection.final_score if selection else 0.0
            )
        except Exception as e:
            logger.error(f"Screenshot Selection Failed: {e}", exc_info=True)
            return fusion_service_pb2.FrameSelectionResponse(success=False, error_message=str(e))

    def compute_frame_hash(self, request) -> fusion_service_pb2.FrameHashResponse:
        """
        Computes dHash/pHash for material optimization
        """
        try:
            import cv2
            import imagehash
            from PIL import Image
            
            result_map = {}
            cap = cv2.VideoCapture(request.video_path)
            
            for ts in request.timestamps:
                cap.set(cv2.CAP_PROP_POS_MSEC, ts * 1000)
                ret, frame = cap.read()
                if ret:
                    # Convert BGR to RGB for PIL
                    img = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
                    dhash = str(imagehash.dhash(img))
                    result_map[str(ts)] = dhash
            
            cap.release()
            return fusion_service_pb2.FrameHashResponse(success=True, timestamp_to_hash=result_map)
            
        except Exception as e:
             logger.error(f"Hash Compute Failed: {e}", exc_info=True)
             return fusion_service_pb2.FrameHashResponse(success=False, error_message=str(e))

    def generate_enhancement_text(self, request) -> fusion_service_pb2.GenerateTextResponse:
        """
        Adapts TextGenerator logic.
        """
        try:
             # Reconstruct SemanticFeatures obj
             from module2_content_enhancement.semantic_feature_extractor import SemanticFeatures
             
             sem = SemanticFeatures(
                 knowledge_type=request.semantic_features.knowledge_type,
                 context_similarity=request.semantic_features.context_similarity,
                 domain_consistency=request.semantic_features.domain_consistency,
                 matched_keywords=list(request.semantic_features.matched_keywords),
                 has_sequence_pattern=request.semantic_features.has_sequence_pattern,
                 has_hierarchy_pattern=request.semantic_features.has_hierarchy_pattern,
                has_process_words=request.semantic_features.has_process_words,
                 has_spatial_words=request.semantic_features.has_spatial_words,
                 confidence=request.semantic_features.confidence
             )
             
              # Using asyncio to call async generator
             # Load context from files
             context_config = getattr(request, 'context_config', None)
             file_ctx = self._load_context(context_config, 
                                          request.visual_features.segment_start, 
                                          request.visual_features.segment_end)
             
             from module2_content_enhancement.data_structures import FaultClass, EnhancementType, CrossSentenceMergedSegment
             from module2_content_enhancement.fault_detector import FaultCandidate
             
             # Reconstruct segment for generation
             segment = CrossSentenceMergedSegment(
                 segment_id=request.request_id,
                 full_text=request.visual_features.asr_segment_text,
                 source_sentence_ids=[]
             )
             
             # Reconstruct FaultCandidate
             fault_candidate = FaultCandidate(
                 fault_id=request.request_id,
                 fault_class=FaultClass(int(request.fault_type)) if request.fault_type.isdigit() else FaultClass.CLASS_2,
                 source_subtitle_ids=[],
                 source_segment_id=request.request_id,
                 timestamp_start=request.visual_features.segment_start,
                 timestamp_end=request.visual_features.segment_end,
                 fault_text=request.visual_description, # GenerateTextRequest uses visual_description for specific fault text
                 context_before=file_ctx["context_before"],
                 context_after=file_ctx["context_after"],
                 detection_reason="Remote Trigger",
                 detection_confidence=0.9,
                 suggested_enhancement=EnhancementType.TEXT
             )

             text_obj = asyncio.run(self.text_generator.generate_supplement(
                 fault_candidate=fault_candidate,
                 merged_segment=segment,
                 corrected_subtitles=file_ctx["subtitles"],
                 context_before=file_ctx["context_before"],
                 context_after=file_ctx["context_after"],
                 domain=request.domain,
                 domain_keywords={}, # Optional in core logic
             ))
             
             text = text_obj.generated_text
             
             return fusion_service_pb2.GenerateTextResponse(success=True, generated_text=text)
        except Exception as e:
             logger.error(f"Text Gen Failed: {e}", exc_info=True)
             return fusion_service_pb2.GenerateTextResponse(success=False, generated_text=f"Error: {e}")

    def optimize_materials(self, request) -> fusion_service_pb2.OptimizeMaterialsResponse:
        """
        Adapts GlobalMaterialOptimizer.
        """
        try:
             from module2_content_enhancement.data_structures import Enhancement, EnhancementType
              
             # Convert Protos to Python Data Structures
             enhancements = []
             for cand in request.candidates:
                 e = Enhancement(
                     enhancement_id=cand.enhancement_id,
                     original_segment_id="unknown",
                     timestamp_start=cand.timestamp_start,
                     timestamp_end=cand.timestamp_end,
                     enhancement_type=EnhancementType(cand.enhancement_type) if cand.enhancement_type else EnhancementType.SCREENSHOT,
                     fault_text=cand.fault_text,
                     media_paths=list(cand.media_paths),
                     suggested_text="",
                     confidence_score=1.0
                 )
                 enhancements.append(e)
                 
             # Execute Optimization
             optimized = asyncio.run(self.material_optimizer.optimize_enhancements(enhancements))
             
             # Collect Kept IDs (those with media_paths not empty)
             kept_ids = []
             reasons = {}
             
             for e in optimized:
                 if e.media_paths:
                     kept_ids.append(e.enhancement_id)
                 else:
                     reasons[e.enhancement_id] = e.material_error
                     
             return fusion_service_pb2.OptimizeMaterialsResponse(
                 success=True,
                 kept_enhancement_ids=kept_ids,
                 redundancy_reasons=reasons
             )
        except Exception as e:
             logger.error(f"Optimization Failed: {e}", exc_info=True)
             return fusion_service_pb2.OptimizeMaterialsResponse(success=False, error_message=str(e))

    def extract_video_clip(self, request) -> fusion_service_pb2.VideoClipResponse:
        """
        Adapts VideoClipExtractor.
        """
        try:
              # VideoClipExtractor needs a real visual_extractor instance tied to the video
             from module2_content_enhancement.visual_feature_extractor import VisualFeatureExtractor
             from module2_content_enhancement.video_clip_extractor import VideoClipExtractor # Added this import
             
             v_extractor = self.vfe_manager.get_extractor(request.video_path)
             
             # Re-initialize local extractor instance for this call
             
             # Re-initialize local extractor instance for this call
             # Assuming self.video_extractor is an instance of VideoClipExtractor
             # and it needs its visual_extractor updated.
             # If self.video_extractor is not yet initialized, this would fail.
             # A more robust approach might be to instantiate VideoClipExtractor here
             # if it's not meant to be a persistent class member.
             # For now, following the provided code's assumption.
             if not hasattr(self, 'video_extractor') or not isinstance(self.video_extractor, VideoClipExtractor):
                 self.video_extractor = VideoClipExtractor(v_extractor)
             else:
                 self.video_extractor.visual_extractor = v_extractor
             
             # Load context for subtitles
             context_config = getattr(request, 'context_config', None)
             file_ctx = self._load_context(context_config, request.start_sec, request.end_sec)
             self.video_extractor.set_subtitles(file_ctx["subtitles"])
             
             clip = asyncio.run(self.video_extractor.extract_video_clip(
                 timestamp_start=request.start_sec,
                 timestamp_end=request.end_sec,
                 output_dir=request.output_dir if request.output_dir else None,
                 video_path=request.video_path
             ))
             
             if clip:
                 return fusion_service_pb2.VideoClipResponse(
                     success=True,
                     clip_path=clip.clip_path,
                     duration=clip.extended_end - clip.extended_start
                 )
             else:
                 return fusion_service_pb2.VideoClipResponse(success=False, error_message="Clip extraction returned None (filter/overlap)")
                 
        except Exception as e:
             logger.error(f"Clip Extraction Failed: {e}", exc_info=True)
             return fusion_service_pb2.VideoClipResponse(success=False, error_message=str(e))

    # =========================================================================
    # V7.x: Modality Classification Adapter
    # =========================================================================
    
    def get_modality_classification(self, request):
        """
        V7.x 模态分类适配器
        
        调用 CVKnowledgeValidator 进行视觉状态检测和模态分类。
        截图/视频生成复用现有 SelectBestFrame / ExtractVideoClip RPC。
        """
        from module2_content_enhancement.cv_knowledge_validator import CVKnowledgeValidator
        
        video_path = request.video_path
        start_sec = request.start_sec
        end_sec = request.end_sec
        
        try:
            with CVKnowledgeValidator(video_path) as validator:
                # CV状态检测
                stable_islands, action_units, redundancy = validator.detect_visual_states(
                    start_sec, end_sec
                )
                
                # 主动作判定
                modality = "text_only"
                knowledge_subtype = "no_visual"
                screenshot_times = []
                action_segments = []
                
                if action_units:
                    primary_action = max(action_units, key=lambda a: a.duration_ms)
                    modality = primary_action.modality
                    knowledge_subtype = primary_action.knowledge_subtype
                    
                    if primary_action.modality == "video_screenshot":
                        screenshot_times = validator._extract_key_screenshot_times(primary_action)
                    
                    for a in action_units:
                        action_segments.append(fusion_service_pb2.ActionSegmentProto(
                            start_sec=a.start_sec,
                            end_sec=a.end_sec,
                            modality=a.modality or "",
                            subtype=a.knowledge_subtype or "",
                            duration_ms=int(a.duration_ms),  # proto expects int32
                            ssim_drop=float(a.ssim_drop) if a.ssim_drop else 0.0
                        ))
                        
                elif stable_islands:
                    modality = "screenshot"
                    knowledge_subtype = "stable"
                    longest_stable = max(stable_islands, key=lambda s: s.duration_ms)
                    screenshot_times = [(longest_stable.start_sec + longest_stable.end_sec) / 2]
                
                return fusion_service_pb2.ModalityClassificationResponse(
                    success=True,
                    modality=modality,
                    knowledge_subtype=knowledge_subtype,
                    screenshot_times=screenshot_times,
                    action_segments=action_segments,
                    stable_island_count=len(stable_islands),
                    action_unit_count=len(action_units)
                )
                
        except Exception as e:
            logger.error(f"Modality Classification Failed: {e}", exc_info=True)
            return fusion_service_pb2.ModalityClassificationResponse(
                success=False,
                error_message=str(e)
            )
