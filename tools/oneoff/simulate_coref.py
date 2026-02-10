
import sys
import os
import asyncio
import logging
from pathlib import Path
from dataclasses import dataclass

from services.python_grpc.src.config_paths import load_yaml_dict, resolve_video_config_path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Simulation")

try:
    from services.python_grpc.src.content_pipeline.coreference_resolver import CoreferenceResolver
    from services.python_grpc.src.content_pipeline.phase2a.segmentation.concrete_knowledge_validator import ConcreteKnowledgeValidator
    from services.python_grpc.src.transcript_pipeline.llm.client import create_llm_client
except ImportError as e:
    logger.error(f"Failed to import modules: {e}")
    sys.exit(1)

# Mock Classes
@dataclass
class MockUnit:
    full_text: str
    start_sec: float = 0.0
    end_sec: float = 10.0
    source_sentence_ids: list = None
    unit_id: str = "SU001"

    def __post_init__(self):
        if self.source_sentence_ids is None:
            self.source_sentence_ids = ["S001"]

@dataclass
class MockSubtitle:
    text: str
    start_sec: float
    end_sec: float
    subtitle_id: str = "S001"

@dataclass
class MockRequest:
    timestamp_sec: float
    semantic_unit_id: str
    screenshot_id: str

@dataclass
class MockMaterialRequests:
    screenshot_requests: list

async def main():
    logger.info("Starting CoreferenceResolver Simulation...")
    
    # 1. Config & Clients
    config_path = resolve_video_config_path(anchor_file=__file__)
    if not config_path or not config_path.exists():
        logger.error(f"Config not found at {config_path}")
        return

    try:
        # Create LLM Client (DeepSeek)
        # Assuming environment variable DEEPSEEK_API_KEY is set or config has it.
        # We can try to set it manually if missing for simulation purposes if the user provided it elsewhere, 
        # but for now rely on existing env/config.
        if "DEEPSEEK_API_KEY" not in os.environ:
             # Try to load from config directly to check if valid
             cfg = load_yaml_dict(config_path)
             if not cfg.get('ai', {}).get('api_key'):
                 logger.warning("DEEPSEEK_API_KEY not found in env or config. Simulation might fail.")
        
        llm_client = create_llm_client(config_path=str(config_path), purpose="analysis")
        logger.info("LLM Client created.")
    except Exception as e:
        logger.error(f"Failed to create LLM client: {e}")
        return

    try:
        # Create Vision Validator
        validator = ConcreteKnowledgeValidator(config_path=str(config_path), output_dir=str(project_root / "temp_output"))
        # Disable cache to force fresh run
        validator._hash_cache = None 
        logger.info("ConcreteKnowledgeValidator created.")
    except Exception as e:
        logger.error(f"Failed to create Validator: {e}")
        return

    # 2. Initialize Resolver
    # Set confidence_threshold=1.0 ensures we ALWAYS try vision if there's a gap, to test the flow.
    # In reality, DeepSeek might give 0.9, so 1.0 is safe for testing "Vision fallback".
    resolver = CoreferenceResolver(
        llm_client=llm_client,
        concrete_validator=validator,
        # confidence_threshold=1.0 
    )

    # 3. Prepare Test Data
    # A sentence with clear ambiguous references
    test_text = "鐐瑰嚮杩欎釜鎸夐挳锛屽彲浠ョ湅鍒拌缁嗕俊鎭€傜劧鍚庨€夋嫨閭ｄ竴椤归厤缃€?
    # Corresponding subtitles
    subtitles = [
        MockSubtitle("鐐瑰嚮杩欎釜鎸夐挳锛屽彲浠ョ湅鍒拌缁嗕俊鎭€?, 0.0, 5.0, "S001"),
        MockSubtitle("鐒跺悗閫夋嫨閭ｄ竴椤归厤缃€?, 5.0, 10.0, "S002")
    ]
    
    unit = MockUnit(full_text=test_text, start_sec=0.0, end_sec=10.0, source_sentence_ids=["S001", "S002"])
    
    sentence_timestamps = {
        "S001": {"start_sec": 0.0, "end_sec": 5.0},
        "S002": {"start_sec": 5.0, "end_sec": 10.0}
    }
    
    # Prepare Screenshots
    # Use an existing screenshot from storage to simulate "found screenshot"
    screenshot_dir = project_root / "storage" / "99efb7c15a9121f4e29113821d5c9c73" / "screenshots"
    # Pick a real file
    existing_image = None
    if screenshot_dir.exists():
        pngs = list(screenshot_dir.glob("*.png"))
        if pngs:
            existing_image = pngs[0].name
            logger.info(f"Using existing image for simulation: {existing_image}")
    
    requests = []
    if existing_image:
        requests.append(MockRequest(2.0, "SU001", existing_image))
        requests.append(MockRequest(7.0, "SU001", existing_image)) # Add for S002 coverage
        
    material_requests = MockMaterialRequests(screenshot_requests=requests)

    # 4. Run Resolution
    logger.info(f"\nResolving text: '{test_text}'")
    
    try:
        result = await resolver.resolve_unit_coreference(
            unit=unit,
            material_requests=material_requests,
            screenshots_dir=str(screenshot_dir),
            sentence_timestamps=sentence_timestamps,
            subtitles=subtitles,
            video_path="dummy.mp4" # Not needed if we hit existing screenshot cache
        )

        
        # 5. Output Results
        print("\n" + "="*50)
        print("SIMULATION RESULTS")
        print("="*50)
        print(f"Original Text: {test_text}")
        print(f"Updated Text : {result.updated_text}")
        print("-" * 30)
        print(f"Details ({len(result.gaps)} gaps detected):")
        
        for gap in result.gaps:
             print(f"\nGap ID: {gap.gap_id}")
             print(f"  Sentence: '{gap.sentence_text}'")
             print(f"  DeepSeek Replaced: '{gap.deepseek_replaced_text}' (Conf: {gap.deepseek_confidence})")
             print(f"  Final Replaced   : '{gap.final_replaced_text}' (Conf: {gap.final_confidence})")
             print(f"  Source: {gap.source}")
             print(f"  Reason: {gap.reason}")
             
        print("="*50 + "\n")

    except Exception as e:
        logger.error(f"Resolution failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())


