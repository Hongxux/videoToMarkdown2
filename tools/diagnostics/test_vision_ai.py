import os
import sys
import logging
import asyncio
from pathlib import Path

from services.python_grpc.src.config_paths import load_yaml_dict, resolve_video_config_path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("VisionAITest")

# Use a real prompt for test
TEST_PROMPT = """# Concrete Knowledge Check
Is this image containing concrete knowledge (diagrams, real-world photos, UI screenshots, etc.)?
Output STRICT JSON:
{
    "has_concrete_knowledge": "鏄?鍚?,
    "confidence": 0.0-1.0,
    "concrete_type": "type",
    "reason": "reason"
}
"""

async def run_async_test():
    # Load config
    config_path = resolve_video_config_path(anchor_file=__file__)
    if not config_path or not config_path.exists():
        logger.error(f"Config file not found at {config_path}")
        return

    config_data = load_yaml_dict(config_path)
    
    vision_config_data = config_data.get("vision_ai", {})
    if not vision_config_data.get("enabled"):
        logger.error("Vision AI disabled in config.")
        return

    # Initialize Client directly
    from services.python_grpc.src.content_pipeline.infra.llm.vision_ai_client import VisionAIClient, VisionAIConfig
    
    config = VisionAIConfig(
        enabled=True,
        bearer_token=vision_config_data.get("bearer_token", ""),
        base_url=vision_config_data.get("base_url", ""),
        model=vision_config_data.get("model", "ernie-4.5-turbo-vl-32k"),
        duplicate_detection_enabled=False # Disable for test to force API call
    )
    
    client = VisionAIClient(config)
    
    # Find test image
    import glob
    search_pattern = str(project_root / "storage" / "**" / "*.png")
    candidates = glob.glob(search_pattern, recursive=True)
    
    if not candidates:
        logger.error("No test image found.")
        return
        
    test_image_path = candidates[0]
    logger.info(f"Using test image: {test_image_path}")

    # Call API
    logger.info("Sending async request to Vision AI API...")
    try:
        result = await client.validate_image(
            image_path=test_image_path,
            prompt=TEST_PROMPT,
            skip_duplicate_check=True
        )
        
        logger.info("-" * 40)
        logger.info(f"RAW RESULT: {result}")
        logger.info("-" * 40)
        
        if "error" in result:
            logger.error(f"鉂?API Test Failed: {result['error']}")
        else:
            logger.info("鉁?API Test PASSED. Response received.")
            
    except Exception as e:
        logger.error(f"鉂?Exception during API call: {e}")
    finally:
        await client.close()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run_async_test())


