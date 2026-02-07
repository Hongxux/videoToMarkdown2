
import sys
import os
import logging
import json
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Simulation")

# Add project root to sys.path
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

try:
    from MVP_Module2_HEANCING.module2_content_enhancement.concrete_knowledge_validator import ConcreteKnowledgeValidator
except ImportError as e:
    logger.error(f"Failed to import validator: {e}")
    # Try alternate import path structure if needed
    try:
        sys.path.append(str(project_root / "MVP_Module2_HEANCING"))
        from module2_content_enhancement.concrete_knowledge_validator import ConcreteKnowledgeValidator
    except ImportError as e2:
        logger.error(f"Failed to import validator again: {e2}")
        sys.exit(1)

def main():
    # Config path
    config_path = project_root / "videoToMarkdown" / "config.yaml"
    # Check if config exists
    if not config_path.exists():
         # try another potential location
         config_path = project_root / "config.yaml"
         if not config_path.exists():
            logger.error(f"Config not found at {config_path}")
            return
    
    logger.info(f"Using config: {config_path}")

    # Initialize validator
    try:
        validator = ConcreteKnowledgeValidator(config_path=str(config_path), output_dir=str(project_root / "temp_output"))
        # FORCE DISABLE CACHE FOR SIMULATION
        validator._hash_cache = None
        logger.info("Disabled validator cache for simulation.")
    except Exception as e:
        logger.error(f"Validator init failed: {e}")
        return
    
    if hasattr(validator, '_vision_enabled'):
        if not validator._vision_enabled:
            logger.warning("Vision AI is NOT enabled in validator!")
        else:
            logger.info("Vision AI is enabled.")
    else:
        logger.warning("Could not determine if Vision AI is enabled (private attribute).")

    # Sample image path construction
    storage_base = Path(r"d:\videoToMarkdownTest2\storage\99efb7c15a9121f4e29113821d5c9c73\screenshots")
    
    image_paths = []
    if storage_base.exists():
        all_pngs = list(storage_base.glob("*.png"))
        import random
        # Select 10 random images if available, else all
        if len(all_pngs) > 10:
            image_paths = [str(p) for p in random.sample(all_pngs, 10)]
        else:
            image_paths = [str(p) for p in all_pngs]
    
    if not image_paths:
        logger.error("No images found to simulate on.")
        return

    logger.info(f"Simulating on {len(image_paths)} images...")

    for i, image_path in enumerate(image_paths):
        logger.info(f"Validating image {i+1}/{len(image_paths)}: {image_path}")
        
        # Run validation
        try:
            result = validator.validate(image_path=image_path)
            
            # Output result
            print("\n" + "="*50)
            print(f"VALIDATION RESULT [{i+1}/{len(image_paths)}]: {Path(image_path).name}")
            print("="*50)
            print(f"Has Concrete Knowledge: {result.has_concrete}")
            print(f"Has Formula: {result.has_formula}")
            print(f"Confidence: {result.confidence}")
            print(f"Concrete Type: {result.concrete_type}")
            # print(f"Reason: {result.reason}") # User removed reason in previous steps, but it might still be in the object if not removed from dataclass
            print(f"Image Description: {result.img_description}")
            print(f"Should Include: {result.should_include}")
            print("="*50 + "\n")
            
        except Exception as e:
            logger.error(f"Validation execution failed for {image_path}: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()
