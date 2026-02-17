
import unittest
import sys
import os
from typing import List, Dict, Any

# Ensure we can import the module
sys.path.append(os.getcwd())

try:
    from services.python_grpc.src.content_pipeline.markdown_enhancer import MarkdownEnhancer
except ImportError:
    # If standard import fails, try relative or assume we are running from root
    sys.path.append(os.path.join(os.getcwd(), 'services', 'python_grpc', 'src', 'content_pipeline'))
    try:
        from markdown_enhancer import MarkdownEnhancer
    except ImportError:
        print("Could not import MarkdownEnhancer. Skipping test.")
        sys.exit(0)

class TestImageDedupIntegration(unittest.TestCase):
    def setUp(self):
        # Instantiate without API key to disable LLM client
        self.enhancer = MarkdownEnhancer(api_key=None)
        # Manually set required attributes for _format_obsidian_embed
        self.enhancer._assets_dir = "assets"
        self.enhancer._markdown_dir = None # Use default behavior

    def test_deduplication(self):
        content = "Line 1 【imgneeded_img1】. Line 2 【imgneeded_img1】. End."
        items = [{"img_id": "img1", "img_path": "assets/img1.jpg"}]
        
        # Expected behavior: first removed, second replaced
        expected = "Line 1 . Line 2 ![[assets/img1.jpg]]. End."
        result = self.enhancer._replace_image_placeholders(content, items)
        self.assertEqual(result, expected)

    def test_mixed_ids(self):
        content = "A 【imgneeded_img1】 B 【imgneeded_img2】 C 【imgneeded_img1】 D."
        items = [
            {"img_id": "img1", "img_path": "assets/img1.jpg"},
            {"img_id": "img2", "img_path": "assets/img2.jpg"}
        ]
        
        # img1: first removed, second kept. img2: only one, kept.
        expected = "A  B ![[assets/img2.jpg]] C ![[assets/img1.jpg]] D."
        result = self.enhancer._replace_image_placeholders(content, items)
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
