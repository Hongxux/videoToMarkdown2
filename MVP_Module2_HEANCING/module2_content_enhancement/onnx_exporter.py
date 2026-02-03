import os
import torch
from sentence_transformers import SentenceTransformer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ONNX_Exporter")

def export_to_onnx(model_name: str, output_path: str):
    """
    将 SentenceTransformer 模型导出为 ONNX 格式
    """
    logger.info(f"Loading model: {model_name}")
    model = SentenceTransformer(model_name)
    
    # 准备导出路径
    os.makedirs(output_path, exist_ok=True)
    onnx_file = os.path.join(output_path, "model.onnx")
    
    # 模拟输入 (MiniLM-L12-v2 typical input names)
    # 输入: input_ids, attention_mask, token_type_ids
    dummy_input = model.tokenizer("This is a test sentence.", return_tensors="pt")
    
    logger.info("Exporting to ONNX...")
    # 💥 降级方案: 使用最基础的 export 接口，避免触发 TorchDynamo/onnxscript 依赖
    torch.onnx.export(
        model[0].auto_model,               
        (dummy_input['input_ids'], dummy_input['attention_mask']), 
        onnx_file,
        input_names=['input_ids', 'attention_mask'],
        output_names=['last_hidden_state'],
        dynamic_axes={
            'input_ids': {0: 'batch_size', 1: 'sequence_length'},
            'attention_mask': {0: 'batch_size', 1: 'sequence_length'},
        },
        opset_version=12, # 降低 opset 版本以提升兼容性
        do_constant_folding=True,
    )
    
    logger.info(f"Model exported to {onnx_file}")

if __name__ == "__main__":
    MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
    OUTPUT_DIR = os.path.join(os.getcwd(), "models", "onnx", MODEL_NAME)
    export_to_onnx(MODEL_NAME, OUTPUT_DIR)
