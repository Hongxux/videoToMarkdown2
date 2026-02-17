# PaddleOCR-VL-1.5 部署与调用说明

## 1. 本次部署总结

已在 `conda` 环境 `whisper_env` 下完成 `PaddleOCR-VL-1.5` 的模型下载、初始化和单图功能测试。

### 1.1 环境信息
- Python: `D:\New_ANACONDA\envs\whisper_env\python.exe`
- 主要调用入口: `paddlex.create_pipeline(pipeline="PaddleOCR-VL-1.5")`
- 模型缓存目录: `C:\Users\HongXU\.paddlex\official_models\PaddleOCR-VL-1.5`

### 1.2 测试图片
- `var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU023/SU023_ss_island_005.jpg`

### 1.3 输出字段
脚本输出每个区域的以下字段：
- `区域类型`
- `坐标`
- `识别文本`
- `semantic_tag`
- `layout_relation`
- `multimodal_score`
- `format_preserved_text`

---

## 2. 命令行使用方式

脚本路径：`tools/diagnostics/test_paddleocr_vl15_regions.py`

### 2.1 基本测试

```powershell
D:\New_ANACONDA\envs\whisper_env\python.exe tools/diagnostics/test_paddleocr_vl15_regions.py --image var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU023/SU023_ss_island_005.jpg
```

### 2.2 开启调试日志

```powershell
D:\New_ANACONDA\envs\whisper_env\python.exe tools/diagnostics/test_paddleocr_vl15_regions.py --image var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU023/SU023_ss_island_005.jpg --debug
```

### 2.3 导出图片区域裁剪调试结果（新增）

该参数会对图片类区域（如 `image`、`header_image`、`figure`）输出：
- 裁剪图：`*.jpg`
- OCR 文本：`*.txt`

```powershell
D:\New_ANACONDA\envs\whisper_env\python.exe tools/diagnostics/test_paddleocr_vl15_regions.py --image var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU023/SU023_ss_island_005.jpg --dump-crop-debug var/output/paddleocr_vl15_crop_debug
```

---

## 3. Python 中调用方式

### 3.1 直接调用脚本函数

```python
from pathlib import Path
from tools.diagnostics.test_paddleocr_vl15_regions import run_with_paddleocr_vl

image_path = Path(r"D:\videoToMarkdownTest2\var\storage\storage\65453b3e35f62593c19f79150a89c929\assets\SU023\SU023_ss_island_005.jpg")
regions = run_with_paddleocr_vl(
    image_path,
    preprocess=True,
    crop_debug_dir=Path(r"D:\videoToMarkdownTest2\var\output\paddleocr_vl15_crop_debug"),
)

for r in regions:
    print({
        "区域类型": r["区域类型"],
        "坐标": r["坐标"],
        "识别文本": r["识别文本"],
        "semantic_tag": r["semantic_tag"],
        "layout_relation": r["layout_relation"],
        "multimodal_score": r["multimodal_score"],
        "format_preserved_text": r["format_preserved_text"],
    })
```

### 3.2 仅做底层 pipeline 调用（不走封装脚本）

```python
import paddlex as pdx

img = r"D:\videoToMarkdownTest2\var\storage\storage\65453b3e35f62593c19f79150a89c929\assets\SU023\SU023_ss_island_005.jpg"
pipeline = pdx.create_pipeline(pipeline="PaddleOCR-VL-1.5")
results = list(pipeline.predict(img))
print(results[0].keys())
```

---

## 4. 常见问题

### 4.1 首次运行耗时较长
首次会下载并加载模型，属于正常现象。后续命中缓存后速度会明显提升。

### 4.2 图片区域 OCR 失败如何排查
优先使用 `--dump-crop-debug` 导出裁剪区域，检查：
- bbox 是否覆盖了目标文本
- 裁剪图是否过小或过模糊
- 文本是否本身对比度不足

如果主流程可识别但某些图片区域为空，通常是区域内确实无可读文本或文本过小。
