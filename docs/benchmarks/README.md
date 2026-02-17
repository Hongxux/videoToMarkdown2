# 压测程序使用 README

本文档说明并发压测脚本的用途、运行方式、产物结构，以及内置测试素材路径。

## 1. 统一原则（参照 `docs/并发测试方法.md`）
- 并发采用阶梯升压：`1 -> 2 -> 4 -> 6 -> 8 ...`
- 每个并发档位建议重复 `3-5` 轮
- 判定拐点标准：吞吐增幅 < 5% 且 P95/错误率明显恶化
- 生产参数建议取拐点的 `70%-80%`
- 所有脚本默认保留原始数据并输出图表

## 2. 产物目录约定
所有脚本默认输出到：
- `var/artifacts/benchmarks/<task_name>_<timestamp>/`

目录结构：
- `raw/runs_raw.json|csv`：每轮原始结果
- `raw/summary_by_case.json|csv`：按 case 汇总
- `raw/recommendation.json`：推荐参数与规则
- `raw/*_results_*.json`：每轮明细原始数据
- `raw/system_samples_*.json`：CPU/内存采样原始点
- `charts/concurrency_summary.png`：结果图表
- `report.md`：可读报告

## 3. 环境准备
- Python 可用（建议与项目一致的虚拟环境）
- 已安装依赖（包含 `matplotlib`、`psutil`、`pyyaml`）
- 设置 `PYTHONPATH` 指向仓库根目录
- 需要真实 LLM/Vision 压测时，确保配置中已填写可用密钥

Windows PowerShell 示例：
```powershell
$env:PYTHONPATH = "D:/videoToMarkdownTest2"
```

## 4. 内置测试素材
- DeepSeek 文本样本：`var/artifacts/benchmarks/sample_data/llm_text/deepseek_units.json`
- VL clip 清单：`var/artifacts/benchmarks/sample_data/vl_llm_sample/clip_manifest.json`
- Vision 图片清单：`var/artifacts/benchmarks/sample_data/vision_ai_sample/image_manifest.json`

## 5. DeepSeek 并发+批大小压测
脚本：`scripts/bench_llm_deepseek_concurrency_batch.py`

```powershell
python scripts/bench_llm_deepseek_concurrency_batch.py `
  --dataset "var/artifacts/benchmarks/sample_data/llm_text/deepseek_units.json" `
  --concurrency "1,2,4,6,8,10,12" `
  --chunk-sizes "1,2,4,6,8" `
  --token-budgets "4000,8000,12000" `
  --repeats 3 `
  --task-name "llm_deepseek_concurrency_batch"
```

## 6. VL LLM 并发+负载压测
脚本：`scripts/bench_vl_llm_concurrency_payload.py`

```powershell
python scripts/bench_vl_llm_concurrency_payload.py `
  --clip-manifest "var/artifacts/benchmarks/sample_data/vl_llm_sample/clip_manifest.json" `
  --target-clip-count 12 `
  --concurrency "1,2,3,4,6,8" `
  --max-input-frames "4,8,12,16,24" `
  --repeats 2 `
  --task-name "vl_llm_concurrency_payload"
```

## 7. Vision AI 并发+批量可行性压测
脚本：`scripts/bench_vision_concurrency_batchability.py`

覆盖三个并发点：
- `client_single`：`VisionAIClient.validate_image`
- `client_batch`：`VisionAIClient.validate_images_batch`
- `validator`：`ConcreteKnowledgeValidator.validate_batch`

```powershell
python scripts/bench_vision_concurrency_batchability.py `
  --image-manifest "var/artifacts/benchmarks/sample_data/vision_ai_sample/image_manifest.json" `
  --modes "client_single,client_batch,validator" `
  --concurrency "1,2,4,6,8" `
  --batch-sizes "1,2,4,6,8" `
  --target-image-count 24 `
  --repeats 2 `
  --task-name "vision_concurrency_batchability"
```

## 8. 推荐判定规则
- 单 case 推荐：成功率优先（>=99%），其次吞吐最高且 P95 更低
- Vision 批量能力上线门槛：
  - 吞吐提升 >= 20%
  - 质量匹配下降不超过 1%

## 9. 常见问题
- Vision 压测报 `bearer_token is empty`：检查 `config/video_config.yaml` 中 `vision_ai.bearer_token`
- VL 清单找不到 clip：检查 `clip_manifest.json` 路径是否存在
- 结果异常偏快：确认是否开启了重复帧缓存；可加 `--skip-duplicate-check` 控制变量
