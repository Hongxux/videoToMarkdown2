# README: 修复 PP-Structure 与人物占比预处理降级

本文用于修复以下问题，并提供可复现的验收步骤：

- 进入 `AI Vision` 后，看不到结构化预处理带来的截图增量。
- `PP-structure` 预处理没有生效。
- 人物占比预处理（`person_subject_filter`）没有生效。

适用项目路径：`D:\videoToMarkdownTest2`

## 1. 现象与根因

### 1.1 典型现象

- `run_server.ps1` 已启动，任务也进入 Vision 分析阶段。
- 但 `assets` 下没有 `__ppstructure_*.png` 结构化裁剪图。
- `vision_ai_cache_*.json` 中看不到 `person_mask_ratio` / `prefilter_source` 字段。

### 1.2 已确认根因

1. 服务启动时固定 `PYTHONNOUSERSITE=1`（`run_server.ps1:1`），用户目录安装的包不会被加载。
2. `PP-Structure` 依赖 `paddleocr`，兜底依赖 `paddlex`，两者若未安装在 `whisper_env` 本体内，会直接降级。
3. 人物占比预处理代码依赖 `mediapipe.solutions.selfie_segmentation`，若当前 `mediapipe` 发行包没有 `solutions` API，也会降级。

相关代码位置：

- `services/python_grpc/src/content_pipeline/phase2a/segmentation/concrete_knowledge_validator.py`
- `services/python_grpc/src/content_pipeline/infra/llm/vision_ai_client.py`
- `services/python_grpc/src/server/dependency_check.py`

## 2. 修复目标

修复后必须满足以下三个条件：

1. `PP-structure` 引擎可初始化。
2. `paddlex` 兜底模型可初始化。
3. 人物占比预处理分割器可初始化（`person_segmenter=True`）。

## 3. 推荐修复步骤

## 3.0 本次已验证可用的版本组合（Windows + Python 3.12）

以下组合已在本机验证通过（`--check-deps` 通过，三项能力可初始化）：

- `paddleocr==2.7.3`
- `paddlepaddle==3.3.0`
- `paddlex==3.4.2`
- `mediapipe==0.10.14`
- `protobuf==6.33.5`
- `numpy==1.26.4`
- `opencv-python==4.6.0.66`
- `opencv-contrib-python==4.6.0.66`
- `setuptools==69.5.1`
- `sounddevice` 不安装（见 5.4）

## 3.1 先固定运行环境（必须）

服务必须通过 `whisper_env` 启动，并保留 `PYTHONNOUSERSITE=1`，避免 user-site 污染。

```powershell
cd D:\videoToMarkdownTest2
Get-Content .\run_server.ps1
```

预期至少包含：

- `$env:PYTHONNOUSERSITE = "1"`
- `conda activate whisper_env`

## 3.2 在 `whisper_env` 本体补齐依赖（不要装到 user-site）

```powershell
$py = "D:\New_ANACONDA\envs\whisper_env\python.exe"
$env:PYTHONNOUSERSITE = "1"

& $py -m pip install --upgrade pip wheel --no-user
& $py -m pip install --no-user `
  "setuptools==69.5.1" `
  "paddlepaddle==3.3.0" `
  "paddleocr==2.7.3" `
  "paddlex==3.4.2" `
  "mediapipe==0.10.14" `
  "protobuf==6.33.5" `
  "numpy==1.26.4" `
  "opencv-python==4.6.0.66" `
  "opencv-contrib-python==4.6.0.66" `
  "rapidfuzz==3.14.3" `
  "attrdict==2.0.1" `
  "cython==3.2.4" `
  "fire==0.7.1" `
  "python-docx==1.2.0" `
  "pdf2docx==0.5.9" `
  "visualdl==2.5.3"

# 关键：避免 mediapipe 导入阶段触发 sounddevice/PortAudio UnicodeDecodeError
& $py -m pip uninstall -y sounddevice
```

如果你所在网络受限，请改为内网镜像或离线 wheel 安装（命令不变，仅替换源）。

## 3.3 修复 `mediapipe.solutions` 不可用问题

先验证当前 `whisper_env` 是否可用：

```powershell
$py = "D:\New_ANACONDA\envs\whisper_env\python.exe"
$env:PYTHONNOUSERSITE = "1"
@'
import mediapipe as mp
print("mediapipe_version=", getattr(mp, "__version__", "unknown"))
print("has_solutions=", hasattr(mp, "solutions"))
'@ | & $py -
```

若输出 `has_solutions=False`，建议使用 Python 3.11 环境（长期稳定方案）：

```powershell
conda create -n whisper_env311 python=3.11 -y
conda activate whisper_env311
python -m pip install -r D:\videoToMarkdownTest2\requirements.txt
python -m pip install paddleocr==2.7.0.0 paddlex==3.4.2 mediapipe==0.10.14
python -c "import mediapipe as mp; print(hasattr(mp,'solutions'))"
```

然后把 `run_server.ps1` 的环境切换到 `whisper_env311`。

## 3.4 临时绕过（仅过渡）

如果暂时无法修复 `mediapipe`，可先关闭人物预处理，避免“以为开启实际降级”的误判：

文件：`config/video_config.yaml`

```yaml
vision_ai:
  person_subject_filter:
    enabled: false
```

此方式只影响人物占比预处理，不影响 PP-structure 修复。

## 4. 如何检验修复成功

## 4.1 启动前依赖预检（第一道门）

项目已增强 `--check-deps`，会显式检查：

- `ppstructure_preprocess`
- `paddlex_layout_fallback`
- `person_subject_prefilter`

执行：

```powershell
cd D:\videoToMarkdownTest2
$env:PYTHONNOUSERSITE = "1"
D:\New_ANACONDA\envs\whisper_env\python.exe .\apps\grpc-server\main.py --check-deps
```

通过标准：

```text
Dependency preflight passed.
```

若失败会看到：

```text
Feature readiness failures:
- ppstructure_preprocess: ...
- paddlex_layout_fallback: ...
- person_subject_prefilter: ...
```

## 4.2 运行时探针（第二道门）

```powershell
cd D:\videoToMarkdownTest2
$env:PYTHONNOUSERSITE='1'
$py='D:\New_ANACONDA\envs\whisper_env\python.exe'
@'
from services.python_grpc.src.content_pipeline.phase2a.segmentation.concrete_knowledge_validator import ConcreteKnowledgeValidator
from services.python_grpc.src.content_pipeline.infra.llm.vision_ai_client import VisionAIClient, VisionAIConfig

v=ConcreteKnowledgeValidator(config_path='D:/videoToMarkdownTest2/config/video_config.yaml', output_dir='D:/videoToMarkdownTest2/var/tmp_probe')
print('structure_engine=', v._get_structure_engine() is not None)
print('paddlex_layout=', v._get_paddlex_layout_model() is not None)
print('structure_engine_err=', getattr(v,'_structure_engine_init_error',None))
print('paddlex_layout_err=', getattr(v,'_structure_paddlex_model_init_error',None))

c=VisionAIClient(VisionAIConfig(enabled=True, person_subject_filter_enabled=True))
print('person_segmenter=', c._get_person_segmenter() is not None)
print('person_segmenter_err=', getattr(c,'_person_segmenter_init_error',None))
'@ | & $py -
```

通过标准：

- `structure_engine=True`
- `paddlex_layout=True`
- `person_segmenter=True`
- 三个 `*_err` 为 `None` 或空。

## 4.3 任务产物验收（第三道门）

跑一个新任务后检查：

1. 是否生成 PP-structure 裁剪图：

```powershell
Get-ChildItem var\storage\storage\<task_id>\assets -Recurse -Filter *__ppstructure_*.png
```

2. 人物预处理是否写入缓存字段：

```powershell
rg -n "person_mask_ratio|prefilter_source" var\storage\storage\<task_id>\intermediates\vision_ai_cache_*.json
```

3. 检查最终引用：

```powershell
rg -n "__ppstructure_" var\storage\storage\<task_id>\result.json
```

## 5. 常见失败与处理

## 5.1 `No module named 'paddleocr'` 或 `No module named 'paddlex'`

- 说明包未装进 `whisper_env` 本体，或只装在 user-site。
- 重新执行 3.2，并保持 `PYTHONNOUSERSITE=1` 验证。

## 5.2 `mediapipe has no attribute solutions`

- 当前安装的 `mediapipe` 不包含旧 `solutions` API。
- 按 3.3 切换到 Python 3.11 环境并重新安装。

## 5.3 不修改代码也想先恢复服务可用

- 优先恢复 `paddleocr/paddlex`，保证 PP-structure 生效。
- 暂时关闭 `person_subject_filter.enabled`，待环境就绪后再开启。

## 5.4 `UnicodeDecodeError` 来自 `sounddevice`

典型日志：

```text
UnicodeDecodeError ... sounddevice.py ... _check(...).decode()
```

原因：`mediapipe` 的音频子模块导入 `sounddevice` 时触发 PortAudio 文本解码异常（与本项目视觉链路无关）。

处理：

```powershell
$env:PYTHONNOUSERSITE='1'
D:\New_ANACONDA\envs\whisper_env\python.exe -m pip uninstall -y sounddevice
```

说明：仅影响 MediaPipe 音频任务，不影响本项目使用的 `selfie_segmentation` 人物占比预处理。

## 6. 回归检查清单

每次升级环境后，至少执行一次：

1. `python apps/grpc-server/main.py --check-deps`
2. 运行时探针（4.2）
3. 新任务产物验收（4.3）

全部通过后再作为生产可用环境。

## 7. 本次实战经验总结（2026-02-14）

### 7.1 结论

1. 问题不是“流程没进 Vision”，而是“预处理依赖缺失或不兼容导致静默降级”。
2. `run_server.ps1` 使用 `PYTHONNOUSERSITE=1` 是正确做法，能避免 user-site 污染；但这也要求所有关键依赖必须安装在 `whisper_env` 本体。
3. 在 Python 3.12 下，`paddleocr==2.7.0.0` 容易卡在 `PyMuPDF` 构建；改用 `paddleocr==2.7.3` 可规避该问题。
4. `mediapipe` 导入时可能因 `sounddevice` 触发 `UnicodeDecodeError`，卸载 `sounddevice` 后可恢复 `selfie_segmentation`（不影响本项目视觉链路）。

### 7.2 最终稳定状态（本机已验证）

- `Dependency preflight passed.`
- `structure_engine=True`
- `paddlex_layout=True`
- `person_segmenter=True`
- 实图可生成 `__ppstructure_*.png`
- 人物占比可计算 `person_mask_ratio`

### 7.3 高价值经验

1. 先验收“功能可用性”，再验收“产物是否出现”  
先跑探针确认三项初始化状态，再看任务产物。否则容易把“产物缺失”误判为业务逻辑问题。

2. 一律在同一解释器内安装和验证  
安装命令与验证命令都必须显式使用 `D:\New_ANACONDA\envs\whisper_env\python.exe`，避免“装在 A，跑在 B”。

3. 预检要覆盖功能级依赖  
仅做 `import grpc/httpx` 不够，必须检查 `paddleocr/paddlex/mediapipe.solutions` 是否可初始化。

4. 优先保主链路，再追求最优链路  
即使 `PPStructure` 推理偶发失败，也应保证 `PaddleX` fallback 可用，避免全链路不可用。

### 7.4 下次排障建议顺序

1. `--check-deps`
2. 运行时探针（4.2）
3. 单张实图验证（结构化裁剪 + 人物占比）
4. 新任务端到端验证（4.3）

按这个顺序可以把定位时间从“小时级”降到“分钟级”。
