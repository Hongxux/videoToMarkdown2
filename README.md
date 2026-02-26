# videoToMarkdownTest2

## 仓库分层（2026-02-09）
- `apps/`：启动入口层（gRPC Server / Worker）。
- `services/`：核心服务实现（Java 编排 + Python 推理处理）。
- `contracts/`：gRPC 合约真源与生成代码。
- `config/`：配置单一真源目录。
- `scripts/`：构建、维护、发布脚本。
- `tools/`：诊断、回放、一次性工具。
- `docs/`：架构文档、运维手册与归档资料。
- `var/`、`storage/`：运行态数据目录（不纳入版本控制）。

## 关键入口
- Python gRPC：`apps/grpc-server/main.py`
- Worker：`apps/worker/main.py`
- Java 编排：`services/java-orchestrator/`
- Python 主实现：`services/python_grpc/src/`

## gRPC 合约与生成
- 合约真源：`contracts/proto/video_processing.proto`
- Python 生成代码：`contracts/gen/python/`
- 生成命令：
  - Windows：`scripts/build/generate_grpc.bat`
  - PowerShell：`scripts/build/generate_grpc.ps1`

## 配置管理（单一真源）
- 配置目录：`config/`
  - `config/video_config.yaml`
  - `config/module2_config.yaml`
  - `config/fault_detection_config.yaml`
  - `config/dictionaries.yaml`
- 校验命令：`python scripts/maintenance/check_config_single_source.py`

## 目录迁移说明（已完成）
- 已完成兼容壳下线并删除目录：`MVP_Module2_HEANCING/`、`stage1_pipeline/`、`videoToMarkdown/`、`proto/`。
- 历史文档已迁移到：`services/python_grpc/src/docs/legacy/`。
- 历史依赖清单已迁移到：`requirements/legacy/`。

## 维护与清理
- 清理预览：`python scripts/maintenance/cleanup_workspace.py --dry-run`
- 执行清理：`python scripts/maintenance/cleanup_workspace.py`
- 清理运行产物：`python scripts/maintenance/cleanup_workspace.py --include-runtime-data`

## 架构文档入口
- 总览：`docs/architecture/overview.md`
- 仓库职责图：`docs/architecture/repository-map.md`
- 升级记录：`docs/architecture/upgrade-log.md`

## Android Update Release
- SOP doc: `scripts/release/README.AndroidUpdate.md`
- One-command flow (upload + publish + verify): `scripts/release/release_and_verify_android_update.ps1`
- Interactive wizard: `scripts/release/android_update_wizard.ps1`
