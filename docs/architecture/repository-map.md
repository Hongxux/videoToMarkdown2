# 仓库职责地图

更新日期：2026-02-09  
范围：`D:/videoToMarkdownTest2`

## 一、为什么要做这份地图
- 明确目录职责边界，降低误改与重复建设风险。
- 约束代码落点，避免再次出现“历史目录 + 新目录”并存。
- 为清理脚本、CI 检查与后续演进提供稳定基线。

## 二、顶层目录职责
- `.agent/`：本地代理规则与行为配置，非业务代码。
- `.obsidian/`：知识管理工具工作区配置。
- `.vscode/`：编辑器本地配置。
- `apps/`：可执行入口层，保持薄启动、少业务逻辑。
- `config/`：全局配置单一真源目录。
- `contracts/`：gRPC 协议真源与多语言生成代码承载目录。
- `docs/`：架构文档、运行手册、历史资料归档。
- `requirements/`：依赖清单聚合（含 `requirements/legacy/` 历史依赖归档）。
- `scripts/`：构建、维护、发布脚本。
- `services/`：核心服务实现（Java orchestrator + Python gRPC）。
- `storage/`：历史运行产物目录（新任务应迁移到 `var/storage/`）。
- `third_party/`：第三方代码隔离区。
- `tools/`：诊断、回放、临时工具脚本集合。
- `var/`：当前运行态数据主目录（缓存、模型、产物）。

## 三、根目录关键文件职责
- `.env`：本地运行环境变量（含密钥，不入库）。
- `.env.example`：环境变量模板。
- `.gitignore`：统一忽略规则（缓存、产物、第三方大目录）。
- `AGENTS.md`：协作规范、架构文档维护要求。
- `README.md`：仓库导航与快速上手入口。
- `apps/grpc-server/main.py`：gRPC 标准启动入口。
- `apps/worker/main.py`：Worker 标准启动入口。

## 四、核心调用链与目录映射
- API/编排（Java）：`services/java-orchestrator/`
- Python gRPC 入口：`apps/grpc-server/main.py`
- Python 服务实现：`services/python_grpc/src/`
- 合约真源：`contracts/proto/video_processing.proto`
- Python 生成代码：`contracts/gen/python/`
- 运行产物：`var/storage/{url_hash}/...`

## 五、已完成迁移与下线目录（2026-02-09）
- 历史兼容壳目录已删除，目录层级仅保留新架构路径。
- 历史文档迁移到：`services/python_grpc/src/docs/legacy/`。
- 历史 proto 归档到：`docs/archive/mvp/enterprise_services/protos/video_processing.proto`。
- 历史依赖清单迁移到：`requirements/legacy/`。

## 六、历史遗留区与处理策略
- `storage/`：
  - 状态：历史运行产物仍存在。
  - 策略：新任务统一落 `var/storage/`，历史目录按批次清理。
- `docs/archive/`：
  - 状态：历史文档归档区。
  - 策略：只归档，不作为主链文档入口。

## 七、清理基线（可重复执行）
- 清理脚本：`scripts/maintenance/cleanup_workspace.py`
- 支持能力：
  - 清理 `__pycache__/`、`*.pyc`、`.pytest_cache/`
  - 清理重复目录：`generated_grpc/`、`MiniCMP/`
  - 归档根目录历史文档到 `docs/archive/`
  - 可选清理运行产物：`storage/` 与 `var/storage/`
- 推荐流程：
  - 预览：`python scripts/maintenance/cleanup_workspace.py --dry-run`
  - 执行：`python scripts/maintenance/cleanup_workspace.py`
  - 深度清理：`python scripts/maintenance/cleanup_workspace.py --include-runtime-data`

## 八、后续维护约束（建议）
- 新增业务逻辑优先落在 `services/`，禁止新增 legacy 兼容壳目录。
- 新增协议必须更新 `contracts/proto/` 并重新生成代码。
- 新增配置只放 `config/`，并通过单一真源校验脚本验证。
- 每次结构调整同步更新：
  - `docs/architecture/overview.md`
  - `docs/architecture/repository-map.md`
  - `docs/architecture/upgrade-log.md`
