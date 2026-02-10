# 项目架构重建操作手册（2026-02-09）

## 目标
- 建立 `apps/` + `services/` + `contracts/` + `var/` 分层结构。
- 收敛启动入口与实现目录，避免多处兼容壳并存。

## 目录约定
- `apps/`：启动入口（薄入口，不放业务实现）。
- `services/python_grpc/src/`：Python 业务实现。
- `services/java-orchestrator/`：Java 编排服务。
- `contracts/proto/`：gRPC 协议真源。
- `contracts/gen/python/`：Python gRPC 生成代码。
- `var/`：运行态数据（默认不提交到 Git）。

## 启动方式
- Python gRPC：`python apps/grpc-server/main.py`
- Worker：`python apps/worker/main.py`
- Java：`cd services/java-orchestrator && mvn spring-boot:run`

## gRPC 生成
- Windows bat：`scripts/build/generate_grpc.bat`
- PowerShell：`scripts/build/generate_grpc.ps1`

## 迁移策略
- 运行入口统一使用 `apps/`。
- 业务实现统一落到 `services/python_grpc/src/` 与 `services/java-orchestrator/`。
- 历史文档迁移到 `services/python_grpc/src/docs/legacy/`。
- 历史依赖清单迁移到 `requirements/legacy/`。

## 后续动作建议
- 继续将 `services/python_grpc/src/server/` 拆分为更清晰的编排层、执行层与资源层。
- 清理仅用于历史兼容的根目录脚本与导入路径（完成依赖收口后再删除）。
- 持续执行“目录职责地图 + 升级日志”双文档同步机制。
