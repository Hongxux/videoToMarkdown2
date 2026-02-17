# Docker Compose Release 运行手册

更新时间：2026-02-14  
适用仓库：`D:/videoToMarkdownTest2`

## 1. 目标
- 将当前 Java + Python 双服务链路打包为可复用 release。
- 交付形态统一为 Docker Compose，降低环境差异导致的启动失败。

## 2. 目录与文件
- 编排文件：`docker-compose.yml`
- Python 镜像定义：`deploy/docker/python-grpc.Dockerfile`
- Java 镜像定义：`deploy/docker/java-orchestrator.Dockerfile`
- 环境变量模板：`deploy/docker/.env.example`
- Windows 启停脚本：`scripts/release/docker_release.ps1`
- 发布包构建脚本：`scripts/release/build_docker_release_bundle.ps1`
- 对外 README：`README.DockerRelease.md`

## 3. 前置条件
- 已安装 Docker Desktop（包含 Docker Compose）。
- 机器可访问外部模型/API 所需网络（如 DeepSeek、DashScope）。

## 4. 首次发布步骤
1. 在仓库根目录创建 `.env`：
   - 复制 `deploy/docker/.env.example` 到 `.env`。
2. 按需填写密钥：
   - `DEEPSEEK_API_KEY`
   - `MODULE2_DEEPSEEK_CONCURRENCY_INITIAL`（建议 `56`）
   - `MODULE2_DEEPSEEK_CONCURRENCY_MIN`（建议 `8`）
   - `MODULE2_DEEPSEEK_CONCURRENCY_MAX`（建议 `64`）
   - `MODULE2_DEEPSEEK_CONCURRENCY_WINDOW_SIZE`（建议 `30`）
   - `MODULE2_MARKDOWN_SECTION_MAX_INFLIGHT`（建议 `56`）
   - `MODULE2_KC_MULTI_CHUNK_MAX_INFLIGHT`（建议 `48`）
   - `MODULE2_SEMANTIC_SEGMENT_BATCH_MAX_CONCURRENCY`（建议 `48`）
   - `DASHSCOPE_API_KEY`（如需要 VL 视频分析）
   - `VISION_AI_BEARER_TOKEN`（如需要 Vision 具象校验）
3. 启动服务：
   - `powershell -ExecutionPolicy Bypass -File scripts/release/docker_release.ps1 -Action up`
4. 验证服务：
   - `docker compose ps`
   - `curl http://localhost:8080/api/health`

## 5. 常用命令
- 启动并构建：`docker compose up -d --build`
- 查看状态：`docker compose ps`
- 查看日志：`docker compose logs -f --tail=200`
- 停止并删除容器：`docker compose down`
- 构建发布包：`powershell -ExecutionPolicy Bypass -File scripts/release/build_docker_release_bundle.ps1 -Version v0.1.0`

## 6. 运行时卷映射
- `./config:/app/config`
- `./var:/app/var`

说明：
- `config` 映射用于保持配置单一事实源。
- `var` 映射用于保留任务产物和中间文件，避免重建容器后数据丢失。

## 7. 兼容性与边界
- 当前镜像默认 CPU 路径，未启用 GPU runtime。
- 首次构建时间较长，原因是 Python 侧依赖规模较大。
- 若 `config/video_config.yaml` 中使用固定 token，请在对外发布前先做脱敏。

## 8. 回滚方案
1. 保留上一版镜像 tag。
2. 在 `docker-compose.yml` 中切回上一版 `image` 或 `build` 对应版本。
3. 执行 `docker compose up -d` 完成回滚。
