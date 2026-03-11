# videoToMarkdownTest2

一个面向“视频 / 链接 / 文档 -> Markdown / 移动端阅读”的混合项目，核心链路由 `Python gRPC`、`Java Orchestrator`、`Web Demo` 和 `Android App Demo` 组成。

## 你最应该先看什么

- 想最快体验 Web Demo：看下方“3 分钟启动 Web Demo”。
- 想最快体验 App Demo：看下方“App Demo 快速上手”。
- 想上传到 GitHub：先看下方“哪些内容不要上传”。

## 目录入口

- `services/java-orchestrator/`：Spring Boot 编排层，同时直接托管 `index.html` 对应的 Web Demo。
- `services/python_grpc/src/`：Python 推理与内容处理主链路。
- `apps/grpc-server/main.py`：Python gRPC 服务入口。
- `app/`：Android App Demo。
- `docker-compose.yml`：本地 / 服务器统一启动入口。
- `deploy/docker/.env.example`：Docker Compose 参数模板，包含当前推荐并发参数。
- `scripts/release/quick_start.ps1`：对外推荐的一键启动脚本。

## 默认配置策略

- 截至 `2026-03-07`，`deploy/docker/.env.example` 中除 API Key 外的参数，已对齐作者本机当前验证效果最佳配置。
- 对外使用时，通常只需要填写 `DEEPSEEK_API_KEY` 和 `DASHSCOPE_API_KEY`。
- 其余并发、窗口和 inflight 参数默认保持作者本机配置，不需要使用者再手动调参。

## 3 分钟启动 Web Demo

### 前置条件

- 已安装 Docker Desktop。
- 本机可访问你需要的外部模型服务。
- 如要跑完整处理链路，通常只需要准备 `DEEPSEEK_API_KEY` 和 `DASHSCOPE_API_KEY`。

### 方式 A：一键脚本（推荐）

```powershell
powershell -ExecutionPolicy Bypass -File scripts/release/quick_start.ps1
```

脚本会自动完成这些动作：

- 如果根目录不存在 `.env`，自动从 `deploy/docker/.env.example` 复制一份。
- 使用者通常只需要补全 `DEEPSEEK_API_KEY` 和 `DASHSCOPE_API_KEY`。
- 其余参数默认沿用作者截至 `2026-03-07` 的本机最佳效果配置。
- 执行 `docker compose up -d --build`。
- 轮询 `http://localhost:8080/api/health`。
- 启动成功后输出 Web Demo、健康检查和日志入口。

### 方式 B：手动使用 docker compose（逐步）

如果你想明确看到“compose 是如何构建并拉起整个项目”的每一步，按下面顺序执行即可。

1. 进入仓库根目录：

```powershell
Set-Location D:\videoToMarkdownTest2
```

2. 确认 Docker Desktop 已启动，并且 `docker` 命令可用：

```powershell
docker info
```

如果这里报错，先打开 Docker Desktop，再继续后续步骤。

3. 复制 `.env` 模板：

```powershell
Copy-Item .\deploy\docker\.env.example .\.env -Force
```

4. 打开根目录 `.env`，至少填写下面两个密钥：

```env
DEEPSEEK_API_KEY=你的_deepseek_key
DASHSCOPE_API_KEY=你的_dashscope_key
VISION_AI_BEARER_TOKEN=
```

5. 使用你已经写好的 `docker-compose.yml` 构建并后台启动两个服务：

```powershell
docker compose up -d --build
```

这条命令会做两件事：

- 先根据 `deploy/docker/python-grpc.Dockerfile` 和 `deploy/docker/java-orchestrator.Dockerfile` 构建镜像。
- 再按根目录 `docker-compose.yml` 启动 `python-grpc` 和 `java-orchestrator` 两个容器。

6. 查看容器状态，确认服务已经起来：

```powershell
docker compose ps
```

正常情况下，你会看到：

- `python-grpc` 对外暴露 `50051`
- `java-orchestrator` 对外暴露 `8080`

7. 验证 Web Demo 和健康检查：

```powershell
curl http://localhost:8080/api/health
```

如果健康检查通过，再打开：

- Web Demo：`http://localhost:8080`
- Health API：`http://localhost:8080/api/health`

8. 如果启动失败，优先看日志：

```powershell
docker compose logs -f --tail=200
```

9. 如果你改了 `.env` 或配置文件，建议重建对应容器：

```powershell
docker compose up -d --build --force-recreate
```

10. 停止项目：

```powershell
docker compose down
```

### API Key 配置（Java + Python 共用）

- 根目录 `D:\videoToMarkdownTest2\.env` 是 Docker Compose 的统一配置入口；`python-grpc` 和 `java-orchestrator` 都会从这里读取环境变量。
- `DEEPSEEK_API_KEY`：必填；`java-orchestrator` 的 `DeepSeekAdvisorService` 和 `python-grpc` 的文本增强链路都会使用。
- `DASHSCOPE_API_KEY`：建议填写；主要供 `python-grpc` 的 VL / 视频理解链路使用。
- `VISION_AI_BEARER_TOKEN`：可选；仅在开启视觉校验相关能力时需要。
- `.env` 建议保存为 `UTF-8` 无 BOM 纯文本；Docker 对首个变量名较敏感，带 BOM 时可能导致首个 key 注入失败。

推荐至少填写：

```env
DEEPSEEK_API_KEY=你的_deepseek_key
DASHSCOPE_API_KEY=你的_dashscope_key
VISION_AI_BEARER_TOKEN=
```

- 如果根目录还没有 `.env`，先复制模板：

```powershell
Copy-Item D:\videoToMarkdownTest2\deploy\docker\.env.example D:\videoToMarkdownTest2\.env -Force
```

- 修改 `.env` 后，需要重建容器环境变量；仅改文件本身不会自动进入已运行容器。推荐执行：

```powershell
docker compose up -d --force-recreate python-grpc java-orchestrator
```

- 如果你只想确认变量是否已经进容器，优先看运行日志是否还出现 `DEEPSEEK_API_KEY is empty`，或直接重新触发对应功能链路验证。

启动成功后直接打开：

- Web Demo：`http://localhost:8080`
- Health API：`http://localhost:8080/api/health`

### 常用运维命令

```powershell
# 查看状态
powershell -ExecutionPolicy Bypass -File scripts/release/docker_release.ps1 -Action ps

# 查看日志
powershell -ExecutionPolicy Bypass -File scripts/release/docker_release.ps1 -Action logs

# 停止服务
powershell -ExecutionPolicy Bypass -File scripts/release/docker_release.ps1 -Action down
```

### 首次构建经验（2026-03-09 已验证）

- `python-grpc` 镜像现在会在构建期预装 `ffmpeg` 和 `Whisper base`，默认只预装 `base`，不额外预装 `medium/large`。
- Whisper 模型缓存路径已经固定为 `/opt/huggingface/hub`，并通过 `HF_HOME`、`HUGGINGFACE_HUB_CACHE`、`WHISPER_MODEL_CACHE_DIR` 对齐“构建期预装”和“运行期读取”，容器首次启动时不应再重复下载 `base`。
- `python-grpc` 构建显式使用 CPU 版 `torch` / `torchvision` / `torchaudio`，避免 Linux 下默认解析出超大的 CUDA 依赖，降低镜像膨胀和构建超时概率。
- `java-orchestrator` 构建已启用 Maven 本地缓存挂载和轻量重试；如果失败信息是 Maven Central 握手/内容截断，优先重试；如果失败信息是 `compile` 或 `unclosed character literal`，优先按源码编译错误排查，而不是误判为 Docker 问题。
- 首次完整构建通常最慢的阶段是：Python 依赖安装、`Whisper base` 下载、镜像导出；后续重建会明显更快，因为 Docker layer、pip 缓存和 Maven 缓存会复用。
- 需要单独验证镜像时，优先用：

```powershell
docker compose build python-grpc
docker compose build java-orchestrator
```

- 这两条命令已在当前仓库结构下做过真实验证，成功产出：
  - `videotomarkdown-release-python-grpc:latest`
  - `videotomarkdown-release-java-orchestrator:latest`

## App Demo 快速上手

### 路径 A：给体验者

- 推荐把 APK 发布到 GitHub Releases。
- 体验者只需要下载 APK、安装，然后连接你公开部署的服务地址。

### 路径 B：给开发者

1. 先按上面的步骤启动后端。
2. 再构建并安装 Android App。

#### Android 模拟器

```powershell
.\gradlew.bat :app:installDebug -PmobileApiBaseUrl=http://10.0.2.2:8080/api/mobile
```

#### Android 真机

```powershell
.\gradlew.bat :app:installDebug -PmobileApiBaseUrl=http://<你的局域网IP>:8080/api/mobile
```

说明：

- `mobileApiBaseUrl` 可以直接传完整 `/api/mobile`，也可以只传主机根地址，构建时会自动补齐。
- 如果你不传 `-PmobileApiBaseUrl`，项目会优先读取 `MOBILE_APP_API_BASE_URL`，其次读取 `MOBILE_API_BASE_URL`。
- 如果属性和环境变量都没传，默认回落到 `http://10.0.2.2:8080/api/mobile`，方便 Android 模拟器直连本地后端。

### Android 更新发布

- 自动更新脚本说明：`scripts/release/README.AndroidUpdate.md`
- 一条命令上传 + 发布 + 校验：`scripts/release/release_and_verify_android_update.ps1`

## 哪些内容不要上传

下面这些内容应该留在本地，不应该进入 GitHub：

- 密钥与本地配置：`.env`、各种 token、证书、企业微信密钥、局域网私有参数。
- 登录态与抓取态：`cookies.txt`、`cookies.pkl`。
- 本地数据库：`*.db`、`*.sqlite*`。
- 运行产物：`var/`、`output/`、`storage/`、`services/java-orchestrator/var/`、`services/java-orchestrator/output/`。
- 日志与崩溃文件：`*.log`、`hs_err_pid*.log`、`replay_pid*.log`。
- 大文件：视频、音频、模型、APK、AAB。
- IDE 与构建缓存：`.idea/`、`.vscode/`、`.gradle/`、`.m2/`、`local.properties`、`tmp_*/`。

如果这些文件此前已经被 Git 跟踪，仅补 `.gitignore` 还不够，还需要把它们从索引里移除。推荐先用 `git status` 检查，再执行有针对性的 `git rm --cached`。

## 上传到 GitHub 前的建议流程

1. 检查 `.env`、Cookie、数据库、日志是否仍在 Git 跟踪中。
2. 确认 `deploy/docker/.env.example` 保留的是“作者最佳效果模板值 + 空白密钥位”，不是你的真实密钥。
3. 优先通过 `scripts/release/quick_start.ps1` 验证 Web Demo 默认成功路径。
4. 用模拟器命令验证 App Demo 至少能连通本地后端。
5. 再推送到 GitHub，并在 Releases 中附上 APK 与简短安装说明。

## 当前推荐的 GitHub 对外交付方式

- 源码仓库：用于开发者 clone 与二次开发。
- Docker Compose：作为 Web Demo 的默认启动方式。
- GitHub Releases：用于发 APK、更新说明和截图。

## 补充说明

- `index.html` 不是一个建议“单独双击打开”的静态页，它依赖同域下的 `/api/mobile` 接口。
- 正确的 Web Demo 体验方式，是先启动 `java-orchestrator` + `python-grpc`，再访问 `http://localhost:8080`。
- 如果你准备公开演示，建议额外部署一个稳定的线上 Demo 地址，避免继续依赖临时 `ngrok` 域名。
