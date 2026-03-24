# Video To Markdown

一个面向公开体验的 Docker 版本，只保留后端服务和 Web 界面。

## 效果展示

当前仓库已预留展示位，建议将截图放到 `assets/readme/` 后再替换下面内容：

- `Web 首页截图：assets/readme/web-home.png`
- `任务页截图：assets/readme/web-task.png`
- `Markdown 输出截图：assets/readme/markdown-result.png`

## 环境要求

- Git
- Docker Desktop 或 Docker Engine + Docker Compose
- 可访问 DeepSeek 与 DashScope
- 建议至少 `8 GB` 可用内存
- 建议至少 `10 GB` 可用磁盘空间

## 快速开始

### 1. 克隆仓库

```bash
git clone <your-repo-url>
cd videoToMarkdown
```

### 2. 复制环境变量模板

PowerShell:

```powershell
Copy-Item .env.example .env
```

Bash:

```bash
cp .env.example .env
```

### 3. 填写必要配置

正常情况下只需要填写下面两个变量：

```env
DEEPSEEK_API_KEY=your-deepseek-api-key
DASHSCOPE_API_KEY=your-dashscope-api-key
```

`VISION_AI_BEARER_TOKEN` 只有在你明确启用对应视觉能力时才需要。

### 4. 构建并启动

```bash
docker compose up -d --build
```

### 5. 打开服务

- Web 界面：`http://localhost:8080`
- 健康检查：`http://localhost:8080/api/health`

## `.env` 配置说明

### 必填

- `DEEPSEEK_API_KEY`：文本与结构化链路使用。
- `DASHSCOPE_API_KEY`：视觉分析链路使用。

### 可选

- `VISION_AI_BEARER_TOKEN`：仅在你明确启用对应视觉服务时需要。
- `TASK_RUNTIME_REDIS_ENABLED`：默认为 `0`，不开启 Redis 运行时存储。

### 一般无需修改

- 并发、窗口、inflight 和缓存相关参数已经给出可直接运行的默认值。
- 正常体验下不需要修改 Dockerfile、Compose 文件或配置文件路径。

## Docker 通用性说明

这份公开版默认面向“直接 clone 后运行”的场景设计：

- 正常情况下，不需要修改 `docker-compose.yml`
- 正常情况下，不需要修改 `deploy/docker/*.Dockerfile`
- 正常情况下，不需要修改 `config/*.yaml` 的路径配置

只有在以下场景才建议你再做额外调整：

- 宿主机网络受限，无法拉取依赖或访问模型服务
- 宿主机内存较小，需要主动下调并发参数
- 你明确知道自己要接入不同的外部代理或企业内网环境

## 常用运维命令

查看状态：

```bash
docker compose ps
```

查看日志：

```bash
docker compose logs -f --tail=200
```

重新构建：

```bash
docker compose up -d --build --force-recreate
```

停止服务：

```bash
docker compose down
```

## 常见问题

### 构建失败

- 先确认 Docker Engine 已启动
- 再确认宿主机可以访问 PyPI、Maven Central、DeepSeek、DashScope
- 若是首次构建，依赖下载和模型准备会明显更慢

### 健康检查未通过

优先查看：

```bash
docker compose logs -f --tail=200
```

再确认 `python-grpc` 和 `java-orchestrator` 两个容器是否都已启动。

### API Key 已填写但功能不可用

- 确认 `.env` 保存后已经重新执行 `docker compose up -d --build`
- 确认 `.env` 中没有多余引号、空格或错误换行
- 确认对应外部模型服务本身可访问

## 仓库裁剪说明

这份公开版故意不包含以下内容：

- Android 工程与更新链路
- 第三方抓取仓库
- 开发文档与架构文档
- 诊断脚本、基准脚本和实验工具
- 本地日志、缓存、输出产物和临时文件

目标只有一个：让公开用户能够以最短路径启动 Docker 后端并访问 Web 界面。
