# videoToMarkdown Docker Release 指南

本文用于指导你完成两件事：
1. 在当前仓库里构建可分发的 Docker Release 压缩包。
2. 在目标机器上解压并一键启动服务。

## 1. 本地构建 Release 包

### 1.1 前置条件
- Windows PowerShell 5+ 或 PowerShell 7+
- 已安装 Docker Desktop（仅在你需要本机验证启动时）

### 1.2 执行打包脚本
在仓库根目录执行：

```powershell
powershell -ExecutionPolicy Bypass -File scripts/release/build_docker_release_bundle.ps1
```

可选参数：

```powershell
powershell -ExecutionPolicy Bypass -File scripts/release/build_docker_release_bundle.ps1 -Version v0.1.0
```

脚本输出示例：
- `release_name=videoToMarkdown-docker-release-v0.1.0`
- `release_dir=.../var/releases/videoToMarkdown-docker-release-v0.1.0`
- `release_zip=.../var/releases/videoToMarkdown-docker-release-v0.1.0.zip`

### 1.3 脱敏说明
打包脚本会自动对发布包内配置做脱敏：
- `config/video_config.yaml` 中 `bearer_token` 和 `api_key` 会清空。
- `config/module2_config.yaml` 中 `api_key` 会清空。

这样可以避免把本机密钥带到对外发布包里。

## 2. 在目标机器使用 Release 包

### 2.1 解压
把 `videoToMarkdown-docker-release-<version>.zip` 解压到任意目录。

### 2.2 配置环境变量
在解压目录执行：

```powershell
copy .env.example .env
```

`release` 包中的 `.env` 是统一入口，`python-grpc` 和 `java-orchestrator` 都会从这里读取环境变量：
- `DEEPSEEK_API_KEY`：必填；`java-orchestrator` 的 DeepSeek advisor 和 `python-grpc` 的文本增强链路都会使用。
- `DASHSCOPE_API_KEY`：建议填写；主要供 `python-grpc` 的 VL 视频分析链路使用。
- `VISION_AI_BEARER_TOKEN`：可选；仅在需要 ERNIE Vision 具象校验时填写。

`.env` 建议保存为 `UTF-8` 无 BOM 纯文本；Docker 对首个变量名较敏感，带 BOM 时可能导致首个 key 注入失败。

推荐至少填写：

```env
DEEPSEEK_API_KEY=你的_deepseek_key
DASHSCOPE_API_KEY=你的_dashscope_key
VISION_AI_BEARER_TOKEN=
```

其余推荐参数保持模板默认值即可：
- `MODULE2_DEEPSEEK_CONCURRENCY_INITIAL`：全局并发初始值（建议 `56`）
- `MODULE2_DEEPSEEK_CONCURRENCY_MIN`：全局并发最小值（建议 `8`）
- `MODULE2_DEEPSEEK_CONCURRENCY_MAX`：全局并发上限（建议 `64`）
- `MODULE2_DEEPSEEK_CONCURRENCY_WINDOW_SIZE`：并发调节滑窗（建议 `30`）
- `MODULE2_MARKDOWN_SECTION_MAX_INFLIGHT`：Markdown 增强并发上限（建议 `56`）
- `MODULE2_KC_MULTI_CHUNK_MAX_INFLIGHT`：KC multi-unit 并发上限（建议 `48`）
- `MODULE2_SEMANTIC_SEGMENT_BATCH_MAX_CONCURRENCY`：语义分段并发上限（建议 `48`）

如果你在服务启动后才修改 `.env`，需要重新创建容器，变量才会重新注入：

```powershell
powershell -ExecutionPolicy Bypass -File scripts/release/docker_release.ps1 -Action down
powershell -ExecutionPolicy Bypass -File scripts/release/docker_release.ps1 -Action up
```

### 2.3 启动服务
在解压目录执行：

```powershell
powershell -ExecutionPolicy Bypass -File scripts/release/docker_release.ps1 -Action up
```

### 2.4 验证服务

```powershell
powershell -ExecutionPolicy Bypass -File scripts/release/docker_release.ps1 -Action ps
curl http://localhost:8080/api/health
```

### 2.5 常用运维命令

```powershell
# 查看日志
powershell -ExecutionPolicy Bypass -File scripts/release/docker_release.ps1 -Action logs

# 重启
powershell -ExecutionPolicy Bypass -File scripts/release/docker_release.ps1 -Action restart

# 停止并清理容器
powershell -ExecutionPolicy Bypass -File scripts/release/docker_release.ps1 -Action down
```

## 3. 端口与目录约定
- Java API: `8080`
- Python gRPC: `50051`
- 配置目录映射：`./config -> /app/config`
- 运行产物映射：`./var -> /app/var`

## 4. 常见问题

### 4.1 提示 Docker engine is not running
先启动 Docker Desktop，再重试脚本。

### 4.2 启动后部分能力不可用
优先检查 `.env` 中密钥是否填写。

### 4.3 首次启动慢
首次构建镜像会下载依赖，属于正常现象；后续会快很多。
