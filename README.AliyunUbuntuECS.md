# videoToMarkdown 阿里云 Ubuntu ECS 迁移与高频更新指南

更新时间：2026-03-04  
适用仓库：`D:/videoToMarkdownTest2`  
适用场景：阿里云按量计费 ECS（Ubuntu） + Docker Compose 持续发布

---

## 1. 目标与原则

你的核心目标有 3 个：
1. 把当前 Java + Python 双服务系统迁移到阿里云 Ubuntu ECS。
2. 支持后续频繁更新（尽量低停机、可回滚）。
3. 把当前 API Key 相关配置一并迁移，并保持安全可维护。

推荐原则：
- **镜像/代码可替换**：每次更新都可快速替换。
- **配置与密钥外置**：统一放在 `.env`，不写进代码与 YAML 明文。
- **运行数据持久化**：`var/` 必须持久化，不跟随容器删除。

---

## 2. 先看迁移边界（必须迁移 vs 不需要迁移）

### 2.1 必须迁移
- 编排与构建：
  - `docker-compose.yml`
  - `deploy/docker/python-grpc.Dockerfile`
  - `deploy/docker/java-orchestrator.Dockerfile`
  - `deploy/docker/.env.example`
- 配置目录：
  - `config/video_config.yaml`
  - `config/module2_config.yaml`
  - `config/fault_detection_config.yaml`
  - `config/dictionaries.yaml`
- 运行态数据（保留历史任务与状态）：
  - `var/storage/`
  - `var/state/`（含 `collections.db`）
  - `var/cards/`
  - `var/uploads/`
  - `var/telemetry/`（如需保留审计/分析记录）
  - `var/app-updates/`（如启用 Android 更新）
- API Key 与密钥：
  - `DEEPSEEK_API_KEY`
  - `DASHSCOPE_API_KEY`（可选）
  - `VISION_AI_BEARER_TOKEN`（可选）
  - 如启用企业微信：`WECOM_*`

### 2.2 按需迁移
- `storage/`（历史目录，当前主链路已迁到 `var/storage/`，仅历史兼容时需要）。
- `var/cache/`、`var/models/`（不迁也能启动，但会增加冷启动时间和网络下载量）。

### 2.3 不需要迁移（生产运行非必需）
- `.idea/`、`.vscode/`、`tests/`、`docs/archive/` 等研发与归档资料。
- 根目录旧模板里未被当前 compose 使用的变量（例如 MySQL/RabbitMQ 旧字段）。

---

## 3. 一次性迁移：从本机到阿里云 Ubuntu ECS

## 3.1 创建 ECS 与安全组
- 建议 Ubuntu 22.04/24.04。
- 安全组最小放行：
  - `22/tcp`（SSH）
  - `80/tcp`、`443/tcp`、`443/udp`（如果你接入网关并启用 HTTPS/HTTP3）
  - 若不经过网关直出：临时放行 `8080/tcp`（生产建议只内网）
- 规格建议起步（按 CPU 路径）：
  - 最低：4 vCPU / 8 GB（轻量测试）
  - 建议：8 vCPU / 16 GB（更稳）

## 3.2 在 ECS 安装 Docker 与 Compose

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg lsb-release

sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker $USER
```

执行完成后重新登录 SSH 一次。

## 3.3 准备部署目录（建议）

```bash
sudo mkdir -p /srv/v2m
sudo chown -R $USER:$USER /srv/v2m
cd /srv/v2m
```

建议结构：
- `/srv/v2m/app`：代码与 compose
- `/srv/v2m/app/var`：运行态数据（与 compose 卷映射一致）

## 3.4 迁移代码与配置

方式 A（推荐）：服务器直接 clone 仓库。
```bash
cd /srv/v2m
git clone <你的仓库地址> app
cd app
```

方式 B：本机打包后上传（scp/rsync），保证 `docker-compose.yml + deploy/docker + config + services + apps + contracts` 完整。

## 3.5 迁移历史运行数据（如果你要保留）

从旧机器同步到新机器的 `/srv/v2m/app/var/`。
示例（在旧机器执行）：
```bash
rsync -avz --progress /path/to/old-repo/var/ <user>@<ecs-ip>:/srv/v2m/app/var/
```

---

## 4. API Key 与密钥迁移（重点）

## 4.1 生成生产 `.env`

```bash
cd /srv/v2m/app
cp deploy/docker/.env.example .env
chmod 600 .env
```

## 4.2 填写你当前在用的密钥

编辑 `.env`，至少填写：

```env
DEEPSEEK_API_KEY=你的真实key
DASHSCOPE_API_KEY=你的真实key(可选)
VISION_AI_BEARER_TOKEN=你的真实token(可选)
```

如你启用了企业微信机器人，再追加：

```env
WECOM_CALLBACK_TOKEN=...
WECOM_ENCODING_AES_KEY=...
WECOM_CORP_ID=...
WECOM_CORP_SECRET=...
WECOM_AGENT_ID=...
WECOM_RECEIVE_ID=...
ORCHESTRATOR_API_URL=http://127.0.0.1:8080/api
```

## 4.3 密钥迁移注意事项
- 不要把真实 key 写进 `config/*.yaml`。
- 不要提交 `.env` 到 Git。
- 换 key 时只改 `.env` 并重启服务即可生效。

---

## 5. 首次启动与验证

在 `/srv/v2m/app` 执行：

```bash
docker compose up -d --build
docker compose ps
```

健康检查：
```bash
curl http://127.0.0.1:8080/api/health
```

查看日志：
```bash
docker compose logs -f --tail=200
```

---

## 6. 后续高频更新（你最常用）

这里给你两种模式。你可以先用模式 A，稳定后升级模式 B。

## 6.1 模式 A：Git Pull + 本机重建（最简单）

适合先跑通，操作步骤最短：

```bash
cd /srv/v2m/app
git fetch --all
git checkout <你的分支或tag>
git pull --ff-only

docker compose up -d --build
docker compose ps
curl http://127.0.0.1:8080/api/health
```

优点：简单直接。  
缺点：每次都在 ECS 上构建，按量机器下成本更高、更新时间更长。

## 6.2 模式 B：ACR 镜像 Tag 发布（高频更新推荐）

推荐用于长期高频发布：
1. 在 CI 或本机构建镜像并推送到阿里云 ACR（例如 `python-grpc:20260304-1`、`java-orchestrator:20260304-1`）。
2. 生产机 compose 使用 `image:` 指向固定 tag。
3. 更新时只改 tag + `docker compose pull && docker compose up -d`。

优点：发布更快、回滚更稳、ECS 构建成本更低。  
缺点：初期需要多维护一个镜像仓库流程。

---

## 7. 回滚方案（必须准备）

回滚前提：每次发布记录一个版本号/tag，例如 `release-20260304-1`。

模式 A 回滚：
```bash
cd /srv/v2m/app
git checkout <上一个稳定tag>
docker compose up -d --build
curl http://127.0.0.1:8080/api/health
```

模式 B 回滚：
1. 把 compose 中镜像 tag 改回上一个稳定 tag。
2. 执行：
```bash
docker compose pull
docker compose up -d
curl http://127.0.0.1:8080/api/health
```

---

## 8. 备份与成本控制（按量计费必做）

## 8.1 最小备份集
- `/srv/v2m/app/.env`
- `/srv/v2m/app/config/`
- `/srv/v2m/app/var/state/`
- `/srv/v2m/app/var/storage/`（如果你需要保留历史产物）
- `/srv/v2m/app/var/app-updates/`（如启用安卓更新）

## 8.2 成本控制建议
- 高频发布时优先模式 B（ACR tag），减少 ECS 本机构建。
- 对外只暴露网关端口（80/443），减少无效流量与攻击面。
- 定期清理不再需要的历史任务产物（`var/storage`）和旧镜像。

---

## 9. 常见问题排查

1. `docker compose up` 后 `python-grpc` 不健康  
   - 先看日志：`docker compose logs --tail=200 python-grpc`
   - 核对 `.env` 里的 key 是否为空。
   - 核对 `config/video_config.yaml`、`config/module2_config.yaml` 是否存在且路径正确。

2. Java 服务启动了但接口报 500  
   - 看 `java-orchestrator` 日志，重点关注 gRPC 连接与 `DEEPSEEK_API_KEY`。

3. 更新后任务历史丢失  
   - 检查卷映射是否仍是 `./var:/app/var`，以及你是否误删了宿主机 `var/`。

4. 更换 API Key 后仍旧鉴权失败  
   - 确认 `.env` 已保存，执行 `docker compose up -d` 重新创建容器。
   - 避免同名环境变量在系统 shell 中有旧值覆盖。

---

## 10. 你可以直接照抄的一次性上线清单

```bash
# 1) 进机器
ssh <user>@<ecs-ip>

# 2) 安装 docker（参考第 3.2 节）

# 3) 拉代码
mkdir -p /srv/v2m && cd /srv/v2m
git clone <repo-url> app
cd app

# 4) 配置 key
cp deploy/docker/.env.example .env
chmod 600 .env
vim .env

# 5) 启动
docker compose up -d --build

# 6) 验证
docker compose ps
curl http://127.0.0.1:8080/api/health
```

如果你要，我可以下一步再给你一版“模式 B（ACR 镜像 Tag）专用 compose 模板”，让更新命令固定为两条：
- `docker compose pull`
- `docker compose up -d`
