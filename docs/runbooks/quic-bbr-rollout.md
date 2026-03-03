# QUIC/BBR 落地清单（对齐当前仓库部署）

## 1. 当前基线
- 仓库当前发布形态是 `docker-compose.yml` 直出：
  - `java-orchestrator` 暴露 `8080/tcp`
  - `python-grpc` 暴露 `50051/tcp`
- 当前没有独立网关层，因此要启用 QUIC/HTTP3，需要先在 `java-orchestrator` 前增加网关（Caddy 或 Nginx）。

## 2. 推荐拓扑
- `Client -> Cloudflare(HTTP/3) -> Caddy/Nginx(443 tcp+udp) -> java-orchestrator:8080`
- 若暂时不接 Cloudflare：
  - `Client -> Caddy/Nginx(HTTP/3) -> java-orchestrator:8080`

## 3. 已提供文件
- Caddy 配置：`deploy/gateway/caddy/Caddyfile`
- Nginx 配置：`deploy/gateway/nginx/nginx.conf`
- Caddy 叠加 compose：`deploy/docker/docker-compose.gateway.caddy.yml`
- Nginx 叠加 compose：`deploy/docker/docker-compose.gateway.nginx.yml`
- Linux BBR/sysctl 脚本：`scripts/release/linux_enable_bbr.sh`
- Linux 验证脚本：`scripts/release/linux_verify_transport.sh`

## 4. 执行步骤（Caddy 推荐）

### 4.1 修改域名
- 将 `deploy/gateway/caddy/Caddyfile` 中的 `example.com` 改为真实域名。

### 4.2 启用主服务
```bash
docker compose up -d --build
```

### 4.3 启用网关（Caddy）
```bash
docker compose \
  -f docker-compose.yml \
  -f deploy/docker/docker-compose.gateway.caddy.yml \
  up -d gateway-caddy
```

### 4.4 Linux 主机启用 BBR 与传输参数
```bash
sudo bash scripts/release/linux_enable_bbr.sh
```

### 4.5 验证
```bash
bash scripts/release/linux_verify_transport.sh your.domain.com
```

## 5. 执行步骤（Nginx 备选）

### 5.1 准备证书
- 将证书放到：`deploy/gateway/nginx/certs/fullchain.pem` 与 `privkey.pem`
- `nginx.conf` 内 `server_name` 替换为真实域名。

### 5.2 关键注意
- Nginx 必须具备 HTTP/3 支持（`nginx -V` 中存在 `--with-http_v3_module`）。
- 若镜像无 HTTP/3，需切换到支持 QUIC 的 Nginx 构建。

### 5.3 启动
```bash
docker compose \
  -f docker-compose.yml \
  -f deploy/docker/docker-compose.gateway.nginx.yml \
  up -d gateway-nginx
```

## 6. Cloudflare 对齐参数
- SSL/TLS 模式：`Full (strict)`
- Edge Certificates：开启 `HTTP/3 (with QUIC)`
- DNS：业务域名使用代理（橙云）
- 缓存策略：
  - 对 `/api/*` 建议 `Bypass Cache`
  - 静态资源可按需缓存
- 超时策略：
  - 若存在上传超时，优先缩短单分片请求时长并提高分片并行度上限的可控值
  - 再评估 WAF/防火墙的连接空闲超时

## 7. 参数调优建议
- 应用层（已配置在 `application.properties`）：
  - `file.transfer.max-concurrent`
  - `file.transfer.copy-buffer-bytes`
  - `server.connection-timeout`
  - `server.tomcat.keep-alive-timeout`
  - `server.tomcat.max-keep-alive-requests`
- 内核层（脚本已写入）：
  - `net.ipv4.tcp_congestion_control=bbr`
  - `net.core.default_qdisc=fq`
  - `tcp_rmem/tcp_wmem`、`rmem_max/wmem_max`
  - `tcp_keepalive_*`
  - `nf_conntrack_udp_timeout*`

## 8. 回滚策略
- 关闭网关叠加服务：
```bash
docker compose \
  -f docker-compose.yml \
  -f deploy/docker/docker-compose.gateway.caddy.yml \
  down gateway-caddy
```
- 回滚 sysctl：
```bash
sudo rm -f /etc/sysctl.d/99-v2m-quic-bbr.conf
sudo sysctl --system
```

## 9. 验收标准
- `curl -I --http3 https://your.domain.com` 成功返回。
- `ss -lntup` 可见 `443/tcp` 和 `443/udp` 监听。
- `sysctl net.ipv4.tcp_congestion_control` 返回 `bbr`。
- 大文件上传链路在分片失败场景下仅重传失败分片，无整文件重传。
