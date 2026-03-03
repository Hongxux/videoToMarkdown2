#!/usr/bin/env bash
set -euo pipefail

TARGET_HOST="${1:-}"
if [[ -z "${TARGET_HOST}" ]]; then
  echo "用法: bash scripts/release/linux_verify_transport.sh <域名或IP>"
  exit 1
fi

echo "[verify] 内核拥塞控制与队列"
sysctl net.ipv4.tcp_congestion_control || true
sysctl net.core.default_qdisc || true

echo "[verify] 监听端口（期望 443/tcp + 443/udp）"
ss -lntup | grep -E '(:443\\s)' || true

echo "[verify] HTTP/3 握手检查（需要 curl 支持 HTTP/3）"
curl -I --http3 "https://${TARGET_HOST}" || true

echo "[verify] KeepAlive 响应头与 Alt-Svc"
curl -I "https://${TARGET_HOST}" | grep -Ei 'keep-alive|alt-svc|server' || true

echo "[verify] BBR socket 抽样"
ss -tin | grep -i bbr || true
