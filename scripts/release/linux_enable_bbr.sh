#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "请使用 root 执行：sudo bash scripts/release/linux_enable_bbr.sh"
  exit 1
fi

echo "[bbr] 检查内核版本"
uname -r

echo "[bbr] 加载 tcp_bbr 模块（若已内置会自动忽略）"
modprobe tcp_bbr || true

SYSCTL_FILE="/etc/sysctl.d/99-v2m-quic-bbr.conf"
cat > "${SYSCTL_FILE}" <<'EOF'
# 拥塞控制与队列
net.core.default_qdisc=fq
net.ipv4.tcp_congestion_control=bbr

# 发送/接收窗口与缓冲区
net.core.rmem_max=33554432
net.core.wmem_max=33554432
net.ipv4.tcp_rmem=4096 87380 33554432
net.ipv4.tcp_wmem=4096 65536 33554432
net.ipv4.tcp_window_scaling=1

# KeepAlive，降低长连接被中间设备回收概率
net.ipv4.tcp_keepalive_time=600
net.ipv4.tcp_keepalive_intvl=30
net.ipv4.tcp_keepalive_probes=5

# QUIC/UDP 场景常用缓存
net.ipv4.udp_rmem_min=16384
net.ipv4.udp_wmem_min=16384

# 连接跟踪超时（按需开启）
net.netfilter.nf_conntrack_udp_timeout=30
net.netfilter.nf_conntrack_udp_timeout_stream=180
EOF

echo "[bbr] 应用 sysctl"
sysctl --system >/dev/null

echo "[bbr] 验证关键参数"
sysctl net.ipv4.tcp_congestion_control
sysctl net.core.default_qdisc
sysctl net.ipv4.tcp_keepalive_time
sysctl net.ipv4.tcp_keepalive_intvl
sysctl net.ipv4.tcp_keepalive_probes

echo "[bbr] 完成。建议重启网关容器后复测。"
