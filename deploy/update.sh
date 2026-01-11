#!/bin/bash

###############################################################################
# Bicode 项目快速更新脚本
# 用于更新代码后快速部署，无需完整安装
  # 1 添加执行权限
  # chmod +x /opt/bicode/deploy/update.sh

  # 2运行脚本
  # /opt/bicode/deploy/update.sh

###############################################################################

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PROJECT_DIR="/opt/bicode"
API_PORT="8001"

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# 1. 拉取最新代码
log_info "拉取最新代码..."
cd "$PROJECT_DIR"
git pull origin main

# 2. 更新后端依赖
log_info "更新后端依赖..."
cd "$PROJECT_DIR/backend"
source venv/bin/activate
pip install -r requirements.txt

# 3. 重新构建前端
log_info "构建前端..."
cd "$PROJECT_DIR/EflowJRbi"
npm install
npm run build

# 4. 重启服务
log_info "重启服务..."
sudo systemctl restart bicode-api
sudo systemctl reload nginx || sudo systemctl restart nginx

# 等待服务启动
sleep 3

# 检查状态
if systemctl is-active --quiet bicode-api; then
    log_info "✓ 服务更新完成"
else
    log_warn "✗ 后端服务异常，请检查日志"
    sudo journalctl -u bicode-api -n 20 --no-pager
fi

echo ""
echo "访问地址:"
echo "  子域名: http://safari.eflow-media.com"
echo "  IP 地址: http://43.160.248.9"
