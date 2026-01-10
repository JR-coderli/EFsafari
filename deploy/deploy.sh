#!/bin/bash

###############################################################################
# Bicode 项目自动部署脚本 - CentOS 9 Stream
# 服务器: 43.160.248.9
###############################################################################

set -e  # 遇到错误立即退出

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 项目配置
PROJECT_DIR="/opt/bicode"
GITHUB_REPO="https://github.com/JR-coderli/EFsafari.git"
SERVER_IP="43.160.248.9"
DB_HOST="localhost"
DB_PORT="8123"
DB_NAME="ad_platform"
API_PORT="8001"

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

###############################################################################
# 1. 检查是否为 root 用户
###############################################################################
check_root() {
    if [ "$EUID" -ne 0 ]; then
        log_error "请使用 root 用户或 sudo 运行此脚本"
        exit 1
    fi
}

###############################################################################
# 2. 安装系统依赖
###############################################################################
install_dependencies() {
    log_info "安装系统依赖..."

    dnf install -y git python3 python3-pip python3-devel nodejs npm

    # 启用 nginx 模块并安装
    dnf module enable nginx:stable -y 2>/dev/null || true
    # 使用 --setopt 绕过 exclude 限制
    dnf install --setopt=exclude= -y nginx

    # 验证安装
    python3 --version || (log_error "Python3 安装失败" && exit 1)
    node --version || (log_error "Node.js 安装失败" && exit 1)
    npm --version || (log_error "npm 安装失败" && exit 1)
    nginx -v 2>/dev/null || (log_error "nginx 安装失败" && exit 1)

    log_info "系统依赖安装完成"
}

###############################################################################
# 3. 克隆/更新代码
###############################################################################
setup_code() {
    log_info "设置项目代码..."

    # 创建项目目录
    mkdir -p "$PROJECT_DIR"

    if [ -d "$PROJECT_DIR/.git" ]; then
        log_info "更新现有代码..."
        cd "$PROJECT_DIR"
        git pull origin main
    else
        log_info "克隆代码仓库..."
        rm -rf "$PROJECT_DIR"
        git clone "$GITHUB_REPO" "$PROJECT_DIR"
    fi

    log_info "代码准备完成"
}

###############################################################################
# 4. 设置后端
###############################################################################
setup_backend() {
    log_info "设置后端..."

    cd "$PROJECT_DIR/backend"

    # 创建虚拟环境
    if [ ! -d "venv" ]; then
        log_info "创建 Python 虚拟环境..."
        python3 -m venv venv
    fi

    # 激活虚拟环境并安装依赖
    source venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt

    # 创建 .env 文件
    log_info "创建环境配置文件..."
    cat > .env << EOF
# ClickHouse 配置（本地）
CLICKHOUSE_HOST=$DB_HOST
CLICKHOUSE_PORT=$DB_PORT
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=$DB_NAME

# JWT 密钥（生产环境请修改）
JWT_SECRET_KEY=$(openssl rand -hex 32)

# API 配置
API_HOST=0.0.0.0
API_PORT=$API_PORT
EOF

    # 创建用户数据目录
    mkdir -p backend/api/users

    log_info "后端设置完成"
}

###############################################################################
# 5. 构建前端
###############################################################################
setup_frontend() {
    log_info "构建前端..."

    cd "$PROJECT_DIR/EflowJRbi"

    # 安装依赖
    npm install

    # 构建生产版本
    npm run build

    log_info "前端构建完成"
}

###############################################################################
# 6. 配置 Nginx
###############################################################################
setup_nginx() {
    log_info "配置 Nginx..."

    # 删除旧配置
    rm -f /etc/nginx/conf.d/bicode.conf

    # 创建新配置
    cat > /etc/nginx/conf.d/bicode.conf << EOF
server {
    listen 80;
    server_name safari.eflow-media.com $SERVER_IP;

    # 前端静态文件
    location / {
        root $PROJECT_DIR/EflowJRbi/dist;
        try_files \$uri \$uri/ /index.html;

        # 缓存静态资源
        location ~* \.(js|css|png|jpg|jpeg|gif|ico|svg|woff|woff2|ttf|eot)$ {
            expires 1y;
            add_header Cache-Control "public, immutable";
        }
    }

    # API 代理
    location /api/ {
        proxy_pass http://127.0.0.1:$API_PORT;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;

        # WebSocket 支持（如果需要）
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
    }

    # Gzip 压缩
    gzip on;
    gzip_types text/plain text/css application/json application/javascript text/xml application/xml application/xml+rss text/javascript;
    gzip_min_length 1000;
}
EOF

    # 测试配置
    nginx -t || (log_error "Nginx 配置错误" && exit 1)

    log_info "Nginx 配置完成"
}

###############################################################################
# 7. 配置 systemd 服务
###############################################################################
setup_systemd() {
    log_info "配置 systemd 服务..."

    # 获取当前用户（如果不是 root 运行，但脚本已检查 root）
    RUN_USER=${SUDO_USER:-$(whoami)}

    # 创建后端服务
    cat > /etc/systemd/system/bicode-api.service << EOF
[Unit]
Description=Bicode Backend API
After=network.target clickhouse-server.service

[Service]
Type=simple
User=$RUN_USER
Group=$RUN_USER
WorkingDirectory=$PROJECT_DIR/backend
Environment="PATH=$PROJECT_DIR/backend/venv/bin"
ExecStart=$PROJECT_DIR/backend/venv/bin/uvicorn api.main:app --host 0.0.0.0 --port $API_PORT
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

    # 重载 systemd
    systemctl daemon-reload

    log_info "systemd 服务配置完成"
}

###############################################################################
# 8. 配置防火墙
###############################################################################
setup_firewall() {
    log_info "配置防火墙..."

    # 检查 firewalld 是否运行
    if systemctl is-active --quiet firewalld; then
        firewall-cmd --permanent --add-service=http
        firewall-cmd --permanent --add-service=https
        firewall-cmd --permanent --add-port=$API_PORT/tcp
        firewall-cmd --reload
        log_info "防火墙规则已更新"
    else
        log_warn "firewalld 未运行，跳过防火墙配置"
    fi
}

###############################################################################
# 9. 配置定时任务（ETL）
###############################################################################
setup_cron() {
    log_info "配置 ETL 定时任务..."

    # 创建日志目录
    mkdir -p /var/log/bicode
    chown $(whoami):$(whoami) /var/log/bicode

    # 创建 crontab 条目
    # 每天 9, 10, 11, 12, 13, 14, 20 点执行，使用前一天的日期
    CRON_JOB="0 9,10,11,12,13,14,20 * * * cd $PROJECT_DIR/backend && /usr/bin/python3 run_etl.py -d \$(date -d 'yesterday' +'\%Y-\%m-\%d') >> /var/log/bicode/etl.log 2>&1"

    # 检查是否已存在
    if crontab -l 2>/dev/null | grep -q "run_etl.py"; then
        log_warn "ETL 定时任务已存在，跳过"
    else
        # 添加到 crontab（保留现有任务）
        (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
        log_info "ETL 定时任务已配置"
    fi

    log_info "定时任务配置完成"
}

###############################################################################
# 10. 启动服务
###############################################################################
start_services() {
    log_info "启动服务..."

    # 启动并启用后端 API
    systemctl enable bicode-api
    systemctl restart bicode-api

    # 启动并启用 Nginx
    systemctl enable nginx
    systemctl restart nginx

    # 等待服务启动
    sleep 3

    # 检查服务状态
    if systemctl is-active --quiet bicode-api; then
        log_info "✓ 后端 API 服务运行正常"
    else
        log_error "✗ 后端 API 服务启动失败"
        journalctl -u bicode-api -n 20 --no-pager
        exit 1
    fi

    if systemctl is-active --quiet nginx; then
        log_info "✓ Nginx 服务运行正常"
    else
        log_error "✗ Nginx 服务启动失败"
        exit 1
    fi
}

###############################################################################
# 11. 显示部署信息
###############################################################################
show_info() {
    echo ""
    echo "=========================================="
    echo -e "${GREEN}部署完成！${NC}"
    echo "=========================================="
    echo ""
    echo "访问地址:"
    echo "  子域名: http://safari.eflow-media.com"
    echo "  IP 地址: http://$SERVER_IP"
    echo ""
    echo "⚠️  确保已配置 DNS A 记录:"
    echo "   主机记录: safari"
    echo "   记录类型: A"
    echo "   记录值: 43.160.248.9"
    echo ""
    echo "服务管理命令:"
    echo "  后端服务: sudo systemctl status|restart|reload bicode-api"
    echo "  Nginx:    sudo systemctl status|restart nginx"
    echo ""
    echo "查看日志:"
    echo "  后端日志: sudo journalctl -u bicode-api -f"
    echo "  ETL 日志: tail -f /var/log/bicode/etl.log"
    echo ""
    echo "定时任务:"
    echo "  查看任务: crontab -l"
    echo "  编辑任务: crontab -e"
    echo ""
    echo "ETL 执行时间: 每天 9, 10, 11, 12, 13, 14, 20 点"
    echo ""
}

###############################################################################
# 主函数
###############################################################################
main() {
    echo "=========================================="
    echo "  Bicode 项目自动部署脚本"
    echo "  服务器: $SERVER_IP"
    echo "=========================================="
    echo ""

    check_root
    install_dependencies
    setup_code
    setup_backend
    setup_frontend
    setup_nginx
    setup_systemd
    setup_firewall
    setup_cron
    start_services
    show_info
}

# 运行主函数
main
