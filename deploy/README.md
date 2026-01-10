# Bicode 项目部署文档

## 服务器信息

- **IP 地址**: 43.160.248.9
- **子域名**: safari.eflow-media.com
- **系统**: CentOS 9 Stream
- **项目路径**: /opt/bicode

## DNS 配置

在 DNS 服务商添加 A 记录：

| 主机记录 | 记录类型 | 记录值 |
|---------|---------|--------|
| safari | A | 43.160.248.9 |

---

## 快速部署

### 方法一：自动部署（推荐）

```bash
# 1. 上传 deploy.sh 到服务器
scp deploy/deploy.sh root@43.160.248.9:/root/

# 2. SSH 登录服务器
ssh root@43.160.248.9

# 3. 添加执行权限并运行
chmod +x /root/deploy.sh
./root/deploy.sh
```

### 方法二：从 GitHub 直接运行

```bash
# SSH 登录服务器后执行
curl -fsSL https://raw.githubusercontent.com/JR-coderli/EFsafari/main/deploy/deploy.sh -o deploy.sh
chmod +x deploy.sh
./deploy.sh
```

---

## 脚本说明

| 脚本 | 说明 | 使用场景 |
|------|------|----------|
| `deploy.sh` | 完整部署脚本 | 首次部署或完全重新安装 |
| `update.sh` | 快速更新脚本 | 代码更新后快速部署 |
| `run_etl_now.sh` | 手动运行 ETL | 测试或手动拉取数据 |

---

## 部署后检查

### 1. 检查服务状态

```bash
# 后端 API 服务
sudo systemctl status bicode-api

# Nginx 服务
sudo systemctl status nginx
```

### 2. 检查日志

```bash
# 后端日志
sudo journalctl -u bicode-api -f

# ETL 日志
tail -f /var/log/bicode/etl.log
```

### 3. 测试访问

```bash
# API 健康检查
curl http://localhost:8001/health

# 或从外部访问
curl http://safari.eflow-media.com/api/health
```

### 4. 访问前端

浏览器打开: http://safari.eflow-media.com (或 http://43.160.248.9)

---

## 服务管理命令

```bash
# 后端 API
sudo systemctl start bicode-api    # 启动
sudo systemctl stop bicode-api     # 停止
sudo systemctl restart bicode-api  # 重启
sudo systemctl reload bicode-api   # 重载配置
sudo systemctl status bicode-api   # 状态

# Nginx
sudo systemctl start nginx
sudo systemctl stop nginx
sudo systemctl restart nginx
sudo systemctl reload nginx
```

---

## 定时任务 (ETL)

### 配置的执行时间

每天 **9, 10, 11, 12, 13, 14, 20** 点执行，拉取前一天的数据。

### 查看和管理定时任务

```bash
# 查看定时任务
crontab -l

# 编辑定时任务
crontab -e

# 查看 ETL 日志
tail -f /var/log/bicode/etl.log
```

### 手动运行 ETL

```bash
# 使用脚本
chmod +x /opt/bicode/run_etl_now.sh
/opt/bicode/run_etl_now.sh

# 或直接运行
cd /opt/bicode/backend
python3 run_etl.py -d 2026-01-09
```

---

## 代码更新流程

### 1. 服务器上手动更新

```bash
cd /opt/bicode
git pull origin main
./update.sh
```

### 2. 或使用快速更新脚本

```bash
# 上传 update.sh 到服务器并运行
scp deploy/update.sh root@43.160.248.9:/root/
ssh root@43.160.248.9 "chmod +x /root/update.sh && /root/update.sh"
```

---

## 环境配置

### 后端环境变量

配置文件: `/opt/bicode/backend/.env`

```bash
# ClickHouse 配置（本地）
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=ad_platform

# JWT 密钥
JWT_SECRET_KEY=your-random-secret-key

# API 配置
API_HOST=0.0.0.0
API_PORT=8001
```

### 修改 JWT 密钥

```bash
# 生成新密钥
openssl rand -hex 32

# 编辑 .env 文件
vi /opt/bicode/backend/.env

# 重启服务
sudo systemctl restart bicode-api
```

---

## 目录结构

```
/opt/bicode/
├── backend/                 # 后端代码
│   ├── api/                # API 路由
│   ├── clickflare_etl/     # Clickflare ETL
│   ├── mtg_etl/           # MTG ETL
│   ├── venv/              # Python 虚拟环境
│   ├── .env               # 环境配置
│   └── run_etl.py         # ETL 入口
├── EflowJRbi/             # 前端代码
│   └── dist/              # 构建产物
├── deploy/                # 部署脚本
│   ├── deploy.sh
│   ├── update.sh
│   └── run_etl_now.sh
└── run_etl.py            # 统一 ETL 入口
```

---

## 防火墙配置

```bash
# 查看防火墙状态
sudo firewall-cmd --list-all

# 开放端口
sudo firewall-cmd --permanent --add-service=http
sudo firewall-cmd --permanent --add-service=https
sudo firewall-cmd --permanent --add-port=8001/tcp
sudo firewall-cmd --reload
```

---

## 常见问题

### 1. 后端服务启动失败

```bash
# 查看详细日志
sudo journalctl -u bicode-api -n 50 --no-pager

# 常见原因：
# - ClickHouse 未启动
# - 端口被占用
# - Python 依赖缺失
```

### 2. 前端页面空白

```bash
# 检查 Nginx 配置
sudo nginx -t

# 检查前端构建文件
ls -la /opt/bicode/EflowJRbi/dist/

# 查看 Nginx 日志
sudo tail -f /var/log/nginx/error.log
```

### 3. ETL 执行失败

```bash
# 查看 ETL 日志
tail -f /var/log/bicode/etl.log

# 手动运行测试
cd /opt/bicode/backend
python3 run_etl.py -d $(date -d 'yesterday' +'%Y-%m-%d')
```

### 4. 定时任务不执行

```bash
# 检查 cron 服务
sudo systemctl status crond

# 查看定时任务
crontab -l

# 检查 cron 日志
sudo tail -f /var/log/cron
```

---

## 安全建议

1. **修改 JWT 密钥**: 部署后立即修改 `.env` 中的 `JWT_SECRET_KEY`
2. **配置 HTTPS**: 生产环境建议配置 SSL 证书
3. **限制 API 访问**: 使用防火墙限制 API 端口访问
4. **定期更新**: 保持系统和依赖包更新

---

## 联系方式

如有问题，请联系管理员。
