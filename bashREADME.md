# Bicode 服务器运维命令参考

> 服务器地址: `43.160.248.9` | 项目目录: `/opt/bicode`

---

## 目录

- [服务管理](#服务管理)
- [端口查看](#端口查看)
- [ETL 数据拉取](#etl-数据拉取)
- [日志查看](#日志查看)
- [定时任务](#定时任务)
- [更新部署](#更新部署)
- [故障排查](#故障排查)

---

## 服务管理

### 后端 API 服务

```bash
# 查看服务状态
sudo systemctl status bicode-api

# 启动服务
sudo systemctl start bicode-api

# 停止服务
sudo systemctl stop bicode-api

# 重启服务
sudo systemctl restart bicode-api

# 重新加载配置（无停机）
sudo systemctl reload bicode-api

# 开机自启动
sudo systemctl enable bicode-api

# 取消开机自启动
sudo systemctl disable bicode-api
```

### Nginx 服务

```bash
# 查看服务状态
sudo systemctl status nginx

# 启动服务
sudo systemctl start nginx

# 停止服务
sudo systemctl stop nginx

# 重启服务
sudo systemctl restart nginx

# 重新加载配置（无停机）
sudo systemctl reload nginx

# 测试配置文件
sudo nginx -t
```

### ClickHouse 服务

```bash
# 查看服务状态
sudo systemctl status clickhouse-server

# 启动服务
sudo systemctl start clickhouse-server

# 停止服务
sudo systemctl stop clickhouse-server

# 重启服务
sudo systemctl restart clickhouse-server
```

---

## 端口查看

### 查看端口占用情况

```bash
# 查看所有监听端口
ss -tulnp

# 查看特定端口（如 8001）
ss -tulnp | grep 8001

# 查看端口 8001 被哪个进程占用
lsof -i :8001

# 使用 netstat（如果已安装）
netstat -tulnp | grep 8001
```

### 项目默认端口

| 服务 | 端口 | 说明 |
|------|------|------|
| 后端 API | 8001 | FastAPI 服务 |
| Nginx | 80 | 前端静态文件 + API 反向代理 |
| ClickHouse | 8123 | HTTP 接口 |

### 杀掉占用端口的进程

```bash
# 杀掉占用 8001 端口的进程
kill -9 $(lsof -t -i:8001)

# 或者使用 fuser
fuser -k 8001/tcp
```

---

## ETL 数据拉取

### 基本用法

```bash
# 进入项目目录
cd /opt/bicode/backend

# 拉取昨天的数据（最常用）
/usr/bin/python3 run_etl.py -d $(date -d 'yesterday' +'%Y-%m-%d')

# 拉取今天的数据
/usr/bin/python3 run_etl.py -d $(date -d 'today' +'%Y-%m-%d')

# 拉取指定日期的数据
/usr/bin/python3 run_etl.py -d 2026-01-10
```

### 拉取日期范围

```bash
# 拉取最近 7 天的数据（循环执行）
for i in {0..6}; do
    DATE=$(date -d "$i days ago" +'%Y-%m-%d')
    echo "拉取日期: $DATE"
    /usr/bin/python3 run_etl.py -d "$DATE"
done

# 拉取指定日期范围（如 2026-01-01 到 2026-01-10）
for i in {0..9}; do
    DATE=$(date -d "2026-01-01 +$i days" +'%Y-%m-%d')
    echo "拉取日期: $DATE"
    /usr/bin/python3 run_etl.py -d "$DATE"
done
```

### 使用现有脚本

```bash
# 使用部署脚本中的 ETL 脚本
bash /opt/bicode/deploy/run_etl_now.sh
```

---

## 日志查看

### 后端 API 日志

```bash
# 查看实时日志（跟随输出）
sudo journalctl -u bicode-api -f

# 查看最近 50 行
sudo journalctl -u bicode-api -n 50

# 查看今天的日志
sudo journalctl -u bicode-api --since today

# 查看最近 1 小时的日志
sudo journalctl -u bicode-api --since "1 hour ago"
```

### ETL 日志

```bash
# 查看实时日志
tail -f /var/log/bicode/etl.log

# 查看最近 50 行
tail -n 50 /var/log/bicode/etl.log

# 查看今天的日志
grep "$(date +%Y-%m-%d)" /var/log/bicode/etl.log

# 查找错误信息
grep -i "error\|exception\|fail\|traceback" /var/log/bicode/etl.log

# 查看执行成功的记录
grep "ETL completed successfully" /var/log/bicode/etl.log
```

### Nginx 日志

```bash
# 访问日志
tail -f /var/log/nginx/access.log

# 错误日志
tail -f /var/log/nginx/error.log

# 查看 404 错误
grep " 404 " /var/log/nginx/access.log
```

---

## 定时任务

### 查看 Crontab

```bash
# 查看当前用户的定时任务
crontab -l

# 编辑定时任务
crontab -e

# 删除所有定时任务
crontab -r
```

### 当前 ETL 定时任务配置

```
0 9,10,11,12,13,14,20 * * * cd /opt/bicode/backend && /usr/bin/python3 run_etl.py -d $(date -d 'yesterday' +'%Y-%m-%d') >> /var/log/bicode/etl.log 2>&1
```

**执行时间**: 每天 9, 10, 11, 12, 13, 14, 20 点

### Crontab 时间格式说明

```
* * * * * 命令
│ │ │ │ │
│ │ │ │ └─── 星期几 (0-7, 0和7都代表周日)
│ │ │ └───── 月份 (1-12)
│ │ └─────── 日期 (1-31)
│ └───────── 小时 (0-23)
└─────────── 分钟 (0-59)
```

---

## 更新部署

### 快速更新（推荐）

```bash
# 使用快速更新脚本
bash /opt/bicode/deploy/update.sh
```

### 手动更新步骤

```bash
# 1. 拉取最新代码
cd /opt/bicode
git pull origin main

# 2. 更新后端依赖
cd /opt/bicode/backend
source venv/bin/activate
pip install -r requirements.txt

# 3. 重新构建前端
cd /opt/bicode/EflowJRbi
npm install
npm run build

# 4. 重启服务
sudo systemctl restart bicode-api
sudo systemctl reload nginx
```

---

## 故障排查

### 检查所有服务状态

```bash
# 一键检查所有服务
sudo systemctl status bicode-api nginx clickhouse-server
```

### 检查端口是否正常监听

```bash
# 检查所有必需端口
ss -tulnp | grep -E '8001|80|8123'
```

### 测试 API 连接

```bash
# 测试本地 API
curl http://localhost:8001/health

# 测试外部访问
curl http://43.160.248.9/api/dashboard/health
```

### 测试 ClickHouse 连接

```bash
# 测试 ClickHouse HTTP 接口
curl http://localhost:8123/ping

# 查询数据库列表
curl http://localhost:8123/ --query "SHOW DATABASES"
```

### 常见问题

**问题**: ETL 报 `ModuleNotFoundError`

```bash
# 解决方法：安装缺失的模块
/usr/bin/python3 -m pip install clickhouse_connect

# 或安装全部依赖
cd /opt/bicode/backend && /usr/bin/python3 -m pip install -r requirements.txt
```

**问题**: 端口被占用

```bash
# 查找并杀掉占用进程
kill -9 $(lsof -t -i:8001)
```

**问题**: 服务启动失败

```bash
# 查看详细错误日志
sudo journalctl -u bicode-api -n 50 --no-pager
```

---

## 快捷命令汇总

```bash
# === 一键命令 ===

# 重启所有服务
sudo systemctl restart bicode-api nginx

# 查看所有服务状态
sudo systemctl status bicode-api nginx clickhouse-server

# 拉取昨天数据
cd /opt/bicode/backend && /usr/bin/python3 run_etl.py -d $(date -d 'yesterday' +'%Y-%m-%d')

# 查看 ETL 日志
tail -n 50 /var/log/bicode/etl.log

# 查看后端日志
sudo journalctl -u bicode-api -n 50

# 检查端口占用
ss -tulnp | grep -E '8001|80|8123'
```

---

## 项目路径

| 路径 | 说明 |
|------|------|
| `/opt/bicode` | 项目根目录 |
| `/opt/bicode/backend` | 后端代码 |
| `/opt/bicode/EflowJRbi` | 前端代码 |
| `/opt/bicode/backend/api` | FastAPI 应用 |
| `/opt/bicode/backend/run_etl.py` | ETL 主入口 |
| `/var/log/bicode/etl.log` | ETL 日志 |
| `/etc/nginx/conf.d/bicode.conf` | Nginx 配置 |
| `/etc/systemd/system/bicode-api.service` | Systemd 服务配置 |

---

## 访问地址

- **前端**: http://safari.eflow-media.com 或 http://43.160.248.9
- **API (直接)**: http://43.160.248.9:8001
- **API (通过 Nginx)**: http://43.160.248.9/api/
- **API 文档**: http://43.160.248.9:8001/docs

---

*最后更新: 2026-01-11*
