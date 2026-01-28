---
description: 重启前后端服务，清理进程和缓存
execution: python E:/code/bicode/.claude/commands/restart_services.py
---

重启前后端服务。执行以下步骤：
1. 杀死所有 Node.js 和 Python 进程（释放端口）
2. 清理 Python 缓存 (__pycache__, *.pyc)
3. 清理 Vite 缓存 (node_modules/.vite)
4. 启动后端 FastAPI 服务 (端口 8000) - 从 backend 目录启动，模块: api.main:app
5. 启动前端 Vite 服务 (端口 3000/3001/3002，自动寻找可用端口)

项目目录：E:/code/bicode
后端目录：E:/code/bicode/backend (启动: python -m uvicorn api.main:app)
前端目录：E:/code/bicode/EflowJRbi (启动: npm run dev)
