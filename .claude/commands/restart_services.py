"""
重启前后端服务脚本
清理进程、缓存，然后启动服务
"""
import subprocess
import time
import os
import sys
from pathlib import Path

# 项目路径
PROJECT_ROOT = Path("E:/code/bicode")
BACKEND_DIR = PROJECT_ROOT / "backend"
FRONTEND_DIR = PROJECT_ROOT / "EflowJRbi"

def run_command(cmd, description):
    """运行命令并打印输出"""
    print(f"\n{'='*60}")
    print(f"[执行] {description}")
    print(f"[命令] {cmd}")
    print(f"{'='*60}")

    try:
        if sys.platform == "win32":
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='ignore'
            )
        else:
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True
            )

        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr)

        return result.returncode == 0
    except Exception as e:
        print(f"[错误] {e}")
        return False

def main():
    print("\n" + "="*60)
    print("       重启前后端服务 - 清理进程和缓存")
    print("="*60)

    # 1. 杀死 Node.js 进程
    print("\n[1/6] 杀死所有 Node.js 进程...")
    if sys.platform == "win32":
        run_command("taskkill /F /IM node.exe /T", "杀死 Node.js 进程")
    else:
        run_command("pkill -9 node", "杀死 Node.js 进程")

    # 2. 杀死 Python 进程
    print("\n[2/6] 杀死所有 Python 进程...")
    if sys.platform == "win32":
        run_command("taskkill /F /IM python.exe /T", "杀死 Python 进程")
    else:
        run_command("pkill -9 python", "杀死 Python 进程")

    time.sleep(2)

    # 3. 清理 Python 缓存
    print("\n[3/6] 清理 Python 缓存...")
    for cache_dir in BACKEND_DIR.rglob("__pycache__"):
        try:
            print(f"删除: {cache_dir}")
            os.removedirs(cache_dir)
        except:
            pass

    # 删除 *.pyc 文件
    for pyc_file in BACKEND_DIR.rglob("*.pyc"):
        try:
            print(f"删除: {pyc_file}")
            pyc_file.unlink()
        except:
            pass

    # 4. 清理 Vite 缓存
    print("\n[4/6] 清理 Vite 缓存...")
    vite_cache = FRONTEND_DIR / "node_modules" / ".vite"
    if vite_cache.exists():
        run_command(f'rm -rf "{vite_cache}"', "删除 Vite 缓存" if sys.platform != "win32" else "rmdir /s /q")
    else:
        print("Vite 缓存目录不存在，跳过")

    # 5. 启动后端
    print("\n[5/6] 启动后端 FastAPI 服务...")
    backend_cmd = f'cd "{BACKEND_DIR}" && python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload'
    print(f"后台运行: {backend_cmd}")

    if sys.platform == "win32":
        subprocess.Popen(
            backend_cmd,
            shell=True,
            cwd=BACKEND_DIR,
            creationflags=subprocess.CREATE_NEW_CONSOLE
        )
    else:
        subprocess.Popen(
            backend_cmd,
            shell=True,
            cwd=BACKEND_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

    time.sleep(3)

    # 6. 启动前端
    print("\n[6/6] 启动前端 Vite 服务...")
    frontend_cmd = f'cd "{FRONTEND_DIR}" && npm run dev'
    print(f"后台运行: {frontend_cmd}")

    if sys.platform == "win32":
        subprocess.Popen(
            frontend_cmd,
            shell=True,
            cwd=FRONTEND_DIR,
            creationflags=subprocess.CREATE_NEW_CONSOLE
        )
    else:
        subprocess.Popen(
            frontend_cmd,
            shell=True,
            cwd=FRONTEND_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

    print("\n" + "="*60)
    print("       服务启动完成！")
    print("       后端: http://localhost:8000")
    print("       前端: http://localhost:3001")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
