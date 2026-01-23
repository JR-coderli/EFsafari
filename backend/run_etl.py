#!/usr/bin/env python3
"""
ETL Runner

运行 Clickflare ETL（已集成 MTG 数据合并）
支持超时中断和旧任务清理
"""
import os
import sys
import subprocess
import io
import time
import signal
from datetime import datetime
import argparse

# Add api directory to path for Redis import
import yaml
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "api"))
from cache import set_cache, init_redis, clear_data_cache


def kill_old_etl_processes():
    """
    查找并杀死正在运行的旧 ETL 进程（包括本脚本和 cf_etl.py）
    使用 pgrep 命令，不依赖外部包
    返回被杀死的进程数量
    """
    current_pid = os.getpid()
    killed_count = 0

    try:
        # 使用 pgrep 查找 ETL 相关进程
        result = subprocess.run(
            ['pgrep', '-f', 'run_etl.py|cf_etl.py'],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            old_pids = result.stdout.strip().split('\n')
            for pid_str in old_pids:
                if not pid_str:
                    continue

                old_pid = int(pid_str)
                if old_pid == current_pid:
                    continue

                # 检查进程运行时间（通过 /proc/PID/stat）
                try:
                    with open(f'/proc/{old_pid}/stat', 'r') as f:
                        stat_data = f.read().split()
                        # starttime 是第 22 个字段（索引 21）
                        starttime_ticks = int(stat_data[21])
                        # 系统启动时间（通过 /proc/uptime 获取）
                        with open('/proc/uptime', 'r') as u:
                            uptime_seconds = float(u.read().split()[0])
                        # Hz (通常为 100）
                        hz = 100
                        running_seconds = uptime_seconds - (starttime_ticks / hz)

                        # 只杀死运行超过 20 分钟的进程
                        if running_seconds > 1200:
                            print(f"[KILL] Found old ETL process PID={old_pid}, running {int(running_seconds)}s, killing...")
                            os.kill(old_pid, signal.SIGKILL)
                            killed_count += 1
                        else:
                            print(f"[SKIP] Recent ETL process PID={old_pid}, running {int(running_seconds)}s, keeping")

                except (FileNotFoundError, ProcessLookupError, ValueError, IndexError):
                    # 进程可能已经退出
                    pass

        if killed_count > 0:
            print(f"[KILL] Killed {killed_count} old ETL process(es)")
            time.sleep(2)  # 等待进程完全退出
        else:
            print("[INFO] No old ETL processes found (or all are recent)")

    except FileNotFoundError:
        print("[WARN] pgrep command not found, skipping old process check")
    except Exception as e:
        print(f"[WARN] Error checking old processes: {e}")

    return killed_count


def run_etl(date: str) -> tuple[bool, dict]:
    """
    运行 Clickflare ETL（已集成 MTG 数据合并）

    Returns:
        (success, summary_dict) 其中 summary_dict 包含 revenue 和 spend
    """
    print("=" * 60)
    print("Running Clickflare ETL (with MTG integration)")
    print("=" * 60)

    etl_dir = os.path.join(os.path.dirname(__file__), "clickflare_etl")
    etl_script = os.path.join(etl_dir, "cf_etl.py")

    if not os.path.exists(etl_script):
        print(f"Error: ETL script not found: {etl_script}")
        return False, {"revenue": 0, "spend": 0}

    try:
        env = os.environ.copy()
        env['PYTHONUNBUFFERED'] = '1'

        process = subprocess.Popen(
            [sys.executable, etl_script, "-d", date],
            cwd=etl_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env
        )

        # 实时打印并捕获输出
        output_buffer = io.StringIO()
        for line in process.stdout:
            print(line, end='')
            output_buffer.write(line)

        process.wait()
        result_output = output_buffer.getvalue()

        # Parse SUMMARY line: "SUMMARY: revenue=XXX, spend=XXX, mtg_all_success=True/False"
        summary = {"revenue": 0, "spend": 0, "mtg_all_success": True}
        for line in result_output.split('\n'):
            if line.startswith('SUMMARY: revenue='):
                try:
                    parts = line.split()
                    summary["revenue"] = float(parts[0].split('=')[1])
                    summary["spend"] = float(parts[1].split('=')[1].rstrip(','))
                    # 解析 mtg_all_success
                    if len(parts) >= 3 and 'mtg_all_success=' in parts[2]:
                        mtg_success_str = parts[2].split('=')[1]
                        summary["mtg_all_success"] = mtg_success_str.lower() == 'true'
                except Exception as e:
                    print(f"[WARN] Failed to parse SUMMARY line: {e}")
                break

        return process.returncode == 0, summary

    except Exception as e:
        print(f"Error running ETL: {e}")
        return False, {"revenue": 0, "spend": 0}


def main():
    # Step 0: 杀死旧的 ETL 进程
    print("=" * 60)
    print("Checking for old ETL processes...")
    print("=" * 60)
    kill_old_etl_processes()

    # Initialize Redis for ETL status
    config_path = os.path.join(os.path.dirname(__file__), "api", "config.yaml")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    redis_config = config.get("redis", {})
    init_redis(redis_config)

    parser = argparse.ArgumentParser(description="ETL Runner")
    parser.add_argument(
        "-d", "--date",
        type=str,
        help="Report date in YYYY-MM-DD format (default: yesterday)"
    )
    args = parser.parse_args()

    # Determine date to process
    if args.date:
        try:
            datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print(f"Error: Invalid date format '{args.date}'. Use YYYY-MM-DD.")
            sys.exit(1)
        report_date = args.date
    else:
        # Default to yesterday
        report_date = (datetime.now() - __import__('datetime').timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"No date specified, using yesterday: {report_date}")

    # Run ETL
    success, summary = run_etl(report_date)

    # Summary
    print("\n" + "=" * 60)
    print("ETL SUMMARY")
    print("=" * 60)
    print(f"Report Date:       {report_date}")
    print(f"Status:            {'✓ SUCCESS' if success else '✗ FAILED'}")
    print(f"  - Revenue:       ${summary['revenue']:,.2f}")
    print(f"  - Spend:         ${summary['spend']:,.2f}")
    print("=" * 60)

    # Save ETL status to Redis (24h TTL)
    last_update = datetime.now().strftime("%Y-%m-%d %H:%M")
    etl_status = {
        "last_update": last_update,
        "report_date": report_date,
        "success": success,
        "all_success": summary.get("mtg_all_success", True),
        "revenue": summary["revenue"],
        "spend": summary["spend"]
    }
    set_cache("etl:last_update", etl_status, ttl=24*3600)
    # Clear data cache after ETL
    clear_data_cache()
    print("Data cache cleared after ETL success")
    print(f"ETL status saved to Redis: {last_update}")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
