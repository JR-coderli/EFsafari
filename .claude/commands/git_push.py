#!/usr/bin/env python3
"""
Git push command for Claude Code
Automatically stages, commits, and pushes changes to GitHub
"""
import subprocess
import sys
from datetime import datetime

def run_command(cmd):
    """Run a shell command and return success status."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd='E:/code/bicode')
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
        return result.returncode == 0
    except Exception as e:
        print(f"Error: {e}")
        return False

def main():
    # Get current time for commit message
    now = datetime.now()
    commit_msg = f"{now.year}年{now.month}月{now.day}日 {now.hour:02d}:{now.minute:02d}:{now.second:02d}"

    print(f"📦 Pushing changes to GitHub...")
    print(f"📝 Commit message: {commit_msg}")

    # Check for git repo
    if not run_command("git rev-parse --git-dir > nul 2>&1"):
        print("❌ Not a git repository")
        sys.exit(1)

    # Add all changes
    print("➕ Adding changes...")
    if not run_command("git add ."):
        print("❌ Failed to add changes")
        sys.exit(1)

    # Commit
    print(f"💾 Creating commit...")
    commit_cmd = f'git commit -m "{commit_msg}"'
    if not run_command(commit_cmd):
        # Check if there's nothing to commit
        result = subprocess.run("git status --short", shell=True, capture_output=True, text=True, cwd='E:/code/bicode')
        if not result.stdout.strip():
            print("ℹ️  Nothing to commit (working tree clean)")
            sys.exit(0)
        print("❌ Failed to create commit")
        sys.exit(1)

    # Push
    print("🚀 Pushing to origin/main...")
    if not run_command("git push origin main"):
        print("❌ Failed to push")
        sys.exit(1)

    print("✅ Successfully pushed to GitHub!")

if __name__ == "__main__":
    main()
