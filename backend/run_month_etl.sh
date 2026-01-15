#!/bin/bash
###############################################################################
# 月度数据 ETL 批量拉取脚本
# 用途：批量拉取指定日期范围的 ETL 数据
###############################################################################

# ==================== 配置区域 ====================

# 开始日期 (格式: YYYY-MM-DD)
START_DATE="2024-12-01"

# 结束日期 (格式: YYYY-MM-DD)
END_DATE="2024-12-31"

# ETL 脚本路径 (相对或绝对路径)
ETL_SCRIPT="backend/run_etl.py"

# 是否显示每日进度 (true/false)
SHOW_PROGRESS=true

# 每日任务间隔秒数 (0=无间隔，用于避免服务器压力)
SLEEP_INTERVAL=0

# ====================================================

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查 ETL 脚本是否存在
if [ ! -f "$ETL_SCRIPT" ]; then
    print_error "ETL 脚本不存在: $ETL_SCRIPT"
    print_info "请确保脚本路径正确，或在项目根目录下运行此脚本"
    exit 1
fi

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--start)
            START_DATE="$2"
            shift 2
            ;;
        -e|--end)
            END_DATE="$2"
            shift 2
            ;;
        --script)
            ETL_SCRIPT="$2"
            shift 2
            ;;
        --no-progress)
            SHOW_PROGRESS=false
            shift
            ;;
        --sleep)
            SLEEP_INTERVAL="$2"
            shift 2
            ;;
        -h|--help)
            echo "用法: $0 [选项]"
            echo ""
            echo "选项:"
            echo "  -s, --start DATE      开始日期 (YYYY-MM-DD)"
            echo "  -e, --end DATE        结束日期 (YYYY-MM-DD)"
            echo "      --script PATH     ETL 脚本路径"
            echo "      --no-progress     不显示每日进度"
            echo "      --sleep SECONDS   每日任务间隔秒数"
            echo "  -h, --help            显示帮助信息"
            echo ""
            echo "示例:"
            echo "  # 使用脚本内配置的日期范围"
            echo "  $0"
            echo ""
            echo "  # 指定日期范围"
            echo "  $0 -s 2024-01-01 -e 2024-01-31"
            echo ""
            echo "  # 后台运行（不占用终端）"
            echo "  nohup $0 -s 2024-01-01 -e 2024-01-31 > etl_log.txt 2>&1 &"
            exit 0
            ;;
        *)
            print_error "未知选项: $1"
            echo "使用 -h 或 --help 查看帮助"
            exit 1
            ;;
    esac
done

# 验证日期格式
validate_date() {
    local date=$1
    if ! [[ "$date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        print_error "日期格式错误: $date (应为 YYYY-MM-DD)"
        exit 1
    fi
}

validate_date "$START_DATE"
validate_date "$END_DATE"

# 显示配置信息
echo "=============================================="
echo "      月度 ETL 数据批量拉取工具"
echo "=============================================="
print_info "开始日期: $START_DATE"
print_info "结束日期: $END_DATE"
print_info "ETL 脚本: $ETL_SCRIPT"
[ "$SLEEP_INTERVAL" -gt 0 ] && print_warn "每日间隔: $SLEEP_INTERVAL 秒"
echo "=============================================="
echo ""

# 统计变量
total_days=0
success_days=0
failed_days=0
failed_dates=()

# 主循环
current="$START_DATE"
while [[ "$current" < "$END_DATE" ]] || [[ "$current" == "$END_DATE" ]]; do
    total_days=$((total_days + 1))

    if [ "$SHOW_PROGRESS" = true ]; then
        echo ""
        print_info "[$total_days] 处理日期: $current"
    fi

    # 执行 ETL
    if python "$ETL_SCRIPT" -d "$current"; then
        success_days=$((success_days + 1))
        if [ "$SHOW_PROGRESS" = true ]; then
            echo -e "  ${GREEN}✓ 成功${NC}"
        fi
    else
        failed_days=$((failed_days + 1))
        failed_dates+=("$current")
        if [ "$SHOW_PROGRESS" = true ]; then
            echo -e "  ${RED}✗ 失败${NC}"
        fi
    fi

    # 检查是否到达结束日期
    if [[ "$current" == "$END_DATE" ]]; then
        break
    fi

    # 计算下一天
    current=$(date -d "$current +1 day" +%Y-%m-%d)

    # 间隔等待
    if [ "$SLEEP_INTERVAL" -gt 0 ] && [[ "$current" < "$END_DATE" ]]; then
        sleep "$SLEEP_INTERVAL"
    fi
done

# 打印汇总
echo ""
echo "=============================================="
echo "                  执行汇总"
echo "=============================================="
print_info "总天数: $total_days"
print_info "成功: $success_days"
if [ $failed_days -gt 0 ]; then
    print_error "失败: $failed_days"
    echo ""
    echo "失败日期:"
    for date in "${failed_dates[@]}"; do
        echo "  - $date"
    done
else
    print_info "失败: $failed_days"
fi
echo "=============================================="

# 返回码：有失败则返回 1
if [ $failed_days -gt 0 ]; then
    exit 1
fi

exit 0
