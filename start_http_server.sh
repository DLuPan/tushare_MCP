#!/bin/bash
# Tushare MCP Server HTTP SSE 模式启动脚本 (Linux/macOS)
#
# 使用方式:
#   chmod +x start_http_server.sh
#   ./start_http_server.sh              # 后台启动（默认）
#   ./start_http_server.sh foreground   # 前台启动
#   ./start_http_server.sh stop         # 停止后台服务
#   ./start_http_server.sh restart      # 重启服务
#   ./start_http_server.sh status       # 查看服务状态

# 切换到脚本所在目录
cd "$(dirname "$0")" || exit 1

# 配置文件
VENV_BIN="$(pwd)/.venv/bin"
PID_FILE="$(pwd)/.server.pid"
LOG_FILE="$(pwd)/.server.log"

# 函数：检查 .venv 环境
check_venv() {
    if [ ! -d "$VENV_BIN" ]; then
        echo "[错误] 未找到 .venv 虚拟环境！"
        echo "请先运行：uv venv 或 uv sync"
        exit 1
    fi

    if [ ! -x "$VENV_BIN/uvicorn" ]; then
        echo "[错误] .venv 环境中未找到 uvicorn！"
        echo "请运行：uv sync 或 uv pip install uvicorn"
        exit 1
    fi
}

# 函数：检查端口占用
check_port() {
    local PORT=$1
    local PID=""

    # 尝试 lsof
    if command -v lsof &> /dev/null; then
        PID=$(lsof -Pi :$PORT -sTCP:LISTEN -t 2>/dev/null | head -1)
        if [ -n "$PID" ]; then
            echo "$PID"
            return 0
        fi
    fi

    # 尝试 ss (Linux)
    if command -v ss &> /dev/null; then
        PID=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
        if [ -n "$PID" ]; then
            echo "$PID"
            return 0
        fi
    fi

    # 尝试 netstat (备选)
    if command -v netstat &> /dev/null; then
        PID=$(netstat -tlnp 2>/dev/null | grep ":$PORT " | awk '{print $7}' | cut -d'/' -f1 | head -1)
        if [ -n "$PID" ] && [ "$PID" != "-" ]; then
            echo "$PID"
            return 0
        fi
    fi

    echo ""
    return 1
}

# 函数：获取进程名称
get_process_name() {
    local PID=$1
    if [ -n "$PID" ]; then
        ps -p "$PID" -o comm= 2>/dev/null || echo "unknown"
    else
        echo "unknown"
    fi
}
# 函数：获取进程状态
get_server_status() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "$PID"
            return 0
        fi
    fi
    echo ""
    return 1
}

# 命令：停止服务
stop_server() {
    echo "[信息] 正在停止服务器..."
    PID=$(get_server_status)
    if [ -n "$PID" ]; then
        kill "$PID" 2>/dev/null
        sleep 2
        if ps -p "$PID" > /dev/null 2>&1; then
            kill -9 "$PID" 2>/dev/null
        fi
        rm -f "$PID_FILE"
        echo "[成功] 服务器已停止 (PID: $PID)"
    else
        echo "[信息] 服务器未运行"
    fi
}

# 命令：启动服务
start_server() {
    # 检查是否已在运行
    EXISTING_PID=$(get_server_status)
    if [ -n "$EXISTING_PID" ]; then
        echo "[警告] 服务器已在运行 (PID: $EXISTING_PID)"
        echo ""
        echo "请选择操作:"
        echo "  1. 停止现有服务并重新启动"
        echo "  2. 取消启动"
        echo ""
        read -p "请输入选择 (1 或 2): " CHOICE

        if [ "$CHOICE" = "1" ]; then
            stop_server
        else
            echo "[取消] 启动已取消"
            exit 0
        fi
    fi

    # 检查端口占用
    OCCUPYING_PID=$(check_port 8000)
    if [ -n "$OCCUPYING_PID" ]; then
        echo "[警告] 端口 8000 已被占用！"
        echo ""

        PROCESS_NAME=$(get_process_name "$OCCUPYING_PID")

        echo "占用端口的进程信息:"
        echo "  PID: $OCCUPYING_PID"
        echo "  进程名：$PROCESS_NAME"
        echo ""

        echo "请选择操作:"
        echo "  1. 终止占用端口的进程并继续启动"
        echo "  2. 取消启动（手动处理）"
        echo ""

        read -p "请输入选择 (1 或 2): " CHOICE

        if [ "$CHOICE" = "1" ]; then
            echo ""
            echo "正在终止进程 PID: $OCCUPYING_PID ..."
            kill -9 "$OCCUPYING_PID" &> /dev/null

            if [ $? -eq 0 ]; then
                echo "[成功] 进程已终止"
                sleep 2
            else
                echo "[错误] 无法终止进程，可能需要 sudo 权限"
                echo "请使用 sudo 运行此脚本，或手动终止进程后重试:"
                echo "  sudo kill -9 $OCCUPYING_PID"
                exit 1
            fi
        else
            echo ""
            echo "[取消] 启动已取消"
            exit 0
        fi
        echo ""
    fi

    # 启动服务器（后台模式）
    echo "[信息] 启动 HTTP SSE 服务器（后台模式）..."

    # 配置虚拟环境
    export VIRTUAL_ENV="$(pwd)/.venv"
    export PATH="$VENV_BIN:$PATH"

    # 后台启动
    nohup "$VENV_BIN/uvicorn" server_http:app --host 127.0.0.1 --port 8000 > "$LOG_FILE" 2>&1 &
    SERVER_PID=$!

    # 保存 PID
    echo "$SERVER_PID" > "$PID_FILE"

    # 等待启动完成
    sleep 2

    # 检查启动状态
    if ps -p "$SERVER_PID" > /dev/null 2>&1; then
        echo "[成功] 服务器已启动"
        echo ""
        echo "========================================"
        echo "服务器信息:"
        echo "  - PID:         $SERVER_PID"
        echo "  - SSE 端点：    http://127.0.0.1:8000/sse"
        echo "  - 健康检查：    http://127.0.0.1:8000/health"
        echo "  - 工具列表：    http://127.0.0.1:8000/tools"
        echo "  - 日志文件：    $LOG_FILE"
        echo ""
        echo "停止命令：./start_http_server.sh stop"
        echo "查看日志：tail -f $LOG_FILE"
        echo "========================================"
    else
        echo "[错误] 服务器启动失败"
        echo "查看日志：cat $LOG_FILE"
        exit 1
    fi
}

# 命令：查看状态
show_status() {
    PID=$(get_server_status)
    if [ -n "$PID" ]; then
        echo "========================================"
        echo "服务器状态：运行中"
        echo "  - PID:      $PID"
        echo "  - 端口：    8000"
        echo "  - 日志：    $LOG_FILE"
        echo "========================================"

        # 显示最近的日志
        if [ -f "$LOG_FILE" ]; then
            echo ""
            echo "最近日志:"
            tail -5 "$LOG_FILE"
        fi
    else
        echo "========================================"
        echo "服务器状态：未运行"
        echo "========================================"
    fi
}

# 主逻辑
check_venv

case "${1:-start}" in
    start|"")
        echo "========================================"
        echo "Tushare MCP Server (HTTP SSE Mode)"
        echo "========================================"
        echo ""
        start_server
        ;;
    stop)
        stop_server
        ;;
    restart)
        stop_server
        echo ""
        sleep 1
        start_server
        ;;
    status)
        show_status
        ;;
    foreground)
        echo "========================================"
        echo "Tushare MCP Server (HTTP SSE Mode)"
        echo "========================================"
        echo ""
        echo "[信息] 前台模式启动 (Ctrl+C 停止)"
        echo ""

        # 配置虚拟环境
        export VIRTUAL_ENV="$(pwd)/.venv"
        export PATH="$VENV_BIN:$PATH"

        exec "$VENV_BIN/uvicorn" server_http:app --host 127.0.0.1 --port 8000
        ;;
    *)
        echo "用法：$0 {start|stop|restart|status|foreground}"
        echo ""
        echo "  start      - 后台启动服务（默认）"
        echo "  stop       - 停止服务"
        echo "  restart    - 重启服务"
        echo "  status     - 查看服务状态"
        echo "  foreground - 前台启动服务"
        exit 1
        ;;
esac
