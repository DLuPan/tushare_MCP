#!/bin/bash
# Tushare MCP Server HTTP SSE 模式启动脚本 (Linux/macOS)
#
# 使用方式:
#   chmod +x start_http_server.sh
#   ./start_http_server.sh

# 切换到脚本所在目录
cd "$(dirname "$0")" || exit 1

echo "========================================"
echo "Tushare MCP Server (HTTP SSE Mode)"
echo "========================================"
echo ""

# 检查依赖
echo "[1/4] 检查依赖和环境..."
if ! command -v uvicorn &> /dev/null; then
    echo "[错误] uvicorn 未安装！"
    echo "请运行: pip install -r requirements.txt"
    exit 1
fi
echo "[成功] uvicorn 已安装"
echo ""

# 检查端口占用
echo "[2/4] 检查端口占用 (8000)..."
if command -v lsof &> /dev/null && lsof -Pi :8000 -sTCP:LISTEN -t &> /dev/null; then
    echo "[警告] 端口 8000 已被占用！"
    echo ""
    
    # 获取占用端口的进程信息
    PID=$(lsof -Pi :8000 -sTCP:LISTEN -t)
    PROCESS_NAME=$(ps -p $PID -o comm=)
    
    echo "占用端口的进程信息:"
    echo "  PID: $PID"
    echo "  进程名: $PROCESS_NAME"
    echo ""
    
    lsof -Pi :8000 -sTCP:LISTEN
    echo ""
    
    echo "请选择操作:"
    echo "  1. 终止占用端口的进程并继续启动"
    echo "  2. 取消启动（手动处理）"
    echo ""
    
    read -p "请输入选择 (1 或 2): " CHOICE
    
    if [ "$CHOICE" = "1" ]; then
        echo ""
        echo "正在终止进程 PID: $PID ..."
        kill -9 $PID &> /dev/null
        
        if [ $? -eq 0 ]; then
            echo "[成功] 进程已终止"
            sleep 2
        else
            echo "[错误] 无法终止进程，可能需要 sudo 权限"
            echo "请使用 sudo 运行此脚本，或手动终止进程后重试:"
            echo "  sudo kill -9 $PID"
            exit 1
        fi
    else
        echo ""
        echo "[取消] 启动已取消"
        echo ""
        echo "您可以手动终止占用端口的进程:"
        echo "  kill -9 $PID"
        echo ""
        echo "或者修改 server_http.py 使用其他端口"
        exit 0
    fi
    echo ""
else
    echo "[信息] 端口 8000 未被占用"
    echo ""
fi

# 检查 Python 虚拟环境
echo "[3/4] 检查 Python 环境..."
if [ -n "$VIRTUAL_ENV" ]; then
    echo "[信息] 已激活虚拟环境: $VIRTUAL_ENV"
else
    echo "[警告] 未检测到虚拟环境，建议在虚拟环境中运行"
fi
echo ""

# 启动服务器
echo "[4/4] 启动 HTTP SSE 服务器..."
echo ""
echo "========================================"
echo "服务器信息:"
echo "  - SSE 端点:    http://127.0.0.1:8000/sse"
echo "  - 健康检查:    http://127.0.0.1:8000/health"
echo "  - 工具列表:    http://127.0.0.1:8000/tools"
echo "  - 消息端点:    http://127.0.0.1:8000/messages"
echo ""
echo "按 Ctrl+C 停止服务器"
echo "========================================"
echo ""

uvicorn server_http:app --host 127.0.0.1 --port 8000

