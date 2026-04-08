#!/bin/bash
# Docker Entrypoint for Tushare MCP Server
# 支持 stdio 和 HTTP SSE 两种模式

set -e

# 默认配置
SERVER_MODE="${SERVER_MODE:-http}"
PORT="${PORT:-8000}"
HOST="${HOST:-0.0.0.0}"

echo "========================================"
echo "Tushare MCP Server Docker Entry Point"
echo "========================================"
echo "运行模式：$SERVER_MODE"
echo "监听地址：$HOST:$PORT"
echo ""

# 检查 TUSHARE_TOKEN 环境变量
if [ -z "$TUSHARE_TOKEN" ]; then
    echo "[警告] 未设置 TUSHARE_TOKEN 环境变量"
    echo "[提示] 部分功能可能无法使用，请通过 -e TUSHARE_TOKEN=xxx 设置"
    echo ""
else
    echo "[信息] 已配置 TUSHARE_TOKEN"
    # 如果 .env 文件不存在，创建它
    if [ ! -f "/app/.env" ]; then
        echo "TUSHARE_TOKEN=$TUSHARE_TOKEN" > /app/.env
        echo "[信息] 已创建 /app/.env 文件"
    fi
fi

# 检查缓存目录
if [ ! -d "/app/.cache" ]; then
    mkdir -p /app/.cache
    echo "[信息] 已创建缓存目录 /app/.cache"
fi

echo ""
echo "========================================"
echo "启动服务器..."
echo "========================================"

# 根据模式启动服务器
case "$SERVER_MODE" in
    stdio)
        echo "[信息] 以 stdio 模式启动（用于 Claude Desktop）"
        exec python server.py
        ;;
    http|sse)
        echo "[信息] 以 HTTP SSE 模式启动（用于调试和远程访问）"
        exec uvicorn server_http:app --host "$HOST" --port "$PORT"
        ;;
    *)
        echo "[错误] 未知的运行模式：$SERVER_MODE"
        echo "[提示] 有效模式：stdio, http, sse"
        exit 1
        ;;
esac
