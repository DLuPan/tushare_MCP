# Tushare MCP Server Dockerfile
# 支持 stdio 和 HTTP SSE 两种运行模式

# ===========================================
# 阶段 1: 构建依赖
# ===========================================
FROM python:3.10-slim as builder

# 设置工作目录
WORKDIR /build

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装 Python 依赖到指定目录
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ===========================================
# 阶段 2: 运行环境
# ===========================================
FROM python:3.10-slim as runtime

# 设置环境变量
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# 创建非 root 用户
RUN groupadd --gid 1000 appgroup && \
    useradd --uid 1000 --gid appgroup --shell /bin/bash --create-home appuser

# 从构建阶段复制依赖
COPY --from=builder /install /usr/local

# 设置工作目录
WORKDIR /app

# 复制项目文件
COPY . .

# 复制并设置入口脚本
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# 创建缓存目录和数据目录
RUN mkdir -p /app/.cache /app/data && \
    chown -R appuser:appgroup /app

# 切换到非 root 用户
USER appuser

# 暴露端口（HTTP 模式）
EXPOSE 8000

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import httpx; r = httpx.get('http://localhost:8000/health', timeout=5); exit(0 if r.status_code == 200 else 1)" || exit 1

# 设置入口点
ENTRYPOINT ["docker-entrypoint.sh"]

# 默认以 HTTP 模式启动
CMD ["http"]
