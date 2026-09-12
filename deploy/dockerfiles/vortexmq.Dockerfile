# ============================================================
# VortexMQ API / Worker 镜像（国内镜像源加速版，部署专用）
#
# 与仓库根 Dockerfile 完全等价（同基础镜像、同依赖、同启动命令），唯一差别是把
# pip 源切到腾讯云镜像并带「失败自动回退官方源」。原因是该实例实测：
#   pypi.org 49 KB/s  vs  mirrors.cloud.tencent.com 856 KB/s
#
# 由 deploy/docker-compose.deploy.yml 的 build.dockerfile 指向本文件，
# 仓库根 Dockerfile 保持不动（本地开发路径不受影响）。
# ============================================================

FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

COPY requirements.txt ./
RUN pip install --no-cache-dir \
        -i https://mirrors.cloud.tencent.com/pypi/simple \
        --trusted-host mirrors.cloud.tencent.com \
        -r requirements.txt \
    || pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000 8001

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
