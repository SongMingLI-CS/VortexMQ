# ============================================================
# VortexMQ 控制台镜像（国内镜像源加速版，部署专用）
#
# 与 console/Dockerfile 完全等价（同 Node/Nginx 基础镜像、同构建步骤、同产物），
# 唯一差别是 npm 走 npmmirror 镜像源加速（国内服务器可达性与带宽显著更好）。
#
# 由 deploy/docker-compose.deploy.yml 的 build.dockerfile 指向本文件
# （相对 console 构建上下文解析为 console/../deploy/dockerfiles/console.Dockerfile）。
# ============================================================

FROM node:20-alpine AS build

WORKDIR /app

# 先装依赖再拷源码：依赖不变时复用层缓存
COPY package.json package-lock.json ./
RUN npm ci --registry=https://registry.npmmirror.com --no-audit --no-fund

COPY tsconfig.json vite.config.ts index.html ./
COPY src ./src

# tsc 是类型闸门（tsconfig strict），失败即构建失败
RUN npm run build


FROM nginx:1.27-alpine

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
