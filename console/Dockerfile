# 前端控制台镜像：Node 构建 → Nginx 静态托管 + 同源反向代理 /api。
# 生产环境浏览器只访问控制台端口（默认 8080），不直连 API，因此不需要 CORS 配置。
FROM node:20-alpine AS build

WORKDIR /app

# 先装依赖再拷源码：依赖不变时复用层缓存
COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig.json vite.config.ts index.html ./
COPY src ./src

# tsc 是类型闸门（tsconfig strict），失败即构建失败
RUN npm run build


FROM nginx:1.27-alpine

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
