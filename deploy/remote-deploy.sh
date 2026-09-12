#!/usr/bin/env bash
# ============================================================
# VortexMQ · 云服务器一键部署脚本（在【目标 Linux 服务器】上执行）
#
# 用法（脚本自动定位仓库根，可在任意目录调用）：
#   bash deploy/remote-deploy.sh
#   API_PORT=8000 CONSOLE_PORT=8081 bash deploy/remote-deploy.sh
#
# 特性：幂等可重复执行；不会删除数据卷（pg / redis / prom / grafana）。
#
# 与同机项目端口隔离（由 deploy/docker-compose.deploy.yml 叠加层实现）：
#   api        8000              公网（/docs、/metrics）
#   console    8081              公网（控制台 UI）
#   postgres   127.0.0.1:15432   仅本机
#   redis      127.0.0.1:16379   仅本机（6379 已被智汇于庄占用）
#   worker     127.0.0.1:18001   仅本机指标
#   prometheus 127.0.0.1:19090   仅本机
#   grafana    127.0.0.1:13000   仅本机
#
# 可覆盖环境变量：
#   COMPOSE_FILE      默认 docker-compose.yml
#   DEPLOY_OVERLAY    默认 deploy/docker-compose.deploy.yml
#   ENV_FILE          默认 <仓库根>/.env
#   API_PORT          默认 8000（公网入口；需在安全组放行）
#   CONSOLE_PORT      默认 8081（控制台；需在安全组放行）
#   VORTEXMQ_PG_PASSWORD / ADMIN_API_KEY / GRAFANA_ADMIN_PASSWORD
#                     未提供且 .env 仍为占位值时自动生成强随机值（十六进制）
#   NO_MIRROR=1       跳过 Docker 镜像加速器自动配置
#   VERIFY_WAIT       就绪等待上限秒，默认 300
# ============================================================
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
DEPLOY_OVERLAY="${DEPLOY_OVERLAY:-deploy/docker-compose.deploy.yml}"
ENV_FILE="${ENV_FILE:-${ROOT_DIR}/.env}"
API_PORT="${API_PORT:-8000}"
CONSOLE_PORT="${CONSOLE_PORT:-8081}"
NO_MIRROR="${NO_MIRROR:-0}"
VERIFY_WAIT="${VERIFY_WAIT:-300}"

# docker compose 必须以「数组」保存再展开（"${COMPOSE_CMD[@]}"）：按字符串保存并加引号
# 执行会把 "docker compose" 当成单个命令名 → command not found（rc=127）。
COMPOSE_CMD=(docker compose)
if [[ -n "${COMPOSE:-}" ]]; then
    read -r -a COMPOSE_CMD <<< "${COMPOSE}"
fi

# 主 compose + 部署叠加层（端口重映射与口令注入）
COMPOSE_ARGS=(-f "${COMPOSE_FILE}" -f "${DEPLOY_OVERLAY}")

SUDO=""
if [ "$(id -u)" -ne 0 ]; then
    if command -v sudo >/dev/null 2>&1; then SUDO="sudo"; fi
fi

if [ -t 1 ]; then
    GREEN=$'\033[32m'; RED=$'\033[31m'; YELLOW=$'\033[33m'; BOLD=$'\033[1m'; NC=$'\033[0m'
else
    GREEN=''; RED=''; YELLOW=''; BOLD=''; NC=''
fi
say()  { printf '%s\n' "$*"; }
ok()   { printf '%s[ OK ]%s %s\n' "${GREEN}" "${NC}" "$*"; }
warn() { printf '%s[WARN]%s %s\n' "${YELLOW}" "${NC}" "$*"; }
die()  { printf '%s[FAIL]%s %s\n' "${RED}" "${NC}" "$*" >&2; exit 1; }

run_compose() { "${COMPOSE_CMD[@]}" --env-file "${ENV_FILE}" "${COMPOSE_ARGS[@]}" "$@"; }

env_get() { sed -n -E "s|^$1=(.*)$|\1|p" "${ENV_FILE}" | tail -n 1 | tr -d '\r'; }

http_code() {
    curl -s -o /dev/null -w '%{http_code}' \
        --connect-timeout 3 --max-time 10 "$1" 2>/dev/null || true
}

echo "${BOLD}==> VortexMQ · 云服务器部署 @ $(hostname) [${ROOT_DIR}]${NC}"

# ---------- 0) 前置检查 ----------
echo
echo "${BOLD}[0/6] 前置检查${NC}"
[ -f "${COMPOSE_FILE}" ] || die "未找到主编排 ${COMPOSE_FILE}，请确认本脚本位于仓库 deploy/ 目录内。"
[ -f "${DEPLOY_OVERLAY}" ] || die "未找到部署叠加层 ${DEPLOY_OVERLAY}。"
ok "编排文件就绪：${COMPOSE_FILE} + ${DEPLOY_OVERLAY}"
command -v docker >/dev/null 2>&1 || die "未检测到 docker，请先安装 Docker Engine：curl -fsSL https://get.docker.com | sh"
docker info >/dev/null 2>&1 || die "Docker 守护进程不可用，请先启动：${SUDO} systemctl enable --now docker"
"${COMPOSE_CMD[@]}" version >/dev/null 2>&1 || die "未检测到 Docker Compose v2 插件（docker compose）。"
ok "$("${COMPOSE_CMD[@]}" version --short 2>/dev/null || docker --version)"
AVAIL_KB="$(df -Pk "${ROOT_DIR}" | awk 'NR==2 {print $4}')"
if [ "${AVAIL_KB:-0}" -lt 6291456 ]; then
    warn "磁盘可用空间仅 $(( ${AVAIL_KB:-0} / 1024 ))MB（镜像构建建议 ≥ 6GB）"
else
    ok "磁盘可用空间 $(( AVAIL_KB / 1024 / 1024 ))GB"
fi

# 行尾规范化：Windows 工作区（core.autocrlf=true）经 scp/tar 上传后，*.sh 会是 CRLF，
# 在 Linux 上执行会报 "set: pipefail: invalid option name"（已实测复现）。
# compose/Dockerfile 的 CRLF 无妨（Docker/Compose 均可解析），仅收敛脚本、Makefile 与 .env*。
mapfile -t CRLF_FILES < <(
    grep -rlIU $'\r' \
        "${ROOT_DIR}/.env" "${ROOT_DIR}/.env.example" "${ROOT_DIR}/Makefile" \
        "${ROOT_DIR}/deploy" "${ROOT_DIR}/scripts" 2>/dev/null || true
)
if [ "${#CRLF_FILES[@]}" -gt 0 ]; then
    warn "检测到 ${#CRLF_FILES[@]} 个 CRLF 文本文件（Windows 工作区特征），规范化为 LF..."
    if command -v dos2unix >/dev/null 2>&1; then
        dos2unix -q "${CRLF_FILES[@]}"
    else
        sed -i 's/\r$//' "${CRLF_FILES[@]}"
    fi
    ok "行尾规范化完成（Makefile / *.sh / .env*）"
else
    ok "行尾均为 LF，无需规范化"
fi

# ---------- 1) Docker 镜像加速器 ----------
echo
echo "${BOLD}[1/6] 镜像源连通性${NC}"
HUB_CODE="$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 5 https://registry-1.docker.io/v2/ 2>/dev/null || true)"
if [ -n "${HUB_CODE}" ] && [ "${HUB_CODE}" != "000" ]; then
    ok "Docker Hub 可达（HTTP ${HUB_CODE}），无需加速器"
elif [ "${NO_MIRROR}" = "1" ]; then
    warn "Docker Hub 不可达，但 NO_MIRROR=1 已指定跳过自动配置"
elif [ -z "${SUDO}" ] && [ "$(id -u)" -ne 0 ]; then
    warn "Docker Hub 不可达，但当前用户无 root/sudo，无法写入 /etc/docker/daemon.json"
else
    warn "Docker Hub 不可达，正在写入镜像加速器并重启 Docker..."
    DAC="/etc/docker/daemon.json"
    ${SUDO} mkdir -p /etc/docker
    if [ -f "${DAC}" ]; then
        ${SUDO} cp -n "${DAC}" "${DAC}.bak.$(date +%Y%m%d%H%M%S)" || true
    fi
    if command -v python3 >/dev/null 2>&1; then
        ${SUDO} python3 - "${DAC}" <<'PY'
import json, os, sys
path = sys.argv[1]
cfg = {}
if os.path.exists(path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            cfg = json.load(fh) or {}
    except Exception:
        cfg = {}
cfg["registry-mirrors"] = [
    "https://mirror.ccs.tencentyun.com",
    "https://docker.m.daocloud.io",
    "https://docker.nju.edu.cn",
    "https://docker.1ms.run",
]
with open(path, "w", encoding="utf-8") as fh:
    json.dump(cfg, fh, ensure_ascii=False, indent=2)
    fh.write("\n")
print("daemon.json updated:", path)
PY
    else
        warn "无 python3：请手工在 ${DAC} 添加 registry-mirrors 后重启 Docker"
    fi
    ${SUDO} systemctl restart docker 2>/dev/null || warn "systemctl restart docker 失败，请手动重启 Docker"
    for _ in $(seq 1 30); do docker info >/dev/null 2>&1 && break; sleep 1; done
    docker info >/dev/null 2>&1 && ok "Docker 已重启且可用" || die "Docker 重启后仍不可用，请检查 /etc/docker/daemon.json"
fi

# ---------- 2) .env 与密钥 ----------
echo
echo "${BOLD}[2/6] 环境变量与密钥${NC}"
if [ ! -f "${ENV_FILE}" ]; then
    [ -f .env.example ] || die "缺少 .env.example，无法生成 ${ENV_FILE}"
    cp .env.example "${ENV_FILE}"
    ok "已由 .env.example 生成 ${ENV_FILE}"
else
    ok "复用既有 ${ENV_FILE}"
fi

set_env_kv() {
    local key="$1" value="${2:-}"
    [ -n "${value}" ] || return 0
    if grep -qE "^${key}=" "${ENV_FILE}"; then
        # 以 | 作 sed 分隔符，避免值中的 / 破坏替换
        sed -i.bak -E "s|^${key}=.*$|${key}=${value}|" "${ENV_FILE}" && rm -f "${ENV_FILE}.bak"
    else
        printf '%s=%s\n' "${key}" "${value}" >> "${ENV_FILE}"
    fi
}

gen_secret() {
    # $1 = 需要的字符数（默认 48）。openssl 生成十六进制串（URL/口令上下文均安全）
    local chars="${1:-48}"
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -hex $(( chars / 2 )) 2>/dev/null && return 0
    fi
    LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom 2>/dev/null | head -c "${chars}" || true
}

# 占位值判定：空、change-me*、your-*、admin、postgres 等默认弱值均视为「未配置」
is_placeholder() {
    case "${1:-}" in
        ""|change-me*|your-*|*CHANGE_ME*|admin|postgres|password) return 0 ;;
        *) return 1 ;;
    esac
}

ensure_secret() {
    # $1=键名 $2=环境变量提供的值 $3=最小长度（既有值短于该长度时视为不合格并重新生成）
    local key="$1" provided="${2:-}" minlen="${3:-32}" cur gen
    cur="$(env_get "${key}")"
    if [ -n "${provided}" ]; then
        set_env_kv "${key}" "${provided}"
        ok "  ${key}：由环境变量提供"
    elif is_placeholder "${cur}" || [ "${#cur}" -lt "${minlen}" ]; then
        if [ -n "${cur}" ] && ! is_placeholder "${cur}"; then
            warn "  ${key}：既有值仅 ${#cur} 字符，低于要求的 ${minlen}，重新生成"
        fi
        gen="$(gen_secret $(( minlen * 2 )))"
        [ -n "${gen}" ] || die "生成 ${key} 随机值失败，请手工在 .env 中配置"
        set_env_kv "${key}" "${gen}"
        ok "  ${key}：已自动生成强随机值（$(( minlen * 2 )) 字符）"
    else
        ok "  ${key}：沿用既有配置"
    fi
}

# 叠加层对下列三项使用 ${VAR:?} 强制要求，缺失会直接导致编排失败
ensure_secret VORTEXMQ_PG_PASSWORD "${VORTEXMQ_PG_PASSWORD:-}" 32
ensure_secret ADMIN_API_KEY "${ADMIN_API_KEY:-}" 48
ensure_secret GRAFANA_ADMIN_PASSWORD "${GRAFANA_ADMIN_PASSWORD:-}" 16

set_env_kv API_PORT "${API_PORT}"
set_env_kv CONSOLE_PORT "${CONSOLE_PORT}"
chmod 600 "${ENV_FILE}" 2>/dev/null || true
ok "API 端口=${API_PORT} / 控制台端口=${CONSOLE_PORT} / 数据库=vortexmq"

# ---------- 3) 端口占用检查 ----------
echo
echo "${BOLD}[3/6] 端口占用检查${NC}"
port_busy() {
    if command -v ss >/dev/null 2>&1; then
        ss -lnt 2>/dev/null | awk 'NR>1 {print $4}' | grep -qE "[:.]$1$"
    else
        netstat -lnt 2>/dev/null | awk 'NR>2 {print $4}' | grep -qE "[:.]$1$"
    fi
}
for p in "${API_PORT}" "${CONSOLE_PORT}" 15432 16379 18001 19090 13000; do
    if port_busy "${p}"; then
        if docker ps --format '{{.Ports}}' 2>/dev/null | grep -qE "[:.]${p}->"; then
            warn "端口 ${p} 已被本机 Docker 容器占用（若为本项目容器，部署会自动收敛）"
        else
            warn "端口 ${p} 被非本项目进程占用；如冲突请显式指定 API_PORT/CONSOLE_PORT 后重跑"
        fi
    else
        ok "端口 ${p} 空闲"
    fi
done

# ---------- 4) 构建镜像并启动全部服务 ----------
echo
echo "${BOLD}[4/6] 构建镜像并启动（postgres/redis/api/worker/console/prometheus/grafana）${NC}"
say "    首次构建需拉取基础镜像并装依赖（pip/npm 已切国内镜像源），请耐心等待..."
if ! run_compose up -d --build; then
    warn "构建或启动失败，最近日志如下："
    run_compose logs --tail 60 || true
    die "部署失败：docker compose up -d --build 未成功"
fi
run_compose ps

# ---------- 5) 就绪等待 + 端到端验证 ----------
echo
echo "${BOLD}[5/6] 服务就绪等待（上限 ${VERIFY_WAIT}s）${NC}"
waited=0
while :; do
    code="$(http_code "http://127.0.0.1:${API_PORT}/health/ready")"
    if [ "${code}" = "200" ]; then
        break
    fi
    if [ "${waited}" -ge "${VERIFY_WAIT}" ]; then
        echo
        warn "API ${VERIFY_WAIT}s 内未就绪，最近日志如下："
        run_compose logs --tail 80 api worker || true
        die "部署失败：API 未就绪（/health/ready 非 200）"
    fi
    printf '.'
    sleep 5
    waited=$((waited + 5))
done
echo
ok "GET /health/ready -> HTTP 200（PostgreSQL + Redis 真实探活通过，约 ${waited}s）"

VERIFY_FAIL=0
live="$(http_code "http://127.0.0.1:${API_PORT}/health")"
if [ "${live}" = "200" ]; then
    ok "GET /health -> HTTP 200（存活探针）"
else
    warn "GET /health -> HTTP ${live:-timeout/refused}（期望 200）"
    VERIFY_FAIL=1
fi

console_root="$(http_code "http://127.0.0.1:${CONSOLE_PORT}/")"
if [ "${console_root}" = "200" ]; then
    ok "GET :${CONSOLE_PORT}/ -> HTTP 200（控制台静态页）"
else
    warn "GET :${CONSOLE_PORT}/ -> HTTP ${console_root:-timeout/refused}（期望 200）"
    VERIFY_FAIL=1
fi

console_health="$(http_code "http://127.0.0.1:${CONSOLE_PORT}/health")"
if [ "${console_health}" = "200" ]; then
    ok "GET :${CONSOLE_PORT}/health -> HTTP 200（控制台同源反代到 API 成功）"
else
    warn "GET :${CONSOLE_PORT}/health -> HTTP ${console_health:-timeout/refused}（期望 200）"
    VERIFY_FAIL=1
fi

worker_state="$(run_compose ps --format '{{.Service}} {{.State}}' 2>/dev/null | grep '^worker' || true)"
say "    worker 容器状态：${worker_state:-未知}"

if [ "${VERIFY_FAIL}" -ne 0 ]; then
    run_compose logs --tail 60 api worker console || true
    die "部署后接口验证未通过"
fi

# ---------- 6) 汇总 ----------
HOST_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
[ -n "${HOST_IP}" ] || HOST_IP="<服务器地址>"
echo
echo "----------------------------------------------"
printf '%s%s[部署完成] VortexMQ 已上线%s\n' "${GREEN}" "${BOLD}" "${NC}"
echo "  管理控制台 : http://${HOST_IP}:${CONSOLE_PORT}/"
echo "  API 文档   : http://${HOST_IP}:${API_PORT}/docs"
echo "  就绪探针   : http://${HOST_IP}:${API_PORT}/health/ready"
echo "  指标       : http://${HOST_IP}:${API_PORT}/metrics"
echo
say "仅本机端口：postgres 15432 / redis 16379 / worker 18001 / prometheus 19090 / grafana 13000"
say "  SSH 隧道示例：ssh -L 13000:127.0.0.1:13000 -L 19090:127.0.0.1:19090 ubuntu@${HOST_IP}"
echo
say "运维命令（在 ${ROOT_DIR} 执行，必须带两个 -f）："
say "  查看状态  ${COMPOSE_CMD[*]} --env-file ${ENV_FILE} ${COMPOSE_ARGS[*]} ps"
say "  跟踪日志  ${COMPOSE_CMD[*]} --env-file ${ENV_FILE} ${COMPOSE_ARGS[*]} logs -f worker"
say "  停止服务  ${COMPOSE_CMD[*]} --env-file ${ENV_FILE} ${COMPOSE_ARGS[*]} down"
say "  签发租户 API Key（明文仅打印一次）："
say "    ${COMPOSE_CMD[*]} --env-file ${ENV_FILE} ${COMPOSE_ARGS[*]} exec -T api python -m app.cli create-tenant default"
warn "云主机需在安全组放行 TCP ${API_PORT}（API）与 ${CONSOLE_PORT}（控制台）。"
warn "控制台页面需填写 X-Admin-Key，值见服务器 ${ENV_FILE} 中的 ADMIN_API_KEY。"
