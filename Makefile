# VortexMQ 常用命令。需要 GNU Make（Git Bash / Chocolatey make / Linux / macOS）。
# 用法：make up / make down / make logs-worker / make stress

PYTHON ?= python
COMPOSE ?= docker compose
STRESS_ARGS ?=

.PHONY: up down logs-worker stress

# 构建并后台启动全部容器：Postgres、Redis、API、Worker、Prometheus、Grafana
up:
	$(COMPOSE) up -d --build

# 停止并删除容器、网络（保留数据卷，压测数据还在）
down:
	$(COMPOSE) down

# 跟踪 Worker 日志，观察消费、退避重试、DLQ
logs-worker:
	$(COMPOSE) logs -f --tail=200 worker

# 对宿主机映射的 API 发起混合压测。须先签发密钥：
#   python -m app.cli create-tenant default
#   make stress STRESS_ARGS='--api-key <明文 Key>'
stress:
	$(PYTHON) scripts/stress_test.py $(STRESS_ARGS)
