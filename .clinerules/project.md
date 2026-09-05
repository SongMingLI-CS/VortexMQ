# VortexMQ project rules

## Mission and working style

- Prioritize a runnable, reliable MVP over speculative features or broad refactors.
- Inspect the relevant code and existing tests before making changes.
- For work spanning multiple components, propose a short plan before editing.
- Keep changes small and reviewable; preserve unrelated user changes.
- State assumptions explicitly when requirements are ambiguous.

## Architecture boundaries

- PostgreSQL is the source of truth for task state; Redis is only the delivery, delay, and coordination layer.
- Preserve strict tenant isolation. Every externally reachable task query or mutation must be scoped by `tenant_id` derived from `X-API-Key`.
- Never trust a client-supplied tenant ID or a Redis message tenant ID without checking it against PostgreSQL.
- Preserve the write order: persist the task in PostgreSQL before publishing it to Redis.
- Redis publication failures must remain recoverable through the Outbox mechanism.
- Keep delayed delivery in tenant-scoped ZSets and immediate delivery in tenant-scoped Streams.
- Preserve Redis Cluster hash-tag compatibility; Lua scripts must not access keys from different slots.
- Keep Worker task claiming idempotent and concurrency-safe. Do not weaken CAS updates, row locks, leases, or ACK ordering.
- Use SQLAlchemy 2.x typed and async APIs throughout; do not introduce legacy `Query` or synchronous database access into request/worker paths.
- Use Alembic for new schema evolution once migrations are introduced; do not add more startup-time ad-hoc migrations.

## API and domain behavior

- Maintain the current status lifecycle and make every transition explicit and tested.
- Keep API errors stable and tenant-safe: cross-tenant resources should appear not found.
- Validate payload size and reserved `_vortex_sys` namespaces at the API boundary.
- Do not expose secrets, API-key hashes, stack traces, or internal tenant identifiers unnecessarily.
- New task handlers should use an explicit registry/interface and must not add task-type conditionals throughout the Worker.

## Testing and verification

- Every bug fix must include a regression test; every new behavior must include appropriate unit or integration coverage.
- Critical flows require E2E coverage across API, PostgreSQL, Redis, Worker processing, and result retrieval.
- Reuse the isolated fixtures in `tests/conftest.py`; never run destructive cleanup against a non-test database or broad Redis keyspace.
- Keep tests deterministic: mock business delays and external services, but exercise real PostgreSQL and Redis semantics where they matter.
- Run focused tests while iterating, then run `python -m pytest -q` before declaring completion.
- Run `python -m compileall -q app tests` after structural or import changes.
- Do not claim success unless commands were actually run; report failures and remaining risks clearly.

## Security and repository hygiene

- Never read, print, modify, or commit `.env`, credentials, tokens, private keys, or production data.
- Keep secrets in environment variables and document only safe examples in `.env.example`.
- Do not use destructive Git or filesystem commands unless the user explicitly requests them and the exact target is verified.
- Do not edit generated caches, virtual environments, database volumes, or unrelated operational data.
- Update relevant README sections when commands, configuration, public APIs, or deployment behavior change.
