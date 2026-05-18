# Configuration

Configuration is split across Docker Compose, `.env`, Python constants and Airflow Variables.

## Local Setup

```powershell
Copy-Item .env.example .env
docker compose up -d --build
```

Airflow UI: `http://localhost:8080/ui/`

## Docker Services

| Service | Purpose | Notes |
| --- | --- | --- |
| `postgres` | Airflow metadata and crawl data | PostgreSQL 16 Alpine, local port `127.0.0.1:5432` |
| `redis` | Redis cache/runtime support | Redis 7.2 Alpine, local port `127.0.0.1:6379` |
| `airflow-webserver` | Airflow API/UI | Runs migrations before API server |
| `airflow-scheduler` | DAG scheduling | Runs migrations before scheduler |
| `airflow-init` | Bootstrap | Creates users, pools and initial setup |

## Important Environment Variables

| Variable | Purpose |
| --- | --- |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | PostgreSQL connection setup |
| `REDIS_PASSWORD` | Redis auth |
| `AIRFLOW__CORE__FERNET_KEY` | Airflow encryption key |
| `_AIRFLOW__API_AUTH__JWT_SECRET`, `_AIRFLOW__CORE__SECRET_KEY` | Airflow API/session security |
| `_AIRFLOW_WWW_USER_USERNAME` | Airflow admin username override |
| `DISCORD_ENABLED` | Enables final Discord notification path |

## Runtime Paths

| Path | Git policy |
| --- | --- |
| `.env` | Local secret file, ignored |
| `data/` | Generated crawl data, ignored |
| `backups/` | Generated DB backups, ignored except placeholder |
| `airflow/logs/` | Runtime logs, ignored |
| `.pytest_cache`, `.ruff_cache`, `.mypy_cache`, `__pycache__` | Tool/runtime caches, ignored |

## Maintenance Commands

```powershell
.\scripts\cleanup.ps1 -Cache -Logs -DryRun
.\scripts\cleanup.ps1 -Cache -Logs
.\scripts\docker-cleanup.ps1
```
