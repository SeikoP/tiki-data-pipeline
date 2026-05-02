# Guides

## First Local Run

```powershell
Copy-Item .env.example .env
docker compose up -d --build
```

Then open `http://localhost:8080/ui/` and trigger DAG `tiki_crawl_fast`.

## Category Initialization

When category files are missing or stale:

```powershell
python src/pipelines/crawl/extract_category_link_selenium.py
python src/pipelines/crawl/crawl_categories_optimized.py
python src/tools/build_category_hierarchy.py
```

Expected generated files are under `data/raw/`.

## Multiple Root Categories

The crawler supports category hierarchies with multiple root/parent paths. Keep category data consistent by rebuilding the hierarchy map after changing category crawl inputs:

```powershell
python src/tools/build_category_tree.py
python src/tools/build_category_hierarchy.py
```

Validate with:

```powershell
pytest tests/test_hierarchy_map.py tests/test_leaf_filtering.py -q
```

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Airflow UI not available | `docker compose ps` and `docker compose logs airflow-webserver` |
| DAG does not parse | `docker compose logs airflow-scheduler` and import errors in DAG logs |
| Redis auth/cache failures | `.env` `REDIS_PASSWORD` and Redis service health |
| PostgreSQL connection failures | `.env` DB variables and `postgres` healthcheck |
| Repeated stale product data | Temporarily set `TIKI_FORCE_REFRESH=true` |
| Missing seller/brand | Check retry settings and relevant product detail tests |
