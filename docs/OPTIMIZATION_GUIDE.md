# Tiki Crawler Optimization Guide

## Overview
This guide provides optimization settings for the Tiki Data Pipeline based on your system resources.

## System Requirements & Recommendations

### Your System (Intel i7-8650U, 16GB RAM)
| Setting | Value | Reason |
|---------|-------|--------|
| `crawl_pool` slots | 6 | Safe for memory with Selenium drivers |
| `MAX_CONCURRENT_DRIVERS` | 6 | Each driver uses ~300-500MB RAM |
| `TIKI_CRAWL_BATCH_SIZE` | 5 | Products per crawl task |
| `TIKI_SAVE_BATCH_SIZE` | 100 | Products per DB batch |
| `max_active_tasks` | 4 | DAG level concurrency |

### For Weaker Systems (8GB RAM)
- Set `crawl_pool` = 3
- Set `MAX_CONCURRENT_DRIVERS` = 3
- Increase delay between requests

## Airflow Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `TIKI_MIN_CATEGORY_LEVEL` | 2 | Minimum category depth to crawl |
| `TIKI_MAX_CATEGORY_LEVEL` | 4 | Maximum category depth |
| `TIKI_MAX_CATEGORIES` | 0 | Limit categories (0 = no limit) |
| `TIKI_DAG_SCHEDULE_MODE` | manual | `manual` or `scheduled` |
| `TIKI_CRAWL_BATCH_SIZE` | 5 | Products per batch |
| `TIKI_SAVE_BATCH_SIZE` | 100 | DB batch size |

## Rate Limiting Best Practices

1. **Delay between requests**: Built into Selenium crawl (2-3 seconds)
2. **Circuit breaker**: Auto-pauses after 5 consecutive failures
3. **Graceful degradation**: Reduces load when errors increase

## Monitoring

### Check Resource Usage
```bash
# Docker container stats
docker stats

# PostgreSQL connections
docker exec -it postgres psql -U user -d crawl_data -c "SELECT count(*) FROM pg_stat_activity;"
```

### Verify Pool Usage
```bash
docker exec -it airflow-scheduler airflow pools list
```

## Troubleshooting

### Out of Memory
1. Reduce `crawl_pool` slots
2. Lower `PARALLELISM` in docker-compose
3. Restart Docker services

### Slow Crawling
1. Increase pool slots (if RAM allows)
2. Check network connectivity
3. Review Airflow task logs
