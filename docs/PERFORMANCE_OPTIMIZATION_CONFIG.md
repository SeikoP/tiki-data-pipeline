# Performance Optimization Configuration Guide

This guide explains the performance optimization features and how to configure them for optimal performance.

## Overview

The performance optimization system provides three main optimizations:

1. **Async I/O** - Asynchronous HTTP requests for improved concurrency
2. **Vectorized Transformation** - NumPy/Pandas operations for faster data processing
3. **Optimized Batch Loading** - PostgreSQL COPY and batch upsert for faster database operations

Each optimization can be enabled/disabled independently via feature flags in the `.env` file.

## Quick Start

### 1. Generate Recommended Configuration

Run the configuration generator to analyze your system and get recommendations:

```bash
python scripts/generate_optimized_config.py --format markdown --output docs/RECOMMENDED_CONFIG.md
```

This will analyze your system capabilities (CPU, memory, disk, network) and generate optimal configuration values.

### 2. Apply Configuration

Copy the recommended values to your `.env` file:

```bash
# Enable optimizations
USE_ASYNC_CRAWLER=true
USE_VECTORIZED_TRANSFORM=true
USE_OPTIMIZED_LOADER=true

# Apply recommended values (example - use values from generator)
ASYNC_CONNECTOR_LIMIT=300
ASYNC_BATCH_SIZE=1000
OPTIMIZED_BATCH_SIZE=2000
```

### 3. Restart Application

Restart your application to apply the new configuration:

```bash
docker-compose restart
```

## Configuration Reference

### Async I/O Optimization

Controls asynchronous HTTP requests for improved concurrency in the crawl stage.

| Variable | Default | Description |
|----------|---------|-------------|
| `USE_ASYNC_CRAWLER` | `false` | Enable async crawler |
| `ASYNC_CONNECTOR_LIMIT` | `300` | Maximum concurrent connections |
| `ASYNC_BATCH_SIZE` | `50` | Products per concurrent batch |
| `ASYNC_TIMEOUT_TOTAL` | `15` | Total request timeout (seconds) |
| `ASYNC_TIMEOUT_CONNECT` | `10` | Connection timeout (seconds) |

**When to enable:**
- High network bandwidth (>20 MB/s)
- Many concurrent requests needed
- I/O-bound crawling operations

**Performance impact:**
- 2-5x faster crawling for large datasets
- Better resource utilization
- Reduced idle time waiting for network

### Vectorized Transformation

Controls vectorized operations using NumPy/Pandas for faster data processing.

| Variable | Default | Description |
|----------|---------|-------------|
| `USE_VECTORIZED_TRANSFORM` | `false` | Enable vectorized operations |
| `THREAD_POOL_SIZE` | `auto` | Thread pool size for I/O operations |
| `PROCESS_POOL_SIZE` | `auto` | Process pool size for CPU operations |

**When to enable:**
- Sufficient memory (>2 GB available)
- Large batch sizes (>100 products)
- CPU-bound transformation operations

**Performance impact:**
- 3-10x faster transformations for large batches
- Better CPU utilization
- Reduced loop overhead

### Optimized Batch Loading

Controls optimized database operations using PostgreSQL COPY and batch upsert.

| Variable | Default | Description |
|----------|---------|-------------|
| `USE_OPTIMIZED_LOADER` | `false` | Enable optimized loader |
| `OPTIMIZED_BATCH_SIZE` | `1000` | Records per batch |
| `USE_COPY_COMMAND` | `true` | Use COPY for bulk inserts |
| `DB_CONNECTION_POOL_MIN` | `2` | Minimum connections |
| `DB_CONNECTION_POOL_MAX` | `auto` | Maximum connections |

**When to enable:**
- Local PostgreSQL database
- Large batch inserts/updates
- Fast disk I/O (>100 MB/s)

**Performance impact:**
- 5-20x faster database loading
- Reduced transaction overhead
- Better connection pooling

### Cache Configuration

Controls caching behavior for improved performance.

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_CACHE_TTL` | `3600` | Cache TTL in seconds |
| `MEMORY_CACHE_SIZE_MB` | `auto` | Memory cache size (MB) |
| `CACHE_EVICTION_THRESHOLD` | `0.9` | Eviction threshold (0.0-1.0) |

## Gradual Rollout Strategy

For production environments, enable optimizations gradually to minimize risk:

### Phase 1: Async Crawler (Week 1)

```bash
USE_ASYNC_CRAWLER=true
USE_VECTORIZED_TRANSFORM=false
USE_OPTIMIZED_LOADER=false
```

**Monitor:**
- Crawl throughput (products/second)
- Error rates
- Network utilization
- Data completeness

**Success criteria:**
- Error rate < 1%
- Data completeness ≥ 70%
- No increase in failed requests

### Phase 2: Vectorized Transformation (Week 2)

```bash
USE_ASYNC_CRAWLER=true
USE_VECTORIZED_TRANSFORM=true
USE_OPTIMIZED_LOADER=false
```

**Monitor:**
- Transformation throughput
- CPU utilization
- Memory usage
- Data quality metrics

**Success criteria:**
- Transformation results match original
- No data validation errors
- Memory usage stable

### Phase 3: Optimized Loader (Week 3)

```bash
USE_ASYNC_CRAWLER=true
USE_VECTORIZED_TRANSFORM=true
USE_OPTIMIZED_LOADER=true
```

**Monitor:**
- Database load throughput
- Connection pool utilization
- Database CPU/memory
- Data integrity

**Success criteria:**
- All products loaded successfully
- No database errors
- Data integrity maintained

### Phase 4: Fine-Tuning (Week 4+)

Adjust configuration values based on observed performance:

- Increase batch sizes if memory allows
- Adjust pool sizes based on CPU utilization
- Tune timeouts based on network conditions
- Optimize cache sizes based on hit rates

## Rollback Instructions

If issues occur, rollback to the original configuration:

### 1. Disable All Optimizations

```bash
USE_ASYNC_CRAWLER=false
USE_VECTORIZED_TRANSFORM=false
USE_OPTIMIZED_LOADER=false
```

### 2. Restart Application

```bash
docker-compose restart
```

### 3. Verify Stability

Monitor for 24 hours to ensure stable operation:
- Check error logs
- Verify data quality
- Monitor resource usage

### 4. Investigate Issues

Review logs to identify the cause:
- Check application logs
- Review database logs
- Analyze error patterns

## Performance Benchmarking

### Before Optimization

Run baseline benchmarks before enabling optimizations:

```bash
python scripts/run_baseline_profiling.py
```

This captures:
- Crawl throughput
- Transform throughput
- Load throughput
- Resource utilization

### After Optimization

Run the same benchmarks after enabling optimizations:

```bash
python scripts/run_comprehensive_benchmarks.py
```

Compare results to measure improvement:
- Throughput increase (products/second)
- Latency reduction (seconds/product)
- Resource efficiency (CPU, memory)

## Troubleshooting

### High Memory Usage

**Symptoms:**
- Out of memory errors
- Slow performance
- System swapping

**Solutions:**
1. Reduce batch sizes:
   ```bash
   ASYNC_BATCH_SIZE=25
   OPTIMIZED_BATCH_SIZE=500
   ```

2. Reduce cache sizes:
   ```bash
   MEMORY_CACHE_SIZE_MB=50
   ```

3. Disable vectorization if memory constrained:
   ```bash
   USE_VECTORIZED_TRANSFORM=false
   ```

### High CPU Usage

**Symptoms:**
- CPU at 100%
- Slow response times
- System unresponsive

**Solutions:**
1. Reduce pool sizes:
   ```bash
   THREAD_POOL_SIZE=4
   PROCESS_POOL_SIZE=2
   ```

2. Reduce concurrent connections:
   ```bash
   ASYNC_CONNECTOR_LIMIT=100
   ```

### Database Connection Errors

**Symptoms:**
- "Too many connections" errors
- Connection timeouts
- Slow database operations

**Solutions:**
1. Reduce connection pool size:
   ```bash
   DB_CONNECTION_POOL_MAX=10
   ```

2. Reduce batch size:
   ```bash
   OPTIMIZED_BATCH_SIZE=500
   ```

3. Increase PostgreSQL max_connections:
   ```sql
   ALTER SYSTEM SET max_connections = 100;
   ```

### Network Errors

**Symptoms:**
- Timeout errors
- Connection refused
- Rate limiting (429 errors)

**Solutions:**
1. Reduce concurrent connections:
   ```bash
   ASYNC_CONNECTOR_LIMIT=100
   ```

2. Increase timeouts:
   ```bash
   ASYNC_TIMEOUT_TOTAL=30
   ASYNC_TIMEOUT_CONNECT=20
   ```

3. Enable rate limiting:
   ```bash
   # Rate limiting is enabled by default
   ```

## Best Practices

### 1. Start Conservative

Begin with conservative values and increase gradually:
- Small batch sizes (100-500)
- Low concurrency (50-100 connections)
- Standard timeouts (15-30 seconds)

### 2. Monitor Continuously

Set up monitoring for:
- Application metrics (throughput, latency, errors)
- System metrics (CPU, memory, disk, network)
- Database metrics (connections, queries, locks)

### 3. Test in Staging

Always test configuration changes in staging before production:
- Run full pipeline with production-like data
- Monitor for 24-48 hours
- Verify data quality and integrity

### 4. Document Changes

Keep a log of configuration changes:
- Date and time of change
- Values changed
- Reason for change
- Observed impact

### 5. Have a Rollback Plan

Always have a rollback plan ready:
- Keep previous configuration backed up
- Document rollback steps
- Test rollback procedure in staging

## Advanced Configuration

### Custom Configuration

For advanced users, you can customize configuration programmatically:

```python
from common.configuration_analyzer import ConfigurationAnalyzer

# Analyze system
analyzer = ConfigurationAnalyzer()
system = analyzer.analyze_system()

# Generate recommendations
config = analyzer.recommend_config(system)

# Customize values
config.http_connector_limit = 500  # Increase for high bandwidth
config.db_batch_size = 5000  # Increase for fast disk

# Validate
is_valid, warnings = analyzer.validate_config(config, system)
```

### Environment-Specific Configuration

Use different configurations for different environments:

**Development:**
```bash
USE_ASYNC_CRAWLER=false  # Easier debugging
USE_VECTORIZED_TRANSFORM=false
USE_OPTIMIZED_LOADER=false
```

**Staging:**
```bash
USE_ASYNC_CRAWLER=true  # Test optimizations
USE_VECTORIZED_TRANSFORM=true
USE_OPTIMIZED_LOADER=true
```

**Production:**
```bash
USE_ASYNC_CRAWLER=true  # Full optimizations
USE_VECTORIZED_TRANSFORM=true
USE_OPTIMIZED_LOADER=true
# Use recommended values from generator
```

## Support

For issues or questions:

1. Check the troubleshooting section above
2. Review application logs
3. Run the configuration generator for recommendations
4. Consult the design document: `.kiro/specs/performance-optimization/design.md`

## References

- Design Document: `.kiro/specs/performance-optimization/design.md`
- Requirements: `.kiro/specs/performance-optimization/requirements.md`
- Tasks: `.kiro/specs/performance-optimization/tasks.md`
- Examples: `examples/async_crawler_integration_example.py`, etc.
