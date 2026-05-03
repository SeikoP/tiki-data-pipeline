from __future__ import annotations

from common.settings import get_pipeline_settings

# Import all bootstrap globals (paths, config, dynamic imports, singletons).
# This preserves legacy behavior without renaming any globals referenced by task callables.
from tiki_crawl_products_v2.bootstrap import CATEGORIES_FILE, Any, datetime, get_variable, json, os
from tiki_crawl_products_v2.tasks.common import (
    _fix_sys_path_for_pipelines_import,  # noqa: F401
    get_logger,  # noqa: F401
)


def _get_xcom_dict(context, task_ids: list[str]) -> dict[str, Any] | None:
    ti = context.get("ti")
    if not ti:
        return None
    for task_id in task_ids:
        try:
            value = ti.xcom_pull(task_ids=task_id)
        except Exception:
            continue
        if isinstance(value, dict):
            return value
    return None


def _db_load_is_complete(context, logger) -> tuple[bool, str]:
    load_result = _get_xcom_dict(context, ["transform_and_load.load_products", "load_products"])
    if not load_result:
        return False, "missing load_products XCom"

    load_stats = load_result.get("load_stats") or {}
    db_loaded = int(load_stats.get("db_loaded") or 0)
    failed_count = int(load_stats.get("failed_count") or 0)

    transform_result = _get_xcom_dict(
        context, ["transform_and_load.transform_products", "transform_products"]
    )
    transformed_count = 0
    if transform_result:
        transformed_count = int(transform_result.get("transformed_count") or 0)

    if failed_count > 0:
        return False, f"load_products reported failed_count={failed_count}"
    if db_loaded <= 0:
        return False, "load_products did not load any rows into DB"
    if transformed_count and db_loaded < transformed_count:
        return False, f"db_loaded={db_loaded} < transformed_count={transformed_count}"

    logger.info(
        "✅ DB load guard passed: db_loaded=%s transformed=%s", db_loaded, transformed_count
    )
    return True, "ok"


def cleanup_incomplete_products_wrapper(**context):
    """Task wrapper to cleanup products with missing required fields (seller and/or brand). Run this
    BEFORE crawling to allow re-crawling of incomplete data.

    This is a PREVENTIVE cleanup - better to clean before crawl than after load.
    """
    logger = get_logger(context)

    try:
        from common.db_schema import ensure_crawl_schema
        from pipelines.crawl.storage.postgres_storage import PostgresStorage

        logger.info("🧹 Starting cleanup of incomplete products (missing seller/brand)...")
        storage = PostgresStorage()
        with storage.get_connection() as conn:
            with conn.cursor() as cur:
                ensure_crawl_schema(cur)
            conn.commit()

        # Clean up products missing seller OR brand (or both)
        # both are required for quality data
        # require_rating=False (default) to prevent deleting products with null ratings
        # which leads to data loss for items with no reviews.
        stats = storage.cleanup_incomplete_products(
            require_seller=True, require_brand=True, require_rating=False
        )

        deleted_count = stats["deleted_count"]
        deleted_no_seller = stats.get("deleted_no_seller", 0)
        deleted_no_brand = stats.get("deleted_no_brand", 0)
        deleted_both = stats.get("deleted_both", 0)

        logger.info("=" * 70)
        logger.info(f"✅ Cleanup complete: {deleted_count} products deleted")
        if deleted_count > 0:
            logger.info(f"   - Missing seller only: {deleted_no_seller}")
            logger.info(f"   - Missing brand only: {deleted_no_brand}")
            logger.info(f"   - Missing both: {deleted_both}")
        logger.info("💡 These products will be re-crawled in the next run")
        logger.info("=" * 70)

        return {
            "status": "success",
            "deleted_count": deleted_count,
            "deleted_no_seller": deleted_no_seller,
            "deleted_no_brand": deleted_no_brand,
            "deleted_both": deleted_both,
        }

    except Exception as e:
        logger.error(f"❌ Error during cleanup: {e}", exc_info=True)
        return {
            "status": "error",
            "message": str(e),
            "deleted_count": 0,
            "deleted_no_seller": 0,
            "deleted_no_brand": 0,
            "deleted_both": 0,
        }


def cleanup_categories_wrapper(**context):
    """
    Task wrapper to cleanup both orphan and redundant categories.

    Xóa:
    1. Orphan categories: có product_count = 0 hoặc không có products
    2. Redundant categories: non-leaf categories
    """
    logger = get_logger(context)

    try:
        from pipelines.crawl.storage.postgres_storage import PostgresStorage

        logger.info("=" * 70)
        logger.info("🧹 CLEANUP CATEGORIES (Orphan + Redundant)")
        logger.info("=" * 70)

        allowed, reason = _db_load_is_complete(context, logger)
        if not allowed:
            logger.warning("⏭️  Skipping category cleanup: %s", reason)
            return {
                "status": "skipped",
                "reason": reason,
                "total_deleted": 0,
                "orphan_deleted": 0,
                "redundant_deleted": 0,
            }

        storage = PostgresStorage()
        with storage.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM products")
                product_count = cur.fetchone()[0]
        if product_count <= 0:
            logger.warning("⏭️  Skipping category cleanup: products table is empty")
            return {
                "status": "skipped",
                "reason": "products table is empty",
                "total_deleted": 0,
                "orphan_deleted": 0,
                "redundant_deleted": 0,
            }

        # PART 1: Cleanup Orphan Categories
        logger.info("📦 Part 1: Cleaning up orphan categories...")

        with storage.get_connection() as conn:
            with conn.cursor() as cur:
                # Xóa categories có product_count = 0 hoặc NULL
                cur.execute("""
                    DELETE FROM categories
                    WHERE (product_count = 0 OR product_count IS NULL)
                    AND is_leaf = true
                """)
                deleted_zero_count = cur.rowcount

                # Xóa leaf categories không có products trong bảng products
                cur.execute("""
                    DELETE FROM categories
                    WHERE is_leaf = true
                    AND NOT EXISTS (
                        SELECT 1 FROM products p
                        WHERE p.category_id = categories.category_id
                    )
                """)
                deleted_no_products = cur.rowcount

                conn.commit()

        # Gọi cleanup_orphan_categories từ storage
        additional_deleted = storage.cleanup_orphan_categories()
        total_orphan_deleted = deleted_zero_count + deleted_no_products + additional_deleted

        logger.info("📊 Orphan cleanup results:")
        logger.info(f"   - Categories với product_count = 0: {deleted_zero_count}")
        logger.info(
            f"   - Categories không có products: {deleted_no_products + additional_deleted}"
        )
        logger.info(f"   - Tổng orphan deleted: {total_orphan_deleted}")

        # PART 2: Cleanup Redundant Categories
        logger.info("📦 Part 2: Cleaning up redundant (non-leaf) categories...")
        removed_redundant = storage.cleanup_redundant_categories()
        logger.info(f"   - Non-leaf categories removed: {removed_redundant}")

        logger.info("=" * 70)
        logger.info(
            f"✅ Total cleanup: {total_orphan_deleted + removed_redundant} categories deleted"
        )
        logger.info(f"   - Orphan: {total_orphan_deleted}")
        logger.info(f"   - Redundant: {removed_redundant}")
        logger.info("=" * 70)

        return {
            "status": "success",
            "total_deleted": total_orphan_deleted + removed_redundant,
            "orphan_deleted": total_orphan_deleted,
            "redundant_deleted": removed_redundant,
        }

    except Exception as e:
        logger.error(f"❌ Error during cleanup: {e}", exc_info=True)
        return {"status": "error", "message": str(e), "deleted_count": 0}


def reconcile_categories_wrapper(**context):
    """Task wrapper to reconcile categories from JSON.

    Updates names, removes orphans, and updates product counts.
    """
    logger = get_logger(context)
    try:
        allowed, reason = _db_load_is_complete(context, logger)
        if not allowed:
            logger.warning("⏭️  Skipping category reconciliation: %s", reason)
            return {"status": "skipped", "message": reason}

        from pipelines.crawl.storage.postgres_storage import PostgresStorage

        storage = PostgresStorage()
        json_path = str(CATEGORIES_FILE)

        if not os.path.exists(json_path):
            logger.warning(f"⚠️  File {json_path} not found, skipping name updates")
            return {"status": "skipped", "message": "File not found"}

        with open(json_path, encoding="utf-8") as f:
            categories_data = json.load(f)

        id_to_name = {
            cat.get("category_id"): cat.get("name")
            for cat in categories_data
            if cat.get("category_id") and cat.get("name")
        }

        updated_names = 0
        with storage.get_connection() as conn:
            with conn.cursor() as cur:
                # 1. Update names
                for cat_id, name in id_to_name.items():
                    cur.execute(
                        "UPDATE categories SET name = %s WHERE category_id = %s AND name = category_id",
                        (name, cat_id),
                    )
                    updated_names += cur.rowcount
                conn.commit()

        # 2. Update product counts (already available in storage)
        updated_counts = storage.update_category_product_counts()

        logger.info(f"✅ Reconciled: updated {updated_names} names, {updated_counts} counts")
        return {
            "status": "success",
            "updated_names": updated_names,
            "updated_counts": updated_counts,
        }

    except Exception as e:
        logger.error(f"❌ Error during category reconciliation: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


def cleanup_old_history_wrapper(**context):
    """
    Task wrapper to archive and delete old crawl history.
    """
    logger = get_logger(context)
    try:
        from common.db_schema import ensure_crawl_schema
        from pipelines.crawl.storage.postgres_storage import PostgresStorage

        storage = PostgresStorage()
        with storage.get_connection() as conn:
            with conn.cursor() as cur:
                ensure_crawl_schema(cur)
            conn.commit()

        # Config via variables
        archive_months = int(get_variable("HISTORY_ARCHIVE_MONTHS", "6"))
        delete_months = int(get_variable("HISTORY_DELETE_MONTHS", "12"))

        logger.info(f"🧹 Cleaning history (Archive: {archive_months}m, Delete: {delete_months}m)")
        result = storage.cleanup_old_history(archive_months, delete_months)

        logger.info(
            f"✅ Done: Archived {result['archived_count']}, Deleted {result['deleted_count']}"
        )
        return result
    except Exception as e:
        logger.error(f"❌ Error during history cleanup: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


# ❌ REMOVED: validate_data task
# Reason: Only counted valid/invalid products, didn't fix errors or affect data quality
# Not critical for pipeline operation, removed to reduce overhead


def cleanup_redis_cache(**context) -> dict[str, Any]:
    """
    Task: Cleanup Redis cache

    Cleanup Redis cache để giải phóng bộ nhớ và đảm bảo cache không quá cũ.
    Task này chạy với trigger_rule="all_done" để chạy ngay cả khi upstream tasks fail.

    Returns:
        Dict: Kết quả cleanup
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("🧹 TASK: Cleanup Redis Cache")
    logger.info("=" * 70)

    result = {
        "status": "failed",
        "redis_reset": False,
        "stats_before": {},
        "stats_after": {},
    }

    try:
        # Ensure src path in sys.path for package-style imports
        import sys
        from pathlib import Path

        src_path = Path("/opt/airflow/src")
        if src_path.exists() and str(src_path) not in sys.path:
            sys.path.insert(0, str(src_path))

        try:
            from pipelines.crawl.storage.redis_cache import get_redis_cache  # type: ignore
        except Exception as import_err:
            logger.warning(
                f"⚠️  Import get_redis_cache failed: {import_err} -> trying dynamic import"
            )
            # Dynamic import fallback
            import importlib.util

            rc_path = src_path / "pipelines" / "crawl" / "storage" / "redis_cache.py"
            get_redis_cache = None  # type: ignore
            if rc_path.exists():
                spec = importlib.util.spec_from_file_location("redis_cache_dyn", rc_path)
                if spec and spec.loader:
                    mod = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(mod)  # type: ignore
                    get_redis_cache = getattr(mod, "get_redis_cache", None)  # type: ignore
            if not get_redis_cache:
                raise RuntimeError(
                    "Không thể import get_redis_cache (dynamic import cũng thất bại)"
                ) from import_err

        # Kết nối Redis
        redis_cache = get_redis_cache("redis://redis:6379/1")  # type: ignore
        if not redis_cache:
            logger.warning("⚠️  Không thể kết nối Redis, skip cleanup")
            result["status"] = "skipped"
            result["reason"] = "Redis not available"
            return result

        # Lấy stats trước khi cleanup (dùng client.info())
        try:
            info_before = redis_cache.client.info()
            db_key = f"db{redis_cache.client.connection_pool.connection_kwargs.get('db', 1)}"
            keys_before = info_before.get(db_key, {}).get("keys", 0)
            hits = info_before.get("keyspace_hits", 0)
            misses = info_before.get("keyspace_misses", 0)
            hit_rate = (hits / (hits + misses) * 100) if (hits + misses) > 0 else 0.0
            stats_before = {
                "keys": keys_before,
                "used_memory_human": info_before.get("used_memory_human"),
                "hit_rate": hit_rate,
                "keyspace_hits": hits,
                "keyspace_misses": misses,
            }
            result["stats_before"] = stats_before
            logger.info("📊 Redis stats trước cleanup:")
            logger.info(f"   - Keys: {keys_before}")
            logger.info(f"   - Memory used: {stats_before.get('used_memory_human', 'N/A')}")
            logger.info(f"   - Hit rate: {hit_rate:.1f}%")
        except Exception as e:
            logger.warning(f"⚠️  Không thể lấy stats trước cleanup: {e}")

        # Reset cache - REMOVED: Redis already uses TTL for expiration.
        # Calling flushdb() wipes the entire cache for every run, causing 100% miss rate in next run.
        logger.info(
            "ℹ️  Redis cleanup: Skipping flushdb() to preserve persistent cache. TTL will handle expiration."
        )
        result["redis_reset"] = False

        # Lấy stats sau khi cleanup
        try:
            import time as _t

            _t.sleep(1)
            info_after = redis_cache.client.info()
            db_key = f"db{redis_cache.client.connection_pool.connection_kwargs.get('db', 1)}"
            keys_after = info_after.get(db_key, {}).get("keys", 0)
            hits_a = info_after.get("keyspace_hits", 0)
            misses_a = info_after.get("keyspace_misses", 0)
            hit_rate_a = (hits_a / (hits_a + misses_a) * 100) if (hits_a + misses_a) > 0 else 0.0
            stats_after = {
                "keys": keys_after,
                "used_memory_human": info_after.get("used_memory_human"),
                "hit_rate": hit_rate_a,
                "keyspace_hits": hits_a,
                "keyspace_misses": misses_a,
            }
            result["stats_after"] = stats_after
            logger.info("📊 Redis stats sau cleanup:")
            logger.info(f"   - Keys: {keys_after}")
            logger.info(f"   - Memory used: {stats_after.get('used_memory_human', 'N/A')}")
        except Exception as e:
            logger.warning(f"⚠️  Không thể lấy stats sau cleanup: {e}")

        result["status"] = "success"
        logger.info("✅ Cleanup Redis cache hoàn tất")

    except Exception as e:
        logger.error(f"❌ Lỗi khi cleanup Redis cache: {e}", exc_info=True)
        result["error"] = str(e)

    logger.info("=" * 70)
    return result


def backup_database(**context) -> dict[str, Any]:
    """
    Task: Backup PostgreSQL database

    Backup database cấu hình bởi POSTGRES_DB vào thư mục backups/postgres sau khi các tasks khác
    hoàn thành.

    Returns:
        Dict: Kết quả backup
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("💾 TASK: Backup Database")
    logger.info("=" * 70)

    try:
        import subprocess
        from pathlib import Path

        # Đường dẫn script backup
        script_path = Path("/opt/airflow/scripts/helper/backup_postgres.py")
        if not script_path.exists():
            # Fallback: thử đường dẫn tương đối
            script_path = (
                Path(__file__).parent.parent.parent / "scripts" / "helper" / "backup_postgres.py"
            )

        if not script_path.exists():
            logger.warning(f"⚠️  Không tìm thấy script backup tại: {script_path}")
            logger.info("💡 Sử dụng pg_dump trực tiếp...")

            # Use pg_dump directly (available in the Airflow container via postgresql-client)
            # Thử nhiều đường dẫn backup - ưu tiên /opt/airflow/data/backups (có quyền ghi)
            backup_dirs = [
                Path(
                    "/opt/airflow/data/backups/postgres"
                ),  # Priority 1: Mounted data dir (có quyền)
                Path("/tmp/backups/postgres"),  # Priority 2: Temp dir (luôn có quyền)
            ]
            backup_dir = None
            for bd in backup_dirs:
                try:
                    bd.mkdir(parents=True, exist_ok=True)
                    # Test write
                    test_file = bd / ".test_write"
                    test_file.write_text("test")
                    test_file.unlink()
                    backup_dir = bd
                    logger.info(f"✅ Sử dụng backup dir: {backup_dir}")
                    break
                except Exception as e:
                    logger.warning(f"⚠️  Không thể ghi vào {bd}: {e}")
                    continue

            if not backup_dir:
                logger.error("❌ Không tìm thấy thư mục backup có thể ghi")
                return {"status": "failed", "reason": "No writable backup directory"}

            db_settings = get_pipeline_settings(use_airflow=False).database
            postgres_user = db_settings.user
            postgres_password = db_settings.password
            db_name = db_settings.database

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = backup_dir / f"{db_name}_{timestamp}.sql"  # Đổi .dump -> .sql

            if not postgres_password:
                logger.warning("⚠️  Không tìm thấy POSTGRES_PASSWORD trong environment")
                return {"status": "skipped", "reason": "No password"}

            logger.info("📦 Đang backup database: %s...", db_name)
            logger.info(f"   File: {backup_file}")

            postgres_host = db_settings.host
            postgres_port = str(db_settings.port)

            # Run pg_dump directly (postgresql-client is installed in the Airflow image)
            cmd = [
                "pg_dump",
                "-h",
                postgres_host,
                "-p",
                postgres_port,
                "-U",
                postgres_user,
                "--format=plain",  # Plain SQL format - dễ restore, tương thích
                "--no-owner",  # Không dump owner info
                "--no-acl",  # Không dump access privileges
                db_name,
            ]

            env = os.environ.copy()
            env["PGPASSWORD"] = postgres_password

            try:
                with open(backup_file, "wb") as f:
                    result = subprocess.run(
                        cmd,
                        stdout=f,
                        stderr=subprocess.PIPE,
                        check=False,
                        timeout=600,  # 10 phút timeout
                        env=env,
                    )

                if result.returncode == 0:
                    file_size = backup_file.stat().st_size
                    size_mb = file_size / (1024 * 1024)
                    logger.info(f"✅ Đã backup thành công: {backup_file.name}")
                    logger.info(f"   Size: {size_mb:.2f} MB")
                    return {
                        "status": "success",
                        "backup_file": str(backup_file),
                        "size_mb": round(size_mb, 2),
                    }
                else:
                    error_msg = result.stderr.decode("utf-8", errors="ignore")
                    logger.error(f"❌ Lỗi khi backup: {error_msg}")
                    if backup_file.exists():
                        backup_file.unlink()
                    return {"status": "failed", "error": error_msg}

            except subprocess.TimeoutExpired:
                logger.error("❌ Timeout khi backup database")
                if backup_file.exists():
                    backup_file.unlink()
                return {"status": "failed", "error": "Timeout"}
            except Exception as e:
                logger.error(f"❌ Exception khi backup: {e}")
                if backup_file.exists():
                    backup_file.unlink()
                return {"status": "failed", "error": str(e)}
        else:
            # Sử dụng script backup (dùng format sql để tránh vấn đề version dump)
            logger.info(f"📦 Đang backup database bằng script: {script_path}")

            db_name = get_pipeline_settings(use_airflow=False).database.database
            cmd = ["python", str(script_path), "--database", db_name, "--format", "sql"]

            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=600,  # 10 phút timeout
                )

                if result.returncode == 0:
                    logger.info("✅ Backup thành công!")
                    if result.stdout:
                        logger.info(result.stdout)
                    return {
                        "status": "success",
                        "output": result.stdout,
                    }
                else:
                    logger.warning(f"⚠️  Backup có lỗi (exit code: {result.returncode})")
                    if result.stdout:
                        logger.info("--- STDOUT ---")
                        logger.info(result.stdout)
                    if result.stderr:
                        logger.warning("--- STDERR ---")
                        logger.warning(result.stderr)
                    # Không fail task, chỉ log warning
                    return {
                        "status": "warning",
                        "error": result.stderr or result.stdout,
                    }
            except subprocess.TimeoutExpired:
                logger.error("❌ Timeout khi backup database")
                return {"status": "failed", "error": "Timeout"}
            except Exception as e:
                logger.error(f"❌ Exception khi backup: {e}")
                return {"status": "failed", "error": str(e)}

    except Exception as e:
        logger.error(f"❌ Lỗi trong backup_database task: {e}", exc_info=True)
        # Không fail task, chỉ log lỗi
        return {"status": "failed", "error": str(e)}
