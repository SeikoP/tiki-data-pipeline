from __future__ import annotations

# Import all bootstrap globals (paths, config, dynamic imports, singletons).
# This preserves legacy behavior without renaming any globals referenced by task callables.
from ..bootstrap import CATEGORIES_FILE, Any, Path, datetime, json, os, redis_cache, src_path, sys
from .common import _fix_sys_path_for_pipelines_import  # noqa: F401
from .common import get_logger  # noqa: F401


def cleanup_incomplete_products_wrapper(**context):
    """
    Task wrapper to cleanup products with missing required fields (seller and/or brand).
    Run this BEFORE crawling to allow re-crawling of incomplete data.

    This is a PREVENTIVE cleanup - better to clean before crawl than after load.
    """
    logger = get_logger(context)

    try:
        from pipelines.crawl.storage.postgres_storage import PostgresStorage

        logger.info("🧹 Starting cleanup of incomplete products (missing seller/brand)...")
        storage = PostgresStorage()

        # Clean up products missing seller OR brand (or both)
        # Both are required for quality data
        # require_rating=True to satisfy user request for deleting null ratings
        stats = storage.cleanup_incomplete_products(
            require_seller=True, require_brand=True, require_rating=True
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


def cleanup_orphan_categories_wrapper(**context):
    """
    Task wrapper to cleanup categories that don't have any matching products.
    Run this after loading categories to keep the table clean.

    Xóa:
    1. Categories có product_count = 0 (hoặc NULL)
    2. Leaf categories không có products trong bảng products
    """
    logger = get_logger(context)

    try:
        from pipelines.crawl.storage.postgres_storage import PostgresStorage

        logger.info("=" * 70)
        logger.info("🧹 CLEANUP ORPHAN CATEGORIES")
        logger.info("=" * 70)

        storage = PostgresStorage()

        # 1. Xóa categories có product_count = 0 hoặc NULL
        with storage.get_connection() as conn:
            with conn.cursor() as cur:
                # Xóa categories có product_count = 0 hoặc NULL
                cur.execute("""
                    DELETE FROM categories
                    WHERE (product_count = 0 OR product_count IS NULL)
                    AND is_leaf = true
                """)
                deleted_zero_count = cur.rowcount

                # 2. Xóa leaf categories không có products trong bảng products
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

                total_deleted = deleted_zero_count + deleted_no_products

                logger.info("📊 Cleanup results:")
                logger.info(f"   - Categories với product_count = 0: {deleted_zero_count}")
                logger.info(f"   - Categories không có products: {deleted_no_products}")
                logger.info(f"   - Tổng cộng: {total_deleted} categories đã xóa")
                logger.info("=" * 70)

        # Gọi cleanup_orphan_categories từ storage để đảm bảo consistency
        # (nó sẽ xóa các categories không có products)
        additional_deleted = storage.cleanup_orphan_categories()

        total_deleted = total_deleted + additional_deleted

        logger.info(f"✅ Cleanup complete: {total_deleted} orphan categories deleted")
        return {
            "status": "success",
            "deleted_count": total_deleted,
            "deleted_zero_count": deleted_zero_count,
            "deleted_no_products": deleted_no_products + additional_deleted,
        }

    except Exception as e:
        logger.error(f"❌ Error during cleanup: {e}", exc_info=True)
        return {"status": "error", "message": str(e), "deleted_count": 0}


def cleanup_redundant_categories_wrapper(**context):
    """
    Task wrapper to remove non-leaf categories.
    """
    logger = get_logger(context)
    try:
        from pipelines.crawl.storage.postgres_storage import PostgresStorage

        storage = PostgresStorage()
        removed = storage.cleanup_redundant_categories()
        logger.info(f"✅ Removed {removed} non-leaf categories")
        return {"status": "success", "removed": removed}
    except Exception as e:
        logger.error(f"❌ Error during redundant category cleanup: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


def reconcile_categories_wrapper(**context):
    """
    Task wrapper to reconcile categories from JSON.
    Updates names, removes orphans, and updates product counts.
    """
    logger = get_logger(context)
    try:
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
        from pipelines.crawl.storage.postgres_storage import PostgresStorage

        storage = PostgresStorage()

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


def validate_data(**context) -> dict[str, Any]:
    """
    Task 5: Validate dữ liệu đã crawl

    Returns:
        Dict: Kết quả validation
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("✅ TASK: Validate Data")
    logger.info("=" * 70)

    try:
        ti = context["ti"]
        output_file = None

        # Ưu tiên: Lấy từ save_products_with_detail (có detail)
        # Cách 1: Lấy từ task_id với TaskGroup prefix
        try:
            output_file = ti.xcom_pull(task_ids="crawl_product_details.save_products_with_detail")
            logger.info(
                f"Lấy output_file từ 'crawl_product_details.save_products_with_detail': {output_file}"
            )
        except Exception as e:
            logger.warning(
                f"Không lấy được từ 'crawl_product_details.save_products_with_detail': {e}"
            )

        # Cách 2: Thử không có prefix
        if not output_file:
            try:
                output_file = ti.xcom_pull(task_ids="save_products_with_detail")
                logger.debug(f"Output from save_products_with_detail: {output_file}")
            except Exception as e:
                logger.warning(f"Không lấy được từ 'save_products_with_detail': {e}")

        # Fallback: Lấy từ save_products (không có detail) nếu không có file với detail
        if not output_file:
            try:
                output_file = ti.xcom_pull(task_ids="process_and_save.save_products")
                logger.info(
                    f"Lấy output_file từ 'process_and_save.save_products' (fallback): {output_file}"
                )
            except Exception as e:
                logger.warning(f"Không lấy được từ 'process_and_save.save_products': {e}")

        # Cách 3: Thử không có prefix
        if not output_file:
            try:
                output_file = ti.xcom_pull(task_ids="save_products")
                logger.debug(f"Output from save_products (fallback): {output_file}")
            except Exception as e:
                logger.warning(f"Không lấy được từ 'save_products': {e}")

        if not output_file or not os.path.exists(output_file):
            raise FileNotFoundError(f"Không tìm thấy file output: {output_file}")

        logger.info(f"Đang validate file: {output_file}")

        with open(output_file, encoding="utf-8") as f:
            data = json.load(f)

        products = data.get("products", [])
        stats = data.get("stats", {})

        # Validation
        validation_result = {
            "file_exists": True,
            "total_products": len(products),
            "crawled_count": stats.get("crawled_count", 0),  # Số lượng products được crawl detail
            "valid_products": 0,
            "invalid_products": 0,
            "errors": [],
        }

        required_fields = ["product_id", "name", "url"]

        for i, product in enumerate(products):
            is_valid = True
            missing_fields = []

            for field in required_fields:
                if not product.get(field):
                    is_valid = False
                    missing_fields.append(field)

            if is_valid:
                validation_result["valid_products"] += 1
            else:
                validation_result["invalid_products"] += 1
                validation_result["errors"].append(
                    {
                        "index": i,
                        "product_id": product.get("product_id"),
                        "missing_fields": missing_fields,
                    }
                )

        logger.info("=" * 70)
        logger.info("📊 VALIDATION RESULTS")
        logger.info("=" * 70)
        logger.info(f"📦 Tổng số products trong file: {validation_result['total_products']}")

        # Log thông tin về crawl detail nếu có
        crawled_count = stats.get("crawled_count", 0)
        if crawled_count > 0:
            logger.info(f"🔄 Products được crawl detail: {crawled_count}")
            logger.info(f"✅ Products có detail (success): {stats.get('with_detail', 0)}")
            if stats.get("timeout", 0) > 0:
                logger.info(f"⏱️  Products timeout: {stats.get('timeout', 0)}")
            if stats.get("failed", 0) > 0:
                logger.info(f"❌ Products failed: {stats.get('failed', 0)}")

        logger.info(f"✅ Valid products: {validation_result['valid_products']}")
        logger.info(f"❌ Invalid products: {validation_result['invalid_products']}")
        logger.info("=" * 70)

        if validation_result["invalid_products"] > 0:
            logger.warning(f"Có {validation_result['invalid_products']} sản phẩm không hợp lệ")
            # Không fail task, chỉ warning

        return validation_result

    except Exception as e:
        logger.error(f"❌ Lỗi khi validate data: {e}", exc_info=True)
        raise


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

        # Reset cache
        logger.info("🧹 Đang cleanup Redis cache...")
        try:
            redis_cache.client.flushdb()
            result["redis_reset"] = True
            logger.info("✅ Đã flush DB Redis cache thành công")
        except Exception as e:
            logger.error(f"❌ Flush DB thất bại: {e}")

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

    Backup database crawl_data vào thư mục backups/postgres sau khi các tasks khác hoàn thành.

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

            # Fallback: sử dụng pg_dump trực tiếp
            container_name = "tiki-data-pipeline-postgres-1"
            # Thử nhiều đường dẫn backup
            backup_dirs = [
                Path("/opt/airflow/backups/postgres"),  # Trong container Airflow
                Path("/backups"),  # Mount từ postgres container
                Path("/opt/airflow/data/backups/postgres"),  # Fallback
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
                    break
                except Exception:
                    continue

            if not backup_dir:
                logger.warning("⚠️  Không tìm thấy thư mục backup có thể ghi, sử dụng /tmp")
                backup_dir = Path("/tmp/backups")
                backup_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = backup_dir / f"crawl_data_{timestamp}.sql"  # Đổi .dump -> .sql

            # Lấy thông tin từ environment variables
            postgres_user = os.getenv("POSTGRES_USER", "airflow_user")
            postgres_password = os.getenv("POSTGRES_PASSWORD", "")

            if not postgres_password:
                logger.warning("⚠️  Không tìm thấy POSTGRES_PASSWORD trong environment")
                return {"status": "skipped", "reason": "No password"}

            logger.info("📦 Đang backup database: crawl_data...")
            logger.info(f"   File: {backup_file}")

            # Chạy pg_dump trong container - dùng plain SQL format
            cmd = [
                "docker",
                "exec",
                "-e",
                f"PGPASSWORD={postgres_password}",
                container_name,
                "pg_dump",
                "-U",
                postgres_user,
                "--format=plain",  # Plain SQL format - dễ restore, tương thích
                "--no-owner",  # Không dump owner info
                "--no-acl",  # Không dump access privileges
                "crawl_data",
            ]

            try:
                with open(backup_file, "wb") as f:
                    result = subprocess.run(
                        cmd,
                        stdout=f,
                        stderr=subprocess.PIPE,
                        check=False,
                        timeout=600,  # 10 phút timeout
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

            cmd = ["python", str(script_path), "--database", "crawl_data", "--format", "sql"]

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
