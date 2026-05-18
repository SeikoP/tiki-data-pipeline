from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def test_tiki_analytics_normalize_dag_imports_without_airflow_runtime():
    root = Path(__file__).parent.parent
    src = root / "src"
    dags = root / "airflow" / "dags"
    for path in (src, dags):
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))

    dag_path = dags / "tiki_analytics_normalize.py"
    spec = importlib.util.spec_from_file_location("tiki_analytics_normalize", dag_path)

    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert hasattr(module, "dag")
    assert callable(module.resolve_analytics_context)
    assert module.dag_config["schedule"] == [module.TIKI_CORE_TABLES_ASSET]


def test_tiki_core_tables_asset_has_stable_non_secret_uri():
    root = Path(__file__).parent.parent
    dags = root / "airflow" / "dags"
    if str(dags) not in sys.path:
        sys.path.insert(0, str(dags))

    from tiki_assets import TIKI_CORE_TABLES_ASSET, TIKI_CORE_TABLES_ASSET_URI

    assert TIKI_CORE_TABLES_ASSET_URI == "postgres://tiki/public/core_product_tables"
    assert "password" not in TIKI_CORE_TABLES_ASSET_URI
    assert getattr(TIKI_CORE_TABLES_ASSET, "uri", str(TIKI_CORE_TABLES_ASSET)) in {
        TIKI_CORE_TABLES_ASSET_URI,
        f"Dataset(uri='{TIKI_CORE_TABLES_ASSET_URI}')",
    }
