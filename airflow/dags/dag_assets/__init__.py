"""
Asset/Dataset definitions for Tiki crawl products DAG
"""
from ..dag_helpers.utils import Variable

# Check if Dataset is available (Airflow 2.7+)
try:
    from airflow.datasets import Dataset
    DATASET_AVAILABLE = True
except ImportError:
    DATASET_AVAILABLE = False
    # Create dummy Dataset class for backward compatibility
    class Dataset:
        def __init__(self, uri):
            self.uri = uri

        def __repr__(self):
            return f"Dataset({self.uri!r})"

# Check if Asset scheduling is enabled
try:
    use_asset_scheduling = Variable.get("TIKI_USE_ASSET_SCHEDULING", default_var="false").lower() == "true"
except Exception:
    use_asset_scheduling = False

# Define Dataset objects
RAW_PRODUCTS_DATASET = Dataset("tiki://products/raw") if DATASET_AVAILABLE else None
PRODUCTS_WITH_DETAIL_DATASET = Dataset("tiki://products/with_detail") if DATASET_AVAILABLE else None
TRANSFORMED_PRODUCTS_DATASET = Dataset("tiki://products/transformed") if DATASET_AVAILABLE else None
FINAL_PRODUCTS_DATASET = Dataset("tiki://products/final") if DATASET_AVAILABLE else None

def get_outlets_for_task(task_name):
    """Get outlets for a task if Asset scheduling is enabled"""
    if not use_asset_scheduling or not DATASET_AVAILABLE:
        return None
    
    outlets_map = {
        "save_products": [RAW_PRODUCTS_DATASET],
        "save_products_with_detail": [PRODUCTS_WITH_DETAIL_DATASET],
        "transform_products": [TRANSFORMED_PRODUCTS_DATASET],
        "load_products": [FINAL_PRODUCTS_DATASET],
    }
    
    return outlets_map.get(task_name)
