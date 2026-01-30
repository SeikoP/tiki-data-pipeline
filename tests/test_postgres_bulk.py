import unittest
import os
import sys
from unittest.mock import MagicMock, patch

# Mock dependencies for local testing
import sys
from unittest.mock import MagicMock

# Mock psycopg2 and its sub-modules
import sys
from unittest.mock import MagicMock
mock_psycopg2 = MagicMock()
sys.modules["psycopg2"] = mock_psycopg2
sys.modules["psycopg2.extras"] = MagicMock()
sys.modules["psycopg2.sql"] = MagicMock()
sys.modules["psycopg2.pool"] = MagicMock()
psycopg2 = mock_psycopg2

# Ensure src is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from pipelines.crawl.storage.postgres_storage import PostgresStorage

class TestPostgresBulk(unittest.TestCase):
    def setUp(self):
        self.storage = PostgresStorage()
        self.storage.get_connection = MagicMock()

    def test_save_products_bulk_copy_threshold(self):
        """Verify that save_products uses COPY when batch size is large."""
        products = [{"product_id": f"p{i}", "name": f"Product {i}", "price": 100} for i in range(100)]
        
        with patch.object(self.storage, '_save_products_bulk_copy') as mock_bulk:
            mock_bulk.return_value = {"saved_count": 100}
            self.storage.save_products(products)
            mock_bulk.assert_called_once()

    def test_save_products_execute_values_threshold(self):
        """Verify that save_products uses execute_values for small batches."""
        products = [{"product_id": f"p{i}", "name": f"Product {i}", "price": 100} for i in range(10)]
        
        with patch.object(self.storage, '_save_products_bulk_copy') as mock_bulk:
            with patch('psycopg2.extras.execute_values') as mock_exec:
                self.storage.save_products(products)
                mock_bulk.assert_not_called()

if __name__ == "__main__":
    unittest.main()
