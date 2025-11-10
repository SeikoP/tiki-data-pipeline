"""
Example Pipeline Template

Template để tạo pipeline mới cho dự án của bạn.
"""
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)


class ExamplePipeline:
    """
    Template class cho data pipeline
    
    Các bước thường có trong pipeline:
    1. Extract: Lấy dữ liệu từ nguồn
    2. Transform: Xử lý/chuyển đổi dữ liệu
    3. Load: Lưu dữ liệu vào đích
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Khởi tạo pipeline với config
        
        Args:
            config: Dictionary chứa cấu hình pipeline
        """
        self.config = config
        logger.info(f"Initialized pipeline with config: {config}")
    
    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract data từ nguồn
        
        Returns:
            List of dictionaries chứa raw data
        """
        logger.info("Extracting data...")
        # TODO: Implement extraction logic
        # Ví dụ: Scrape website, call API, read from database
        return []
    
    def transform(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform raw data thành format mong muốn
        
        Args:
            raw_data: Raw data từ extract step
            
        Returns:
            Transformed data
        """
        logger.info(f"Transforming {len(raw_data)} records...")
        # TODO: Implement transformation logic
        # Ví dụ: Clean data, normalize, enrich
        return raw_data
    
    def load(self, transformed_data: List[Dict[str, Any]]) -> bool:
        """
        Load transformed data vào đích
        
        Args:
            transformed_data: Data đã được transform
            
        Returns:
            True nếu thành công
        """
        logger.info(f"Loading {len(transformed_data)} records...")
        # TODO: Implement loading logic
        # Ví dụ: Save to database, write to file, send to API
        return True
    
    def run(self) -> bool:
        """
        Chạy toàn bộ pipeline: Extract -> Transform -> Load
        
        Returns:
            True nếu thành công
        """
        try:
            # Extract
            raw_data = self.extract()
            
            # Transform
            transformed_data = self.transform(raw_data)
            
            # Load
            success = self.load(transformed_data)
            
            logger.info("Pipeline completed successfully")
            return success
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
            return False


# Example usage
if __name__ == "__main__":
    config = {
        "source": "example",
        "destination": "database"
    }
    
    pipeline = ExamplePipeline(config)
    pipeline.run()

