import json
import os
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Set, Tuple

class DataVerifier:
    """Comprehensive data verification for Tiki category data"""
    
    def __init__(self, file_path: str = "data/raw/categories_recursive_optimized.json"):
        self.file_path = Path(file_path)
        self.data = []
        self.errors = []
        self.warnings = []
        self.stats = {
            'total_items': 0,
            'levels': defaultdict(int),
            'duplicates': 0,
            'orphans': 0,
            'invalid_urls': 0,
            'empty_names': 0,
            'invalid_levels': 0,
            'missing_parents': 0
        }
        
    def log_error(self, message: str, index: int = None):
        """Log error message"""
        if index is not None:
            self.errors.append(f"[Index {index}] {message}")
        else:
            self.errors.append(message)
            
    def log_warning(self, message: str, index: int = None):
        """Log warning message"""
        if index is not None:
            self.warnings.append(f"[Index {index}] {message}")
        else:
            self.warnings.append(message)
    
    def verify_file_exists(self) -> bool:
        """Check if file exists"""
        print(f"🔍 Kiểm tra file: {self.file_path}")
        
        if not self.file_path.exists():
            self.log_error("File không tồn tại! Hãy chạy crawler trước.")
            return False
            
        print("✅ File tồn tại")
        return True
    
    def load_json(self) -> bool:
        """Load and validate JSON structure"""
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                self.data = json.load(f)
        except json.JSONDecodeError as e:
            self.log_error(f"File không phải là JSON hợp lệ: {str(e)}")
            return False
        except UnicodeDecodeError as e:
            self.log_error(f"Lỗi encoding khi đọc file: {str(e)}")
            return False
            
        if not isinstance(self.data, list):
            self.log_error("Dữ liệu gốc không phải là một danh sách (list).")
            return False
            
        self.stats['total_items'] = len(self.data)
        print(f"✅ JSON hợp lệ: {self.stats['total_items']} items")
        
        if self.stats['total_items'] == 0:
            self.log_warning("Danh sách rỗng - không có dữ liệu để kiểm tra")
            return False
            
        return True
    
    def verify_required_fields(self) -> bool:
        """Verify all required fields exist and are valid"""
        print("\n📋 Kiểm tra trường bắt buộc...")
        
        required_fields = ["name", "url", "level"]
        missing_counts = defaultdict(int)
        has_error = False
        
        for idx, item in enumerate(self.data):
            if not isinstance(item, dict):
                self.log_error(f"Item không phải là dictionary", idx)
                has_error = True
                continue
                
            # Check required fields exist
            for field in required_fields:
                if field not in item:
                    missing_counts[field] += 1
                    if missing_counts[field] <= 5:  # Log first 5
                        self.log_error(f"Thiếu trường '{field}'", idx)
                    has_error = True
        
        # Report missing fields
        if missing_counts:
            for field, count in missing_counts.items():
                print(f"   ❌ Trường '{field}' thiếu trong {count} mục")
        else:
            print("   ✅ Tất cả trường bắt buộc đều có")
            
        return not has_error
    
    def verify_field_values(self) -> bool:
        """Verify field values are valid"""
        print("\n🔎 Kiểm tra giá trị trường...")
        
        has_error = False
        
        for idx, item in enumerate(self.data):
            # Check name is not empty
            name = item.get("name", "")
            if not isinstance(name, str) or not name.strip():
                self.stats['empty_names'] += 1
                if self.stats['empty_names'] <= 5:
                    self.log_error(f"Trường 'name' rỗng hoặc không hợp lệ: '{name}'", idx)
                has_error = True
            
            # Check URL format
            url = item.get("url", "")
            if not isinstance(url, str) or not url.startswith("https://tiki.vn"):
                self.stats['invalid_urls'] += 1
                if self.stats['invalid_urls'] <= 5:
                    self.log_error(f"URL không hợp lệ: '{url}'", idx)
                has_error = True
            
            # Check level is valid integer >= 0
            level = item.get("level")
            if not isinstance(level, int) or level < 0:
                self.stats['invalid_levels'] += 1
                if self.stats['invalid_levels'] <= 5:
                    self.log_error(f"Level không hợp lệ (phải là số nguyên >= 0): {level}", idx)
                has_error = True
            else:
                self.stats['levels'][level] += 1
        
        # Report statistics
        if self.stats['empty_names'] > 0:
            print(f"   ❌ Có {self.stats['empty_names']} mục có tên rỗng")
        else:
            print("   ✅ Tất cả tên hợp lệ")
            
        if self.stats['invalid_urls'] > 0:
            print(f"   ❌ Có {self.stats['invalid_urls']} URL không hợp lệ")
        else:
            print("   ✅ Tất cả URL hợp lệ")
            
        if self.stats['invalid_levels'] > 0:
            print(f"   ❌ Có {self.stats['invalid_levels']} level không hợp lệ")
        else:
            print("   ✅ Tất cả level hợp lệ")
            
        return not has_error
    
    def verify_duplicates(self) -> bool:
        """Check for duplicate URLs"""
        print("\n🔄 Kiểm tra trùng lặp...")
        
        url_counts = defaultdict(list)
        
        for idx, item in enumerate(self.data):
            url = item.get("url")
            if url:
                url_counts[url].append(idx)
        
        duplicates = {url: indices for url, indices in url_counts.items() if len(indices) > 1}
        
        if duplicates:
            self.stats['duplicates'] = sum(len(indices) - 1 for indices in duplicates.values())
            print(f"   ❌ Tìm thấy {len(duplicates)} URL bị trùng lặp:")
            
            for url, indices in list(duplicates.items())[:5]:  # Show first 5
                print(f"      • '{url}' xuất hiện {len(indices)} lần tại: {indices}")
            
            if len(duplicates) > 5:
                print(f"      ... và {len(duplicates) - 5} URL trùng lặp khác")
                
            return False
        else:
            print("   ✅ Không có URL trùng lặp")
            return True
    
    def verify_hierarchy(self) -> bool:
        """Verify level distribution and hierarchy logic"""
        print("\n📊 Kiểm tra phân cấp...")
        
        has_warning = False
        
        # Check level distribution
        if self.stats['levels']:
            print("   Phân bố theo Level:")
            for lvl in sorted(self.stats['levels'].keys()):
                print(f"      - Level {lvl}: {self.stats['levels'][lvl]} danh mục")
            
            # Check for root category
            if 0 not in self.stats['levels']:
                self.log_warning("Không tìm thấy danh mục gốc (Level 0)")
                has_warning = True
            else:
                print(f"   ✅ Có {self.stats['levels'][0]} danh mục gốc (Level 0)")
        else:
            self.log_warning("Không có dữ liệu level")
            has_warning = True
            
        return not has_warning
    
    def verify_parent_child_links(self) -> bool:
        """Verify parent-child relationships"""
        print("\n🔗 Kiểm tra liên kết Parent-Child...")
        
        url_set = {item.get("url") for item in self.data if item.get("url")}
        has_error = False
        
        for idx, item in enumerate(self.data):
            level = item.get("level", 0)
            parent_url = item.get("parent_url")
            
            # Level > 0 must have parent_url
            if level > 0:
                if not parent_url:
                    self.stats['missing_parents'] += 1
                    if self.stats['missing_parents'] <= 5:
                        self.log_error(f"Danh mục level {level} thiếu parent_url", idx)
                    has_error = True
                elif parent_url not in url_set:
                    self.stats['orphans'] += 1
                    if self.stats['orphans'] <= 5:
                        self.log_warning(f"Parent URL '{parent_url}' không tồn tại trong danh sách", idx)
            
            # Level 0 should not have parent_url
            elif level == 0 and parent_url:
                self.log_warning(f"Danh mục Level 0 không nên có parent_url: '{parent_url}'", idx)
        
        # Report results
        if self.stats['missing_parents'] > 0:
            print(f"   ❌ Có {self.stats['missing_parents']} danh mục con thiếu parent_url")
            has_error = True
        else:
            print("   ✅ Tất cả danh mục con đều có parent_url")
        
        if self.stats['orphans'] > 0:
            print(f"   ⚠️  Có {self.stats['orphans']} danh mục con có parent URL không hợp lệ")
            print("       (Có thể bình thường nếu parent nằm ngoài phạm vi crawl)")
        else:
            print("   ✅ Tất cả parent URL đều hợp lệ")
            
        return not has_error
    
    def print_summary(self) -> bool:
        """Print verification summary"""
        print("\n" + "=" * 60)
        print("📊 TÓM TẮT KẾT QUẢ KIỂM TRA")
        print("=" * 60)
        
        print(f"\n📦 Tổng số mục: {self.stats['total_items']}")
        
        if self.errors:
            print(f"\n❌ LỖI ({len(self.errors)}):")
            for error in self.errors[:10]:  # Show first 10
                print(f"   • {error}")
            if len(self.errors) > 10:
                print(f"   ... và {len(self.errors) - 10} lỗi khác")
        
        if self.warnings:
            print(f"\n⚠️  CẢNH BÁO ({len(self.warnings)}):")
            for warning in self.warnings[:10]:  # Show first 10
                print(f"   • {warning}")
            if len(self.warnings) > 10:
                print(f"   ... và {len(self.warnings) - 10} cảnh báo khác")
        
        print("\n" + "-" * 60)
        
        has_critical_error = len(self.errors) > 0
        
        if has_critical_error:
            print("⛔ KẾT QUẢ: DỮ LIỆU CÓ LỖI - CẦN KIỂM TRA LẠI CRAWLER")
            return False
        elif self.warnings:
            print("⚠️  KẾT QUẢ: DỮ LIỆU HỢP LỆ NHƯNG CÓ CẢNH BÁO")
            return True
        else:
            print("🎉 KẾT QUẢ: DỮ LIỆU HOÀN TOÀN HỢP LỆ")
            return True
    
    def save_report(self, output_file: str = "data/verification_report.txt"):
        """Save detailed report to file"""
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("=" * 60 + "\n")
            f.write(f"BÁO CÁO KIỂM TRA DỮ LIỆU\n")
            f.write(f"Thời gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"File: {self.file_path}\n")
            f.write("=" * 60 + "\n\n")
            
            f.write(f"Tổng số mục: {self.stats['total_items']}\n\n")
            
            f.write("THỐNG KÊ:\n")
            f.write(f"  - URL trùng lặp: {self.stats['duplicates']}\n")
            f.write(f"  - URL không hợp lệ: {self.stats['invalid_urls']}\n")
            f.write(f"  - Tên rỗng: {self.stats['empty_names']}\n")
            f.write(f"  - Level không hợp lệ: {self.stats['invalid_levels']}\n")
            f.write(f"  - Thiếu parent_url: {self.stats['missing_parents']}\n")
            f.write(f"  - Parent URL không tồn tại: {self.stats['orphans']}\n\n")
            
            if self.stats['levels']:
                f.write("PHÂN BỐ LEVEL:\n")
                for lvl in sorted(self.stats['levels'].keys()):
                    f.write(f"  - Level {lvl}: {self.stats['levels'][lvl]} danh mục\n")
                f.write("\n")
            
            if self.errors:
                f.write(f"LỖI ({len(self.errors)}):\n")
                for error in self.errors:
                    f.write(f"  • {error}\n")
                f.write("\n")
            
            if self.warnings:
                f.write(f"CẢNH BÁO ({len(self.warnings)}):\n")
                for warning in self.warnings:
                    f.write(f"  • {warning}\n")
        
        print(f"\n💾 Báo cáo chi tiết đã được lưu tại: {output_path}")
    
    def run(self, save_report: bool = True) -> bool:
        """Run all verification checks"""
        print("🚀 BẮT ĐẦU KIỂM TRA DỮ LIỆU\n")
        
        # Step by step verification
        if not self.verify_file_exists():
            return False
        
        if not self.load_json():
            return False
        
        # Run all checks (don't stop on first failure)
        results = []
        results.append(self.verify_required_fields())
        results.append(self.verify_field_values())
        results.append(self.verify_duplicates())
        results.append(self.verify_hierarchy())
        results.append(self.verify_parent_child_links())
        
        # Print summary
        is_valid = self.print_summary()
        
        # Save report if requested
        if save_report:
            self.save_report()
        
        return is_valid


def main():
    """Main entry point"""
    # Get file path from command line or use default
    file_path = sys.argv[1] if len(sys.argv) > 1 else "data/raw/categories_recursive_optimized.json"
    
    # Run verification
    verifier = DataVerifier(file_path)
    is_valid = verifier.run(save_report=True)
    
    # Exit with appropriate code
    sys.exit(0 if is_valid else 1)


if __name__ == "__main__":
    main()