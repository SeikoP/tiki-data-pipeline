# Scripts Directory

**Last Updated:** November 19, 2025  
**Status:** ✅ Cleaned & Organized  
**Active Scripts:** 15 files  
**Archived Scripts:** 14 files (moved to `scripts/archive/`)

---

## 📊 **Quick Overview**

| Category | Count | Purpose |
|----------|-------|---------|
| **Database Management** | 5 scripts | PostgreSQL backup, restore, reset |
| **Airflow Management** | 4 scripts | DAG setup, variables, verification |
| **Docker Management** | 3 scripts | Cleanup, rebuild, optimization |
| **CI/CD & Deployment** | 2 scripts | CI pipeline, deployment |
| **Data Utilities** | 3 scripts | Data loading, visualization |
| **Helper Tools** | 3 subdirs | Utilities, docs, backups |
| **Archived** | 14 scripts | Phase scripts, one-time fixes |

---

## 📁 **Directory Structure**

```
scripts/
├── README.md (this file)
├── archive/                    # Archived scripts
│   ├── README.md
│   └── [14 archived files]
├── docs/                       # Documentation build scripts
│   ├── build_data_story.py
│   └── credentials/
├── helper/                     # Helper utilities
│   ├── backup_postgres.py
│   ├── check_and_fix_category_id.py
│   ├── check_data_freshness.py
│   ├── find_uncrawled_products.py
│   ├── fix_category_path_null.py
│   └── load_existing_data_e2e.py
├── utils/                      # Analysis utilities
│   ├── analyze_failed_tasks.py
│   ├── check_code_quality.py
│   └── check_errors.py
└── [Active scripts listed below]
```

---

## ✅ **Active Scripts**

### **Database Management**

#### `backup-postgres.ps1` / `backup-postgres.sh`
**Purpose:** Backup PostgreSQL database  
**Usage:**
```bash
# PowerShell
.\scripts\backup-postgres.ps1

# Bash
./scripts/backup-postgres.sh
```
**Output:** `backups/postgres/<timestamp>.sql`

#### `restore-postgres.ps1`
**Purpose:** Restore PostgreSQL database from backup  
**Usage:**
```bash
.\scripts\restore-postgres.ps1 [backup-file]
```

#### `reset_postgres_password.ps1` / `reset_postgres_password_simple.ps1`
**Purpose:** Reset PostgreSQL password  
**Usage:**
```bash
.\scripts\reset_postgres_password.ps1
```

#### `database_reset_advisor.py`
**Purpose:** Analyze database and provide reset recommendations  
**Usage:**
```bash
python scripts/database_reset_advisor.py
```
**Features:**
- Checks database size
- Analyzes data quality
- Recommends reset strategy

#### `wait_for_postgres.sh`
**Purpose:** Wait for PostgreSQL to be ready (Docker startup)  
**Usage:**
```bash
./scripts/wait_for_postgres.sh
```

---

### **Airflow Management**

#### `setup_airflow_variables.py`
**Purpose:** Setup Airflow variables for DAGs  
**Usage:**
```bash
python scripts/setup_airflow_variables.py
```
**Sets variables:**
- `tiki_max_products_per_category`
- `tiki_batch_size`
- `tiki_timeout`
- And more...

#### `setup_crawl_optimization.py`
**Purpose:** Setup crawl optimization parameters  
**Usage:**
```bash
python scripts/setup_crawl_optimization.py
```

#### `verify_airflow_mount.ps1` / `verify_airflow_mount.sh`
**Purpose:** Verify Airflow volume mounts are correct  
**Usage:**
```bash
# PowerShell
.\scripts\verify_airflow_mount.ps1

# Bash
./scripts/verify_airflow_mount.sh
```

#### `check_airflow_connection.sh`
**Purpose:** Check Airflow database connection  
**Usage:**
```bash
./scripts/check_airflow_connection.sh
```

#### `diagnose_postgres_connection.sh`
**Purpose:** Diagnose PostgreSQL connection issues  
**Usage:**
```bash
./scripts/diagnose_postgres_connection.sh
```

---

### **Docker Management**

#### `docker-cleanup.ps1` / `docker-cleanup.sh`
**Purpose:** Clean up Docker resources (containers, volumes, networks)  
**Usage:**
```bash
# PowerShell
.\scripts\docker-cleanup.ps1

# Bash
./scripts/docker-cleanup.sh
```
**Cleans:**
- Stopped containers
- Dangling images
- Unused volumes
- Unused networks

#### `rebuild_airflow_image.ps1`
**Purpose:** Rebuild Airflow Docker image  
**Usage:**
```bash
.\scripts\rebuild_airflow_image.ps1
```
**When to use:**
- After modifying `airflow/Dockerfile`
- After changing Airflow dependencies
- When Airflow image has issues

---

### **CI/CD & Deployment**

#### `ci.ps1`
**Purpose:** Run CI pipeline (lint, format, type-check, test)  
**Usage:**
```bash
# Full CI
.\scripts\ci.ps1 ci-local

# Individual checks
.\scripts\ci.ps1 lint
.\scripts\ci.ps1 format
.\scripts\ci.ps1 test
.\scripts\ci.ps1 validate-dags
```
**Tools used:**
- `ruff` (linting)
- `black` + `isort` (formatting)
- `mypy` (type-checking)
- `pytest` (testing)
- `bandit` (security)

#### `deploy.ps1`
**Purpose:** Deploy application (build, test, deploy)  
**Usage:**
```bash
.\scripts\deploy.ps1
```

#### `cleanup.ps1`
**Purpose:** Clean up build artifacts and cache files  
**Usage:**
```bash
.\scripts\cleanup.ps1
```

#### `reset_to_default.ps1`
**Purpose:** Reset configuration to defaults  
**Usage:**
```bash
.\scripts\reset_to_default.ps1
```

---

### **Data Utilities**

#### `load_categories_to_db.py`
**Purpose:** Load categories from JSON to database  
**Usage:**
```bash
python scripts/load_categories_to_db.py
```
**Input:** `data/categories.json`  
**Output:** Categories loaded to PostgreSQL

#### `visualize_final_data.py`
**Purpose:** Visualize final database data statistics  
**Usage:**
```bash
python scripts/visualize_final_data.py
```
**Features:**
- Product count by category
- Price distribution
- Sales statistics
- Data quality metrics

#### `apply-optimizations.ps1`
**Purpose:** Apply optimization configurations  
**Usage:**
```bash
.\scripts\apply-optimizations.ps1
```

---

### **Helper Scripts Subdirectories**

#### `helper/` Directory
**Purpose:** Database & data helper utilities

| Script | Purpose |
|--------|---------|
| `backup_postgres.py` | Python PostgreSQL backup |
| `check_and_fix_category_id.py` | Fix category ID issues |
| `check_data_freshness.py` | Check data staleness |
| `find_uncrawled_products.py` | Find products not yet crawled |
| `fix_category_path_null.py` | Fix null category paths |
| `load_existing_data_e2e.py` | Load existing data end-to-end |

#### `utils/` Directory
**Purpose:** Code analysis utilities

| Script | Purpose |
|--------|---------|
| `analyze_failed_tasks.py` | Analyze Airflow failed tasks |
| `check_code_quality.py` | Check code quality metrics |
| `check_errors.py` | Check for errors in codebase |

#### `docs/` Directory
**Purpose:** Documentation generation

| Script | Purpose |
|--------|---------|
| `build_data_story.py` | Build data story documentation |
| `credentials/` | Credentials for doc generation |

---

## 🗑️ **Archived Scripts**

**14 scripts** moved to `scripts/archive/`:

| Category | Scripts |
|----------|---------|
| **Phase Scripts** | `phase4_optimization.py`, `phase4_step2_parallel.py`, `phase5_infrastructure.py`, `reapply_phase2.py` |
| **Analysis** | `optimization_analysis.py`, `optimization_summary.py`, `monitor_crawl_optimization.py` |
| **One-Time Fixes** | `fix_category_path_issue.py`, `fix_category_path_null.py`, `enrich_categories_with_paths.py` |
| **Schema** | `apply_schema_changes.py`, `analyze_db_and_propose_3nf.py` |
| **Examples** | `example_save_to_postgres.py`, `create_test_dag.py` |

**See:** `scripts/archive/README.md` for details

---

## 🚀 **Common Workflows**

### **Starting Fresh**
```bash
# 1. Clean Docker
.\scripts\docker-cleanup.ps1

# 2. Reset database password (if needed)
.\scripts\reset_postgres_password.ps1

# 3. Setup Airflow variables
python scripts/setup_airflow_variables.py

# 4. Verify mounts
.\scripts\verify_airflow_mount.ps1
```

### **Before Deployment**
```bash
# Run full CI
.\scripts\ci.ps1 ci-local

# Clean artifacts
.\scripts\cleanup.ps1

# Rebuild Airflow image
.\scripts\rebuild_airflow_image.ps1

# Deploy
.\scripts\deploy.ps1
```

### **Backup & Restore**
```bash
# Backup
.\scripts\backup-postgres.ps1

# Restore
.\scripts\restore-postgres.ps1 backups/postgres/backup_20251119.sql
```

### **Troubleshooting**
```bash
# Check Airflow connection
./scripts/check_airflow_connection.sh

# Diagnose PostgreSQL
./scripts/diagnose_postgres_connection.sh

# Analyze database
python scripts/database_reset_advisor.py
```

---

## 📝 **Adding New Scripts**

### **Naming Convention**
- PowerShell: `verb-noun.ps1` (e.g., `backup-postgres.ps1`)
- Bash: `verb_noun.sh` (e.g., `backup_postgres.sh`)
- Python: `verb_noun.py` (e.g., `setup_airflow_variables.py`)

### **Script Template (Python)**
```python
"""
Script: <script_name>.py

Purpose: <What this script does>
Usage: python scripts/<script_name>.py [args]
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def main():
    """Main function"""
    print("=" * 70)
    print("<SCRIPT TITLE>")
    print("=" * 70)
    
    # Your code here
    
    print("\nCompleted successfully!")

if __name__ == "__main__":
    main()
```

### **Script Template (PowerShell)**
```powershell
<#
.SYNOPSIS
    <Brief description>

.DESCRIPTION
    <Detailed description>

.EXAMPLE
    .\<script_name>.ps1
#>

Write-Host "Starting <script_name>..." -ForegroundColor Cyan

# Your code here

Write-Host "Completed!" -ForegroundColor Green
```

---

## 🔧 **Script Maintenance**

### **Active Scripts**
- Keep updated with code changes
- Test regularly
- Document usage and examples
- Add error handling

### **Helper Scripts**
- Keep focused and single-purpose
- Document dependencies
- Add usage examples
- Test edge cases

### **Deprecated Scripts**
- Move to `scripts/archive/`
- Document reason for archival
- Keep for historical reference
- Update archive README

---

## 🆘 **Troubleshooting**

### **Permission Denied (PowerShell)**
```bash
# Enable script execution
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### **Python Import Errors**
```bash
# Ensure you're in project root
cd e:\Project\tiki-data-pipeline

# Run script
python scripts/<script_name>.py
```

### **Docker Issues**
```bash
# Clean and restart
.\scripts\docker-cleanup.ps1
docker-compose up -d --build
```

### **Database Connection Errors**
```bash
# Check connection
./scripts/diagnose_postgres_connection.sh

# Wait for database
./scripts/wait_for_postgres.sh
```

---

## 📚 **Related Documentation**

- **Tests:** `tests/README.md` - Test documentation
- **Docs:** `docs/INDEX.md` - Main documentation index
- **Guides:** `docs/07-GUIDES/` - User guides
- **Archive:** `scripts/archive/README.md` - Archived scripts

---

**Last Updated:** November 19, 2025  
**Maintained By:** GitHub Copilot AI Assistant
