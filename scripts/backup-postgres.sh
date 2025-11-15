#!/bin/bash
# Script backup PostgreSQL database
# Chạy script này để backup database ra thư mục backups/postgres

DATABASE="${1:-all}"  # "all", "airflow", "crawl_data"
FORMAT="${2:-custom}"  # "custom", "sql", "tar"

echo "🗄️  PostgreSQL Backup Script"
echo ""

# Kiểm tra container có đang chạy không
CONTAINER_NAME="tiki-data-pipeline-postgres-1"
if ! docker ps --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
    echo "❌ Container PostgreSQL không đang chạy!"
    echo "💡 Chạy: docker compose up -d postgres"
    exit 1
fi

echo "✅ Container PostgreSQL đang chạy: $CONTAINER_NAME"

# Lấy thông tin từ .env
if [ ! -f .env ]; then
    echo "❌ File .env không tồn tại!"
    exit 1
fi

POSTGRES_USER=$(grep "^POSTGRES_USER=" .env | cut -d'=' -f2)
POSTGRES_PASSWORD=$(grep "^POSTGRES_PASSWORD=" .env | cut -d'=' -f2)

if [ -z "$POSTGRES_USER" ]; then
    POSTGRES_USER="airflow_user"
fi

echo "📊 User: $POSTGRES_USER"
echo ""

# Tạo tên file backup
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_DIR="backups/postgres"

# Đảm bảo thư mục tồn tại
mkdir -p "$BACKUP_DIR"

# Backup function
backup_database() {
    local db_name=$1
    local backup_format=$2
    local backup_file=""
    
    echo "📦 Đang backup database: $db_name..."
    
    if [ "$backup_format" = "custom" ]; then
        backup_file="$BACKUP_DIR/${db_name}_${TIMESTAMP}.dump"
        docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$CONTAINER_NAME" \
            pg_dump -U "$POSTGRES_USER" -Fc "$db_name" > "$backup_file"
    elif [ "$backup_format" = "sql" ]; then
        backup_file="$BACKUP_DIR/${db_name}_${TIMESTAMP}.sql"
        docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$CONTAINER_NAME" \
            pg_dump -U "$POSTGRES_USER" -Fp "$db_name" > "$backup_file"
    elif [ "$backup_format" = "tar" ]; then
        backup_file="$BACKUP_DIR/${db_name}_${TIMESTAMP}.tar"
        docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$CONTAINER_NAME" \
            pg_dump -U "$POSTGRES_USER" -Ft "$db_name" > "$backup_file"
    fi
    
    if [ $? -eq 0 ]; then
        echo "✅ Đã backup: $backup_file"
        ls -lh "$backup_file" | awk '{print "   Size: " $5}'
    else
        echo "❌ Lỗi khi backup $db_name"
        return 1
    fi
}

# Thực hiện backup
if [ "$DATABASE" = "all" ]; then
    echo "🔄 Backup tất cả databases..."
    backup_database "airflow" "$FORMAT"
    backup_database "crawl_data" "$FORMAT"
else
    backup_database "$DATABASE" "$FORMAT"
fi

echo ""
echo "✅ Hoàn tất backup!"
echo "📁 Thư mục backup: $BACKUP_DIR"

# Hiển thị danh sách backup files
echo ""
echo "📋 Danh sách backup files:"
ls -lh "$BACKUP_DIR"/*_${TIMESTAMP}* 2>/dev/null | awk '{print "  - " $9 " (" $5 ")"}'

