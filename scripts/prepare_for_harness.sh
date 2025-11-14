#!/bin/bash
# Script để chuẩn bị môi trường trước khi chạy harness
# Kiểm tra và dừng container API nếu đang chạy

echo "Đang kiểm tra container API..."

CONTAINER_NAME="tiki-data-pipeline-api-1"

# Kiểm tra container có đang chạy không
CONTAINER_STATUS=$(docker ps -a --filter "name=$CONTAINER_NAME" --format "{{.Status}}" 2>/dev/null)

if [ -n "$CONTAINER_STATUS" ] && echo "$CONTAINER_STATUS" | grep -q "Up"; then
    echo "Tìm thấy container $CONTAINER_NAME đang chạy"
    echo "Đang dừng container..."
    docker stop $CONTAINER_NAME
    if [ $? -eq 0 ]; then
        echo "Đã dừng container $CONTAINER_NAME"
    else
        echo "Không thể dừng container $CONTAINER_NAME"
        exit 1
    fi
else
    echo "Không có container API đang chạy"
fi

# Kiểm tra port 3002
echo ""
echo "Đang kiểm tra port 3002..."
sleep 2  # Đợi một chút để port được giải phóng

if command -v lsof > /dev/null; then
    PID=$(lsof -ti:3002 2>/dev/null)
    if [ -n "$PID" ]; then
        echo "Port 3002 vẫn còn bị chiếm bởi process ID: $PID"
        ps -p $PID -o pid,comm,args 2>/dev/null
        echo "Vui lòng dừng process này hoặc đợi một chút rồi thử lại"
        exit 1
    fi
elif command -v netstat > /dev/null; then
    PID=$(netstat -tlnp 2>/dev/null | grep :3002 | grep LISTEN | awk '{print $7}' | cut -d'/' -f1 | head -1)
    if [ -n "$PID" ]; then
        echo "Port 3002 vẫn còn bị chiếm bởi process ID: $PID"
        ps -p $PID -o pid,comm,args 2>/dev/null
        echo "Vui lòng dừng process này hoặc đợi một chút rồi thử lại"
        exit 1
    fi
fi

echo "Port 3002 đã sẵn sàng!"
echo ""
echo "Môi trường đã sẵn sàng để chạy harness"

