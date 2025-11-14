#!/bin/bash
# Script để kiểm tra và giải phóng port 3002 trên Linux/Mac

echo "Đang kiểm tra port 3002..."

# Tìm process đang sử dụng port 3002
if command -v lsof > /dev/null; then
    PID=$(lsof -ti:3002)
    if [ -n "$PID" ]; then
        echo "Tìm thấy process đang sử dụng port 3002:"
        echo "  Process ID: $PID"
        ps -p $PID -o pid,comm,args
        
        read -p "Bạn có muốn kill process này không? (y/n) " answer
        if [ "$answer" = "y" ] || [ "$answer" = "Y" ]; then
            kill -9 $PID
            echo "Đã kill process $PID"
            sleep 2
            echo "Port 3002 đã được giải phóng"
        fi
    else
        echo "Port 3002 hiện tại không bị chiếm"
    fi
elif command -v netstat > /dev/null; then
    PID=$(netstat -tlnp 2>/dev/null | grep :3002 | awk '{print $7}' | cut -d'/' -f1 | head -1)
    if [ -n "$PID" ]; then
        echo "Tìm thấy process đang sử dụng port 3002:"
        echo "  Process ID: $PID"
        ps -p $PID -o pid,comm,args
        
        read -p "Bạn có muốn kill process này không? (y/n) " answer
        if [ "$answer" = "y" ] || [ "$answer" = "Y" ]; then
            kill -9 $PID
            echo "Đã kill process $PID"
            sleep 2
            echo "Port 3002 đã được giải phóng"
        fi
    else
        echo "Port 3002 hiện tại không bị chiếm"
    fi
else
    echo "Không tìm thấy lệnh lsof hoặc netstat. Vui lòng cài đặt một trong hai."
    exit 1
fi

# Kiểm tra lại
if command -v lsof > /dev/null; then
    PID=$(lsof -ti:3002)
elif command -v netstat > /dev/null; then
    PID=$(netstat -tlnp 2>/dev/null | grep :3002 | awk '{print $7}' | cut -d'/' -f1 | head -1)
fi

if [ -z "$PID" ]; then
    echo "Port 3002 đã sẵn sàng!"
else
    echo "Port 3002 vẫn còn bị chiếm"
fi

