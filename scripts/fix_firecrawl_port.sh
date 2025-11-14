#!/bin/bash
# Script để fix Firecrawl port 3002 conflict

echo "🔍 Checking port 3002 usage..."
echo ""

# Kiểm tra port 3002
if command -v lsof &> /dev/null; then
    echo "📊 Processes using port 3002:"
    lsof -i :3002 || echo "   No processes found (or lsof not available)"
elif command -v netstat &> /dev/null; then
    echo "📊 Processes using port 3002:"
    netstat -ano | grep :3002 | grep LISTENING || echo "   No processes found"
else
    echo "⚠️  Cannot check port (lsof/netstat not available)"
fi

echo ""
echo "🐳 Checking Docker containers:"
docker ps -a --filter "publish=3002" --format "table {{.ID}}\t{{.Names}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "💡 Solutions:"
echo ""
echo "1. Stop and restart containers:"
echo "   docker-compose down"
echo "   docker-compose --profile firecrawl up -d"
echo ""
echo "2. If port is still in use, kill the process:"
echo "   # Linux/Mac:"
echo "   lsof -ti:3002 | xargs kill -9"
echo "   # Or find PID and kill manually"
echo ""
echo "3. Change port in .env file:"
echo "   PORT=3003"
echo "   Then restart: docker-compose --profile firecrawl up -d"

