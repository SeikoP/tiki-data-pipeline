#!/bin/bash

# Quick health check for Tiki Data Pipeline

echo "🔍 Tiki Data Pipeline - Quick Health Check"
echo "==========================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

check_count=0
pass_count=0
fail_count=0

check() {
    check_count=$((check_count + 1))
    echo -n "[$check_count] $1 ... "
}

pass() {
    pass_count=$((pass_count + 1))
    echo -e "${GREEN}✓ PASS${NC}"
}

fail() {
    fail_count=$((fail_count + 1))
    echo -e "${RED}✗ FAIL${NC}: $1"
}

warn() {
    echo -e "${YELLOW}⚠ WARN${NC}: $1"
}

# 1. Docker
check "Docker installed"
if command -v docker &> /dev/null; then
    pass
else
    fail "Docker not found"
    exit 1
fi

# 2. Docker Compose
check "Docker Compose"
if docker-compose --version &> /dev/null; then
    pass
else
    fail "Docker Compose not found"
    exit 1
fi

# 3. Services running
echo ""
check "PostgreSQL running"
if docker-compose exec -T postgres pg_isready -U postgres &> /dev/null; then
    pass
else
    fail "PostgreSQL not responding"
fi

check "Redis running"
if docker-compose exec -T redis redis-cli ping | grep -q PONG; then
    pass
else
    fail "Redis not responding"
fi

# 4. Databases exist
echo ""
check "Airflow database"
if docker-compose exec -T postgres psql -U postgres -lqt | grep -q "| airflow |"; then
    pass
else
    warn "Airflow database not found"
fi

check "NUQ database"
if docker-compose exec -T postgres psql -U postgres -lqt | grep -q "| nuq |"; then
    pass
else
    warn "NUQ database not found"
fi

# 5. Airflow tables
echo ""
check "Airflow tables"
table_count=$(docker-compose exec -T postgres psql -U airflow -d airflow -c '\dt' 2>/dev/null | wc -l)
if [ "$table_count" -gt 5 ]; then
    pass
else
    fail "No Airflow tables - Need to run: docker-compose run --rm airflow-init"
fi

# 6. API Services
echo ""
check "Airflow API"
if curl -s http://localhost:8080/api/v2/version &> /dev/null; then
    pass
else
    warn "Airflow API not responding (may be starting)"
fi

check "Firecrawl API"
if curl -s http://localhost:3002/health &> /dev/null; then
    pass
else
    warn "Firecrawl API not responding"
fi

# Summary
echo ""
echo "==========================================="
echo "Results: ${GREEN}✓ $pass_count${NC} | ${RED}✗ $fail_count${NC} | ${YELLOW}⚠ checks${NC}"
echo ""

if [ $fail_count -gt 0 ]; then
    echo -e "${RED}❌ Issues detected!${NC}"
    echo ""
    echo "Fix options:"
    echo "  1. python scripts/verify_services.py  (auto-fix)"
    echo "  2. docker-compose restart"
    echo "  3. See TROUBLESHOOTING.md for detailed help"
    exit 1
else
    echo -e "${GREEN}✅ All services healthy!${NC}"
    exit 0
fi

