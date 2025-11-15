.PHONY: help lint format test validate-dags docker-build docker-up docker-down clean install perf-check dead-code complexity

help: ## Hiển thị help message
	@echo "Các lệnh có sẵn:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Cài đặt dependencies
	pip install --upgrade pip
	pip install -r requirements.txt
	pip install ruff black isort mypy pylint bandit safety pytest pytest-cov vulture radon

lint: ## Chạy linting với ruff
	ruff check src/ tests/ airflow/dags/

format: ## Format code với black và isort
	black src/ tests/ airflow/dags/
	isort src/ tests/ airflow/dags/

format-check: ## Kiểm tra format code (không sửa)
	black --check src/ tests/ airflow/dags/
	isort --check-only src/ tests/ airflow/dags/

type-check: ## Kiểm tra type với mypy
	mypy src/ --ignore-missing-imports

test: ## Chạy tests với pytest
	pytest tests/ -v --cov=src --cov-report=term --cov-report=html

test-fast: ## Chạy tests nhanh (không coverage)
	pytest tests/ -v

validate-dags: ## Validate Airflow DAGs
	@echo "Validating Airflow DAGs..."
	@python -c "from airflow.models import DagBag; dag_bag = DagBag(); print('✅ All DAGs validated successfully!' if not dag_bag.import_errors else f'❌ DAG Import Errors: {dag_bag.import_errors}'); print(f'Found {len(dag_bag.dags)} DAG(s)')" || echo "⚠️  Airflow not installed, skipping DAG validation"

security-check: ## Kiểm tra bảo mật với bandit và safety
	bandit -r src/
	safety check

perf-check: ## Kiểm tra performance với Ruff PERF rules
	ruff check --select PERF src/ tests/ airflow/dags/

dead-code: ## Tìm code thừa với Vulture
	vulture src/ airflow/dags/ --min-confidence 80

complexity: ## Phân tích độ phức tạp với Radon
	@echo "📊 Cyclomatic Complexity Analysis:"
	@radon cc src/ airflow/dags/ --min B || true
	@echo ""
	@echo "📈 Maintainability Index Analysis:"
	@radon mi src/ airflow/dags/ --min B || true

docker-cleanup: ## Dọn dẹp Docker cache và unused resources (giải quyết lỗi "no space left")
	@if [ -f scripts/docker-cleanup.sh ]; then \
		bash scripts/docker-cleanup.sh; \
	elif [ -f scripts/docker-cleanup.ps1 ]; then \
		powershell -ExecutionPolicy Bypass -File scripts/docker-cleanup.ps1; \
	else \
		echo "⚠️  Script cleanup không tìm thấy"; \
	fi

docker-build: ## Build Docker images
	docker-compose build

docker-build-clean: docker-cleanup docker-build ## Dọn dẹp và build Docker images

docker-up: ## Khởi động Docker Compose services
	docker-compose up -d

docker-down: ## Dừng Docker Compose services
	docker-compose down

docker-logs: ## Xem logs của Docker Compose services
	docker-compose logs -f

docker-test: ## Test Docker Compose setup
	docker-compose config
	docker-compose build
	docker-compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 30
	@curl -f http://localhost:8080/api/v2/version || echo "⚠️  Airflow API not ready"
	docker-compose down -v

clean: ## Dọn dẹp cache và temporary files
	find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null || true
	find . -type d -name "*.pyc" -exec rm -r {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -r {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -r {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -r {} + 2>/dev/null || true
	rm -rf htmlcov/ .coverage coverage.xml dist/ build/ *.egg-info

ci-local: ## Chạy tất cả các bước CI cục bộ
	@echo "🔍 Running local CI checks..."
	@make format-check
	@make lint
	@make perf-check
	@make type-check
	@make validate-dags
	@make security-check
	@make test
	@echo "✅ All CI checks passed!"

ci-fast: ## Chạy CI nhanh (không test)
	@make format-check
	@make lint
	@make perf-check
	@make validate-dags

code-quality: ## Chạy tất cả code quality checks (perf, dead code, complexity)
	@echo "🔍 Running code quality analysis..."
	@make perf-check
	@echo ""
	@echo "🧹 Finding dead code..."
	@make dead-code
	@echo ""
	@echo "📊 Analyzing complexity..."
	@make complexity

