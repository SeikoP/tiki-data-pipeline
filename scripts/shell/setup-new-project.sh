#!/bin/bash
# Script để setup dự án mới từ template

set -e

PROJECT_NAME=${1:-"my-data-pipeline"}

echo "🚀 Setting up new project: $PROJECT_NAME"

# 1. Copy .env.example to .env
if [ ! -f .env ]; then
    echo "📝 Creating .env from .env.example..."
    cp .env.example .env
    echo "✅ Created .env file. Please update it with your configuration."
else
    echo "⚠️  .env already exists, skipping..."
fi

# 2. Update docker-compose.yaml name
echo "📝 Updating docker-compose.yaml..."
sed -i.bak "s/name: tiki-data-pipeline/name: $PROJECT_NAME/" docker-compose.yaml
rm -f docker-compose.yaml.bak
echo "✅ Updated project name in docker-compose.yaml"

# 3. Create necessary directories
echo "📁 Creating directories..."
mkdir -p airflow/dags
mkdir -p airflow/logs
mkdir -p airflow/plugins
mkdir -p airflow/config
mkdir -p data/raw
mkdir -p data/processed
echo "✅ Created directories"

# 4. Set permissions (Linux/Mac)
if [[ "$OSTYPE" != "msys" && "$OSTYPE" != "win32" ]]; then
    echo "🔐 Setting permissions..."
    chmod +x scripts/*.sh 2>/dev/null || true
    echo "✅ Set permissions"
fi

echo ""
echo "✨ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Update .env with your configuration"
echo "2. Review and customize docker-compose.yaml if needed"
echo "3. Add your DAGs to airflow/dags/"
echo "4. Run: docker-compose up -d"
echo ""

