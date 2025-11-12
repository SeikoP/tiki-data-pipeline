#!/bin/bash
# Script to manually sync files to another repository
# Usage: ./scripts/utils/sync_to_other_repo.sh [target-repo-path]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SOURCE_REPO="$(pwd)"
TARGET_REPO="${1:-../airflow-firecrawl-data-pipeline}"
TARGET_REPO_URL="https://github.com/SeikoP/airflow-firecrawl-data-pipeline.git"

# Files and directories to sync
FILES_TO_SYNC=(
    "docker-compose.yaml"
)

DIRS_TO_SYNC=(
    "scripts"
)

echo -e "${GREEN}=== Sync to Other Repository ===${NC}"
echo ""
echo "Source: $SOURCE_REPO"
echo "Target: $TARGET_REPO"
echo ""

# Check if target repo exists
if [ ! -d "$TARGET_REPO" ]; then
    echo -e "${YELLOW}Target repository not found. Cloning...${NC}"
    git clone "$TARGET_REPO_URL" "$TARGET_REPO"
else
    echo -e "${GREEN}Target repository found.${NC}"
fi

# Navigate to target repo
cd "$TARGET_REPO"

# Update target repo
echo -e "${YELLOW}Updating target repository...${NC}"
git fetch origin
git checkout main || git checkout master
git pull origin main || git pull origin master

# Sync files
echo -e "${YELLOW}Syncing files...${NC}"

# Sync docker-compose.yaml
if [ -f "$SOURCE_REPO/docker-compose.yaml" ]; then
    echo "  - Syncing docker-compose.yaml..."
    cp "$SOURCE_REPO/docker-compose.yaml" "$TARGET_REPO/docker-compose.yaml"
    echo -e "  ${GREEN}✓${NC} docker-compose.yaml synced"
else
    echo -e "  ${RED}⚠${NC} docker-compose.yaml not found"
fi

# Sync scripts directory
if [ -d "$SOURCE_REPO/scripts" ]; then
    echo "  - Syncing scripts directory..."
    rm -rf "$TARGET_REPO/scripts"
    cp -r "$SOURCE_REPO/scripts" "$TARGET_REPO/scripts"
    echo -e "  ${GREEN}✓${NC} scripts directory synced"
else
    echo -e "  ${RED}⚠${NC} scripts directory not found"
fi

# Update .gitignore
if [ ! -f "$TARGET_REPO/.gitignore" ]; then
    touch "$TARGET_REPO/.gitignore"
fi

# Add Python cache patterns to .gitignore if not exists
if ! grep -q "scripts/__pycache__" "$TARGET_REPO/.gitignore"; then
    echo "" >> "$TARGET_REPO/.gitignore"
    echo "# Python cache in scripts" >> "$TARGET_REPO/.gitignore"
    echo "scripts/__pycache__/" >> "$TARGET_REPO/.gitignore"
    echo "scripts/**/__pycache__/" >> "$TARGET_REPO/.gitignore"
    echo "*.pyc" >> "$TARGET_REPO/.gitignore"
fi

# Check for changes
echo ""
echo -e "${YELLOW}Checking for changes...${NC}"
git add -A

if git diff --staged --quiet; then
    echo -e "${YELLOW}No changes to commit.${NC}"
    exit 0
fi

# Show changes
echo -e "${GREEN}Changes detected:${NC}"
git status --short

# Ask for confirmation
read -p "Do you want to commit and push these changes? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Aborted. Changes are staged but not committed.${NC}"
    exit 1
fi

# Commit and push
echo -e "${YELLOW}Committing changes...${NC}"
COMMIT_MSG="chore: Sync docker-compose.yaml and scripts from source repo

Synced from: $(git -C "$SOURCE_REPO" remote get-url origin 2>/dev/null || echo "local")
Source commit: $(git -C "$SOURCE_REPO" rev-parse HEAD 2>/dev/null || echo "unknown")
Synced at: $(date)"

git commit -m "$COMMIT_MSG"

echo -e "${YELLOW}Pushing changes...${NC}"
git push origin main || git push origin master

echo ""
echo -e "${GREEN}✅ Sync completed successfully!${NC}"
echo ""

