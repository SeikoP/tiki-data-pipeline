#!/bin/bash

# Initialize NUQ database schema

set -e

echo "Initializing NUQ database schema..."

# Copy the SQL file into the container and execute it
docker-compose exec -T postgres psql -U postgres -d nuq -f /dev/stdin < firecrawl/apps/nuq-postgres/nuq.sql

echo "NUQ database schema initialized successfully!"

