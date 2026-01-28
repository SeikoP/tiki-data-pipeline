# Tiki Data Pipeline

A robust, modular, and optimized data pipeline for crawling, transforming, and loading Tiki product data.

## 🚀 Overview

This project is an automated ETL pipeline built on **Apache Airflow** that crawls product information from Tiki.vn, processes the data (including AI-powered product name shortening), and stores it in a PostgreSQL database for analysis.

## 🏗️ Architecture (v2)

The project has been refactored into a modular structure for better maintainability and scalability:

- **`airflow/dags/tiki_crawl_products_v2/`**: The core DAG logic.
  - `main_dag.py`: Orchestrates the entire pipeline using TaskGroups and Dynamic Task Mapping.
  - `bootstrap.py`: Handles environment setup, dynamic imports, and component initialization.
  - `tasks/`: Individual Python modules for each stage (crawl, transform, load, maintenance).
- **`src/`**: Shared pipeline logic and utilities.
  - `pipelines/crawl/`: Selenium-based crawlers and storage drivers.
  - `pipelines/transform/`: Data cleaning and enrichment logic.
  - `pipelines/load/`: High-performance database loaders with connection pooling.
- **`scripts/`**: Utility scripts for maintenance and manual debugging.

## ✨ Key Features

- **Dynamic Task Mapping**: Scales crawling tasks dynamically based on the number of categories.
- **High-Performance Loader**: Uses PostgreSQL `COPY` and staging tables for bulk inserts, achieving high throughput.
- **Crawl History Tracking**: Automatically logs price and sales changes over time in the `crawl_history` table.
- **AI Integration**: Uses Groq LLMs to generate concise `short_name` attributes for products.
- **Robust Error Handling**: Connection pooling with health checks and exponential backoff for database operations.
- **Maintenance Tasks**: Automated cleanup of orphaned categories and redundant data.

## 🛠️ Tech Stack

- **Orchestration**: Apache Airflow
- **Language**: Python 3.12+
- **Scraping**: Selenium, requests
- **Database**: PostgreSQL (with `psycopg2` connection pooling)
- **Formatting/Linting**: Ruff, isort
- **AI/LLM**: Groq (Llama 3)

## 🚦 Getting Started

### Prerequisites
- Docker & Docker Compose (for Airflow)
- Python 3.12+
- PostgreSQL

### Installation
1. Clone the repository.
2. Build and start the Airflow environment:
   ```bash
   docker-compose up -d
   ```
3. Copy `.env.example` to `.env` and configure your database and API keys.

### Running the Pipeline
The DAG `tiki_crawl_products_v2` can be triggered manually from the Airflow UI or scheduled to run periodically.

## 📊 Database Schema

The pipeline populates the following main tables:
- `products`: Core product details (price, brand, seller info, etc.).
- `categories`: Normalized category hierarchy and leaf nodes.
- `crawl_history`: Historical snapshots of price and sales counts.

---
*Developed by the Tiki Data Pipeline Team.*
