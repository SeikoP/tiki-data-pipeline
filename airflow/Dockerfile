# Dockerfile (LITE MODE) - Airflow Mới Nhất (3.x), Không Chrome/Selenium
FROM apache/airflow:3.3.0

USER root

# Cài đặt timezone và các công cụ network cơ bản
ENV TZ=Asia/Ho_Chi_Minh
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget gnupg ca-certificates tzdata postgresql-client curl redis-tools && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements và cài đặt python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
