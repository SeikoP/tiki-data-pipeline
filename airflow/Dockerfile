# Dockerfile custom cho Airflow với Chrome để chạy Selenium
FROM apache/airflow:3.1.6

USER root

# Cài đặt timezone
ENV TZ=Asia/Ho_Chi_Minh
ENV AIRFLOW_HOME=/opt/airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"
ENV PYTHONPATH="/opt/airflow:/opt/airflow/src:/opt/airflow/dags:/home/airflow/.local/lib/python3.12/site-packages"

# Cài đặt Chrome (minimal)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget gnupg ca-certificates tzdata postgresql-client && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends google-chrome-stable && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages at build time (faster startup, no runtime install needed)
RUN pip install --no-cache-dir \
    selenium>=4.0.0 \
    beautifulsoup4>=4.12.0 \
    requests>=2.31.0 \
    lxml>=4.9.0 \
    tqdm>=4.65.0 \
    webdriver-manager>=4.0.0 \
    psycopg2-binary>=2.9.0
