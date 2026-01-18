# Dockerfile custom cho Airflow với Chrome để chạy Selenium
FROM apache/airflow:3.1.3

USER root

# Cài đặt timezone
ENV TZ=Asia/Ho_Chi_Minh
ENV AIRFLOW_HOME=/opt/airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"
ENV PYTHONPATH="/home/airflow/.local/lib/python3.12/site-packages:${PYTHONPATH}"

# Cài đặt Chrome (minimal)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget gnupg ca-certificates tzdata && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends google-chrome-stable && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
