# Dockerfile custom cho Airflow với Chrome/Chromium để chạy Selenium
FROM apache/airflow:3.1.2

USER root

# Cài đặt Chrome/Chromium và dependencies
RUN apt-get update && \
    apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libwayland-client0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    libu2f-udev \
    libvulkan1 \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt Google Chrome (cách mới, không dùng apt-key deprecated)
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome-keyring.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

# Hoặc cài Chromium (nhẹ hơn)
# RUN apt-get update && \
#     apt-get install -y chromium chromium-driver && \
#     rm -rf /var/lib/apt/lists/*

# Chuyển về user airflow
USER airflow

# Các packages Python sẽ được cài qua _PIP_ADDITIONAL_REQUIREMENTS trong docker-compose.yaml

