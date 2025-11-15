# Dockerfile custom cho Airflow với Chrome/Chromium để chạy Selenium
FROM apache/airflow:3.1.3

USER root

# Cài đặt Chrome và dependencies trong một layer duy nhất để giảm kích thước image
# Sử dụng --no-install-recommends và dọn dẹp ngay sau khi cài để tiết kiệm không gian
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    gnupg \
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
    libvulkan1 && \
    wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | gpg --batch --yes --no-tty --dearmor -o /usr/share/keyrings/google-chrome-keyring.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends google-chrome-stable && \
    # Dọn dẹp để giảm kích thước image
    apt-get remove -y wget gnupg && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /root/.cache /root/.wget-hsts && \
    # Dọn dẹp Chrome cache và temporary files
    rm -rf /opt/google/chrome/CrashReports /opt/google/chrome/swiftshader

# Chuyển về user airflow
USER airflow

# Các packages Python sẽ được cài qua _PIP_ADDITIONAL_REQUIREMENTS trong docker-compose.yaml
# Điều này cho phép cache tốt hơn và linh hoạt hơn

