# Dockerfile custom cho Airflow với Chrome/Chromium để chạy Selenium
FROM apache/airflow:3.1.5

USER root

# Cài đặt timezone và cấu hình giờ Việt Nam (UTC+7)
ENV TZ=Asia/Ho_Chi_Minh

# Cài đặt Chromium và dependencies (nhẹ hơn Chrome, dễ cài hơn)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    tzdata \
    chromium \
    chromium-driver \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Chuyển về user airflow
USER airflow

# Các packages Python sẽ được cài qua _PIP_ADDITIONAL_REQUIREMENTS trong docker-compose.yaml
# Điều này cho phép cache tốt hơn và linh hoạt hơn

