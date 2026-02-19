FROM python:3.11-slim

# Install Java (required by PySpark) — works on both amd64 and arm64
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# Dynamically set JAVA_HOME (works on amd64 & arm64 / Apple Silicon)
RUN JAVA_PATH=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "JAVA_HOME=${JAVA_PATH}" >> /etc/environment && \
    echo "export JAVA_HOME=${JAVA_PATH}" >> /etc/profile
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark \
    psycopg2-binary \
    pyyaml

WORKDIR /app

# Copy source code and JDBC driver
COPY src/   ./src/
COPY jars/  ./jars/

# Config and data are mounted at runtime via docker-compose volumes

ENTRYPOINT ["python", "src/main.py"]
CMD ["--config", "config/config.yaml"]
