# EdTech Pipeline — stream_consumer container
# Base: python:3.11-slim + OpenJDK 17 (required by PySpark)
FROM python:3.11-slim

# Install OpenJDK 17 (PySpark needs a JVM)
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Install Python dependencies first (layer-cached if requirements don't change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir \
        pyspark==3.5.1 \
        boto3 \
        redis \
        kafka-python \
        pandas \
        pyarrow \
        prometheus_client

# Copy source
COPY spark/ ./spark/

# Prometheus metrics on port 8000
EXPOSE 8000

# Run as non-root
RUN useradd -m -u 1000 edtech
USER edtech

CMD ["python", "-u", "spark/stream_consumer.py"]
