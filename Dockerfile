FROM python:3.11-slim

# Java 17 required for PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY dbt/ dbt/

# Mount data/ at runtime: docker run -v $(pwd)/data:/app/data ...
VOLUME ["/app/data"]

CMD ["python", "src/pipeline.py"]
