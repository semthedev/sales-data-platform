# syntax=docker/dockerfile:1.7
FROM eclipse-temurin:17-jre AS java

FROM python:3.12-slim

# Spark needs a JVM; copying the Temurin runtime does not depend on the Debian release's JDK.
COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}"

# Spark's launcher scripts call ps, which the slim image does not ship.
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

# JDBC drivers are fetched once at build time and verified by checksum, so pipeline runs
# do not resolve packages from Maven at start-up.
ADD --chmod=644 \
    --checksum=sha256:73914527305a40cce504b0d3d90b23caf565136912d607ae8ae7c5895512332c \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar \
    /opt/spark-jars/postgresql-42.7.14.jar
ADD --chmod=644 \
    --checksum=sha256:9dbe9fd70388b67429be7942f53b7984a4b71fd0f2f727f17339f20e27ecf564 \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.9.8/clickhouse-jdbc-0.9.8-all.jar \
    /opt/spark-jars/clickhouse-jdbc-0.9.8-all.jar
ENV SPARK_JARS=/opt/spark-jars/postgresql-42.7.14.jar,/opt/spark-jars/clickhouse-jdbc-0.9.8-all.jar

WORKDIR /app

COPY pyproject.toml README.md ./
COPY sales_pipeline ./sales_pipeline
COPY sql ./sql
COPY sample_data ./sample_data
RUN pip install --no-cache-dir ".[spark]"

ENTRYPOINT ["python", "-m", "sales_pipeline"]
CMD ["all"]
