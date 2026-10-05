FROM kestra/kestra:v1.3.37

USER root

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y \
    && apt-get install -y --no-install-recommends \
        python3-venv \
        python3-pip \
        git \
        openjdk-17-jre-headless \
        procps \
        ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Java
#
# Kestra keeps using the Java runtime provided by its base image.
# Java 17 is installed separately only for Spark.
# ---------------------------------------------------------------------------

RUN ln -sfn \
    "/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)" \
    /opt/java17

ENV SPARK_JAVA_HOME="/opt/java17"

# ---------------------------------------------------------------------------
# dbt
# ---------------------------------------------------------------------------

RUN /usr/bin/python3 -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir \
        dbt-snowflake==1.12.1 \
    && /opt/dbt-venv/bin/dbt --version \
    && git --version

# ---------------------------------------------------------------------------
# Spark / PySpark
# ---------------------------------------------------------------------------

RUN /usr/bin/python3 -m venv /opt/spark-venv \
    && /opt/spark-venv/bin/pip install --no-cache-dir \
        pyspark==3.5.9

ENV PYSPARK_PYTHON="/opt/spark-venv/bin/python"
ENV PYSPARK_DRIVER_PYTHON="/opt/spark-venv/bin/python"

# Do NOT override JAVA_HOME here.
# Kestra must continue using the Java runtime from its base image.
ENV PATH="/opt/spark-venv/bin:/opt/dbt-venv/bin:${PATH}"

# Validate dbt and Spark independently.
RUN dbt --version \
    && git --version \
    && JAVA_HOME=/opt/java17 /opt/spark-venv/bin/spark-submit --version