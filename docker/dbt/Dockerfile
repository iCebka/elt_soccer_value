FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/usr/app/dbt \
    DBT_TARGET_PATH=/tmp/dbt-target \
    DBT_LOG_PATH=/tmp/dbt-logs

WORKDIR /usr/app/dbt

RUN apt-get update \
    && apt-get install --no-install-recommends -y git \
    && rm -rf /var/lib/apt/lists/*

COPY docker/dbt/requirements.lock /tmp/dbt-requirements.lock
RUN python -m pip install --require-hashes -r /tmp/dbt-requirements.lock

COPY dbt/ /usr/app/dbt/

ENTRYPOINT ["dbt"]
CMD ["parse", "--no-version-check"]
