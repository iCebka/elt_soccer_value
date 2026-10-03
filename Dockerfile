FROM kestra/kestra:v2.0.4 AS base

USER root

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN /app/kestra plugins install io.kestra.plugin:plugin-script-python:1.13.0

COPY requirements.txt /opt/transfermarkt/requirements.txt
RUN python3 -m venv /opt/transfermarkt/.venv \
    && /opt/transfermarkt/.venv/bin/pip install --no-cache-dir -r /opt/transfermarkt/requirements.txt

COPY config /opt/transfermarkt/config
COPY src /opt/transfermarkt/src

ENV PATH="/opt/transfermarkt/.venv/bin:${PATH}" \
    PYTHONPATH="/opt/transfermarkt/src" \
    PYTHONUNBUFFERED="1"

WORKDIR /opt/transfermarkt

FROM base AS test
COPY requirements-dev.txt /opt/transfermarkt/requirements-dev.txt
RUN /opt/transfermarkt/.venv/bin/pip install --no-cache-dir -r /opt/transfermarkt/requirements-dev.txt
COPY pytest.ini /opt/transfermarkt/pytest.ini
COPY tests /opt/transfermarkt/tests
ENTRYPOINT ["/opt/transfermarkt/.venv/bin/python"]
CMD ["-m", "pytest"]

FROM base AS runtime

