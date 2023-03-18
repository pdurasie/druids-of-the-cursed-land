FROM python:3.12-rc-slim-buster AS builder

RUN apt-get update && \
    apt-get install -y libpq-dev gcc

RUN python -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

RUN pip install psycopg2

FROM python:3.12-rc-slim-buster

RUN apt-get update && \
    apt-get install -y libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app
ADD database_scripts database_scripts