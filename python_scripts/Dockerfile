FROM python:3.11.2-slim-buster AS builder

RUN apt-get update && \
    apt-get install -y libpq-dev gcc

RUN python -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

FROM python:3.11.2-slim-buster

RUN apt-get update && \
    apt-get install -y libpq-dev libc-dev build-essential && \
    rm -rf /var/lib/apt/lists/*

RUN pip install -U pip

COPY --from=builder /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

# Copy the requirements.txt file into the container
COPY requirements.txt .

# Install the required Python packages using pip
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
ADD database_scripts database_scripts
ADD geohashes geohashes
ADD wkt wkt
ADD berlin_data berlin_data