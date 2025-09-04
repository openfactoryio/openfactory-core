# ------------------------------------------------------------------------------
# Dockerfile for OpenFactory OPC UA Producer
#
# This image builds a Python-based OPC UA producer that streams data from
# an OPC UA device into a Kafka topic (`ofa_assets` by default).
#
# Base image:
#   - python:3.13-slim (lightweight Python runtime)
#
# Build-time arguments:
#   UNAME    - Linux username inside the container (default: opcua_producer)
#   UID      - User ID (default: 1200)
#   GID      - Group ID (default: 1200)
#   VERSION  - Producer version (default: latest)
#
# Installed tools:
#   - git (for pip install from GitHub)
#
# Environment variables:
#   PYTHONDONTWRITEBYTECODE=1 - Prevents Python from writing .pyc files
#   PYTHONUNBUFFERED=1        - Ensures real-time output to logs
#   APPLICATION_VERSION       - Producer version passed as ARG
#
# Working directory:
#   - /home/${UNAME}
#
# Entrypoint:
#   - python3 -u opcua_producer.py
#
# To build the image locally:
#   docker build -t opcua-producer -f connectors/OPCUA/Dockerfile .
# ------------------------------------------------------------------------------

FROM python:3.13-slim

LABEL author="Rolf Wuthrich" 
LABEL organisation="Concordia University"
LABEL description="Docker image for Kafka producer"
LABEL documentation="https://github.com/openfactoryio/openfactory-core/tree/main/connectors/OPCUA"

ARG UNAME=opcua_connector
ARG UID=1200
ARG GID=1200
ARG VERSION=latest

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set environment variable from build argument
ENV APPLICATION_VERSION=${VERSION}

# Create user and install git
RUN groupadd --gid "${GID}" "${UNAME}" && \
    useradd --create-home --uid "${UID}" --gid "${GID}" "${UNAME}" && \
    apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy openfactory project
COPY . /app/

# Install the package using pyproject.toml
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

# Set working directory and switch user
WORKDIR /home/${UNAME}
USER ${UNAME}

# Copy the producer script into the container
COPY connectors/OPCUA/opcua_producer.py .

# Set default command
CMD ["python3", "-u", "opcua_producer.py"]
