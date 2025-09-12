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

# -------------------------------
# Stage 1: Build Stage
# -------------------------------
FROM python:3.13-slim AS build

LABEL maintainer="Rolf Wuthrich"
LABEL organisation="Concordia University"
LABEL description="Docker image for Kafka producer"
LABEL documentation="https://github.com/openfactoryio/openfactory-core/tree/main/connectors/OPCUA"

ARG VERSION=latest

# Install build-time dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /build

# Copy OpenFactory project
COPY . /build/

# Install Python dependencies into system site-packages
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# Strip unnecessary files to reduce layer size
RUN find /usr/local/lib/python3.13/site-packages -type d \( -name "tests" -o -name "test" -o -name "__pycache__" \) -exec rm -rf {} + \
    && find /usr/local/lib/python3.13/site-packages -name "*.pyc" -delete

# -------------------------------
# Stage 2: Runtime Stage
# -------------------------------
FROM python:3.13-slim

ARG UNAME=opcua_connector
ARG UID=1200
ARG GID=1200
ARG VERSION=latest

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV APPLICATION_VERSION=${VERSION}

# Create non-root user
RUN groupadd --gid "${GID}" "${UNAME}" && \
    useradd --create-home --uid "${UID}" --gid "${GID}" "${UNAME}"

# Set working directory
WORKDIR /home/${UNAME}

# Copy installed Python packages from build stage
COPY --from=build /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=build /usr/local/bin /usr/local/bin

# Copy producer script
COPY connectors/OPCUA/opcua_producer.py .

# Switch to non-root user
USER ${UNAME}

# Default entrypoint
CMD ["python3", "-u", "opcua_producer.py"]
