# ------------------------------------------------------------------------------
# Dockerfile for OpenFactory MTConnect Kafka Producer
#
# This image builds a Python-based Kafka producer that streams data from
# an MTConnect agent into a Kafka topic (`ofa_assets` by default).
#
# Base image:
#   - python:3.13-slim (lightweight Python runtime)
#
# Build-time arguments:
#   UNAME    - Linux username inside the container (default: producer)
#   UID      - User ID (default: 1200)
#   GID      - Group ID (default: 1200)
#
# Installed tools:
#   - git (for pip install from GitHub)
#   - kafka-python via: git+https://github.com/rwuthric/python-mtc2kafka.git
#
# Working directory:
#   - /home/${UNAME}
#
# Entrypoint:
#   - python3 -u kafka_producer.py
#
# To build the image locally:
#   docker build -t kafka-mtc-producer .
#
# To run it:
#   docker run --network factory-net \
#     -e KAFKA_BROKER=broker:29092 \
#     -e MTC_AGENT=http://192.168.1.10:5000 \
#     -e KAFKA_PRODUCER_UUID=device-001-producer \
#     kafka-mtc-producer
# ------------------------------------------------------------------------------

FROM python:3.13-slim

LABEL author="Rolf Wuthrich" 
LABEL organisation="Concordia University"
LABEL description="Docker image for Kafka producer"
LABEL documentation="https://github.com/openfactoryio/openfactory-dev/tree/main/connectors/MTConnect"

ARG UNAME=producer
ARG UID=1200
ARG GID=1200

# Create user and install dependencies
RUN groupadd --gid "${GID}" "${UNAME}" && \
    useradd --create-home --uid "${UID}" --gid "${GID}" "${UNAME}" && \
    apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# Install kafka-python library from GitHub
RUN pip install --no-cache-dir git+https://github.com/rwuthric/python-mtc2kafka.git

# Set working directory and switch user
WORKDIR /home/${UNAME}
USER ${UNAME}

# Copy the producer script into the container
COPY kafka_producer.py .

# Set default command
CMD ["python3", "-u", "kafka_producer.py"]
