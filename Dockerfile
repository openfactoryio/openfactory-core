# ------------------------------------------------------------------------------
# Dockerfile for OpenFactory Asset Management Tool
#
# This image provides the OpenFactory management CLI and supporting tooling.
# It is designed to run as a long-lived utility container into which users
# can exec management commands as needed.
#
# Base image:
#   - python:3.13-slim (lightweight Python runtime)
#
# Build-time arguments:
#   UNAME                     - Linux username inside the container (default: ofa)
#   UID                       - User ID (default: 1200)
#   GID                       - Group ID (default: 1200)
#   VERSION                   - Application version (default: dev)
#   APPLICATION_MANUFACTURER  - Application manufacturer string
#   OPENFACTORY_VERSION       - OpenFactory core version or branch (default: main)
#
# Installed tools:
#   - git
#   - ofa: OpenFactory management CLI (installed in ~/.local/bin)
#
# Environment variables:
#   PYTHONDONTWRITEBYTECODE=1 - Prevents Python from writing .pyc files
#   PYTHONUNBUFFERED=1        - Ensures real-time output to logs
#   APPLICATION_VERSION       - Application version passed as ARG
#   OPENFACTORY_VERSION       - OpenFactory core version passed as ARG
#   PATH                      - Extended to include ~/.local/bin for user installs
#
# Runtime configuration:
#   The management tool (ofa) requires additional environment variables to be
#   provided at runtime. Configuration can be supplied using one or a comination of the
#   following mechanisms:
#
#   1) Environment variables via Docker:
#
#        --env-file .env
#        -e KAFKA_BROKER=...
#        -e ASSET_ROUTER_URL=...
#
#      These are injected at the Docker level and affect the container's
#      environment directly.
#
#   2) Application-level configuration via a mounted file:
#
#        -v $(pwd)/.ofaenv:/home/${UNAME}/.ofaenv:ro
#
#      The ofa CLI tool reads this file automatically at startup. This allows
#      persistent, file-based configuration separate from Docker runtime variables.
#
# Persistent data and configuration:
#   Management operations typically rely on local configuration files
#   describing assets deployed on the platform.
#
#   A host directory should be mounted into the container to provide access
#   to these files, for example:
#
#     -v $(pwd)/local:/home/${UNAME}/local:ro
#
# Docker access:
#   This image does NOT include the Docker CLI or daemon.
#   Docker access is provided at runtime by mounting the host Docker socket and
#   adding the corresponding group ID:
#
#     DOCKER_GID=$(stat -c '%g' /var/run/docker.sock)
#     docker run -d \
#       --name manager \
#       -v /var/run/docker.sock:/var/run/docker.sock \
#       --group-add ${DOCKER_GID} \
#       openfactoryio/ofa-cli:latest
#
#   Docker permissions are evaluated using numeric group IDs; the container
#   does not define or assume a 'docker' group.
#
# Working directory:
#   - /home/${UNAME}
#
# Runtime behavior:
#   - The container runs as a non-root user
#   - The default command keeps the container alive indefinitely
#   - Management commands are executed via `docker exec`
#
# Example deployment:
#   docker run -d \
#     --name manager \
#     -v /var/run/docker.sock:/var/run/docker.sock \
#     --group-add $(stat -c '%g' /var/run/docker.sock) \
#     --env-file .env \
#     -v $(pwd)/local:/home/${UNAME}/local:ro \
#     openfactoryio/ofa-cli:latest
#
# Example usage:
#   docker exec -it manager ofa config ls
#
# To build the image locally:
#   docker build -t openfactoryio/ofa-cli:latest .
# ------------------------------------------------------------------------------

FROM python:3.13-slim AS build

LABEL maintainer="Rolf Wuthrich"
LABEL organisation="Concordia University"
LABEL description="Docker image for OpenFactory management tools"
LABEL documentation="https://github.com/openfactoryio/openfactory-core/"

ARG VERSION=dev
ARG APPLICATION_MANUFACTURER=OpenFactoryIO
ARG OPENFACTORY_VERSION=main

ARG UNAME=ofa
ARG UID=1200
ARG GID=1200

# Create user and group
RUN groupadd --gid $GID $UNAME && \
    useradd --create-home --uid $UID --gid $GID $UNAME

# Set environment variable from build argument
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APPLICATION_VERSION=${VERSION} \
    OPENFACTORY_VERSION=${OPENFACTORY_VERSION}

# Install build-time dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends git

# Set working directory
WORKDIR /home/$UNAME
USER $UNAME

COPY . .

# Install Python dependencies into system site-packages
ENV PATH="/home/$UNAME/.local/bin:${PATH}"
RUN pip install --no-cache-dir --upgrade pip && \
    pip install .

CMD ["sleep", "infinity"]
