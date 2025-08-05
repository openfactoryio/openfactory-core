#!/bin/bash
set -e

# -----------------------------------------------------------------------------
# validate_version.sh
#
# Checks if the given Git tag matches the version defined in pyproject.toml.
#
# Usage:
#   ./validate_version.sh <tag>
#
# Arguments:
#   <tag>   Git tag to validate (e.g. '1.2.3' or 'v1.2.3')
#
# Exits with 0 if tag matches the version, otherwise exits with 1.
# -----------------------------------------------------------------------------

if [ -z "$1" ]; then
  echo "Usage: $0 <tag>"
  exit 1
fi

TAG="$1"
# Strip leading 'v' if present
TAG="${TAG#v}"
PYPROJECT_VERSION=$(grep '^version' pyproject.toml | sed -E 's/version\s*=\s*"([^"]+)"/\1/')

echo "Tag: $TAG"
echo "pyproject.toml version: $PYPROJECT_VERSION"

if [ "$TAG" != "$PYPROJECT_VERSION" ]; then
  echo "ERROR: Tag '$TAG' does not match pyproject.toml version '$PYPROJECT_VERSION'"
  exit 1
fi

echo "Version match OK"
