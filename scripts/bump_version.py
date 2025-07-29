"""
Bump the version number in pyproject.toml.

This script updates the `version` field under the [project] section of `pyproject.toml`.
It is typically used as part of a CI/CD pipeline or a release workflow to automate
semantic versioning.

Usage:
    python scripts/bump_version.py <new_version>

Example:
    python scripts/bump_version.py 0.3.1

Arguments:
    <new_version>  The new version string to set (e.g., 1.2.3)

Notes:
    - It will overwrite the `version` field in-place.
    - If `pyproject.toml` or the [project] section is missing, the script will exit with an error.

Dependency:
    - Requires `tomlkit` to be installed:
        pip install tomlkit
"""


from pathlib import Path
import sys
from tomlkit import parse, dumps

def bump_version(version: str) -> None:
    """
    Update the version string in pyproject.toml.

    Args:
        version (str): New version string, e.g., "0.3.1"
    """
    pyproject_path = Path(__file__).resolve().parent.parent / "pyproject.toml"

    if not pyproject_path.exists():
        print("ERROR: pyproject.toml not found.")
        sys.exit(1)

    with pyproject_path.open("r", encoding="utf-8") as f:
        toml_doc = parse(f.read())

    if "project" not in toml_doc or "version" not in toml_doc["project"]:
        print("ERROR: [project] section or version field not found.")
        sys.exit(1)

    old_version = toml_doc["project"]["version"]
    toml_doc["project"]["version"] = version

    with pyproject_path.open("w", encoding="utf-8") as f:
        f.write(dumps(toml_doc))

    print(f"Version updated: {old_version} → {version}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: bump_version.py <new_version>")
        sys.exit(1)

    bump_version(sys.argv[1])
