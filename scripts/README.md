# Scripts for CI/CD and Project Automation

This directory contains utility scripts used for managing and automating various aspects of the OpenFactory project.

## Contents

- `requirements_scripts.txt`: Lists Python dependencies required by the scripts in this directory.
- `bump_version.py`: Updates the version number in `pyproject.toml`. Intended for use in release automation pipelines or manual version bumps.
- `validate_version.sh`: Shell script to verify that a given Git tag matches the version specified in `pyproject.toml`. Intended to be used in CI workflows to ensure release tag consistency.
- *(Add more entries here as new scripts are added)*

## Usage Guidelines

- **Always run scripts from the project root** unless otherwise noted. Many scripts assume paths are relative to the root of the repository.
  
  ```bash
  python scripts/bump_version.py 0.4.0
  ```

* Scripts here are typically used in CI/CD environments, but can also be invoked manually when needed.

## Dependencies

* Required Python packages for scripts are listed in `scripts/requirements_scripts.txt`.
* Before running any Python scripts in this folder, install the dependencies with:

  ```bash
  python3 -m pip install -r scripts/requirements_scripts.txt
  ```

## Contributing

If you're adding a new automation or maintenance script, place it in this directory and:

- Document the script in this README with a brief description of its purpose.
- Add a top-level docstring to the script that includes:
  - What the script does
  - Expected arguments and usage
  - A dedicated **Dependency** section listing any required third-party libraries or external tools needed for the script to run
- Add any new required libraries to `requirements_scripts.txt`

## Notes

These scripts are intentionally kept lightweight and free of external dependencies where possible, to ease integration in constrained environments.
