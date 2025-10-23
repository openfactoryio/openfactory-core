""" ofa templates copy command. """

import click
import shutil
from importlib.resources import files
from pathlib import Path
from openfactory.ofa.templates.templates import TEMPLATE_MAP


@click.command(name='copy')
@click.argument("module")
@click.argument("destination", type=click.Path(file_okay=False, writable=True, path_type=Path))
def copy_templates(module, destination):
    """
    Copy template files for MODULE into DESTINATION.

    DESTINATION will be created automatically if it does not exist.
    """
    if module not in TEMPLATE_MAP:
        raise click.ClickException(f"Unknown module '{module}'. Try: ofa templates list")

    cfg = TEMPLATE_MAP[module]
    pkg = cfg["package"]
    files_to_copy = cfg["files"]

    # Create destination directory if needed
    if not destination.exists():
        destination.mkdir(parents=True, exist_ok=True)
        click.echo(f"📁 Created destination directory: {destination}")

    for filename in files_to_copy:
        src = files(pkg) / filename
        dst = destination / filename

        if dst.exists():
            click.echo(f"⚠️  Skipping {filename}: file already exists at {dst}")
            continue

        shutil.copy(src, dst)
        click.echo(f"📄 Copied {filename} → {dst}")

    click.echo(f"✅ {module.capitalize()} templates exported successfully.")
