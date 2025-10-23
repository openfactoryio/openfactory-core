""" ofa templates ls command. """

import click
from openfactory.ofa.templates.templates import TEMPLATE_MAP


@click.command(name='ls')
def ls() -> None:
    """ List available OpenFactory templates. """
    click.echo("Available template modules:")
    for name in TEMPLATE_MAP:
        click.echo(f"  - {name}")
