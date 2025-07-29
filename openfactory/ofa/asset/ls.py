""" ofa asset ls command. """

import click
from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text
from openfactory import OpenFactory
from openfactory.ofa.ksqldb import ksql


@click.command(name='ls')
def click_ls() -> None:
    """ List deployed OpenFactory assets. """
    ofa = OpenFactory(ksqlClient=ksql.client)

    console = Console()
    table = Table(
        title="Deployed Assets",
        title_justify="left",
        box=box.HORIZONTALS,
        show_lines=True)

    table.add_column("Asset UUID", style="cyan", no_wrap=True)
    table.add_column("Availability", justify="left")
    table.add_column("Type", justify="left")
    table.add_column("Docker container", justify="left")

    for asset in ofa.assets():

        # Availability of device
        if asset.type == 'MTConnectAgent':
            availability = asset.agent_avail.value.upper()
        else:
            availability = asset.avail.value.upper()
        if availability == "AVAILABLE":
            status = Text("AVAILABLE", style="bold green")
        elif availability == "UNAVAILABLE":
            status = Text("UNAVAILABLE", style="bold red")
        else:
            status = Text(availability, style="yellow")

        table.add_row(asset.asset_uuid,
                      status,
                      asset.type,
                      asset.DockerService.value)

    console.print(table)
