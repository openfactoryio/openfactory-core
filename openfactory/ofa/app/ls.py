""" ofa app ls command. """

import click
from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text
from openfactory import OpenFactory
from openfactory.ofa.ksqldb import ksql


@click.command(name='ls')
def click_ls() -> None:
    """ List deployed OpenFactory apps. """
    ofa = OpenFactory(ksqlClient=ksql.client)

    console = Console()
    table = Table(
        title="Deployed Apps",
        title_justify="left",
        box=box.HORIZONTALS,
        show_lines=True)

    table.add_column("Asset UUID", style="cyan", no_wrap=True)
    table.add_column("Availability", justify="left")
    table.add_column("Vendor", justify="left")
    table.add_column("Version", justify="left")
    table.add_column("License", justify="left")

    for app in ofa.applications():

        # Availability of device
        availability = app.avail.value.upper()
        if availability == "AVAILABLE":
            status = Text("AVAILABLE", style="bold green")
        elif availability == "UNAVAILABLE":
            status = Text("UNAVAILABLE", style="bold red")
        else:
            status = Text(availability, style="yellow")

        table.add_row(app.asset_uuid,
                      status,
                      app.application_manufacturer.value,
                      app.application_version.value,
                      app.application_license.value)

    console.print(table)
