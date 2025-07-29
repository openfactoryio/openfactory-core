""" ofa device ls command. """

import click
from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text
from openfactory import OpenFactory
from openfactory.ofa.ksqldb import ksql


@click.command(name='ls')
def click_ls() -> None:
    """ List deployed devices. """
    ofa = OpenFactory(ksqlClient=ksql.client)

    console = Console()
    table = Table(
        title="Deployed Devices",
        title_justify="left",
        box=box.HORIZONTALS,
        show_lines=True)

    table.add_column("Asset UUID", style="cyan", no_wrap=True)
    table.add_column("Availability", justify="left")
    table.add_column("MTConnect Agent", justify="left")
    table.add_column("Kafka Producer", justify="left")
    table.add_column("Supervisor", justify="left")

    for dev in ofa.devices():

        # Availability of device
        availability = dev.avail.value.upper()
        if availability == "AVAILABLE":
            status = Text("AVAILABLE", style="bold green")
        elif availability == "UNAVAILABLE":
            status = Text("UNAVAILABLE", style="bold red")
        else:
            status = Text(availability, style="yellow")

        # MTConnect Agent
        agent_id = f"{dev.asset_uuid}-AGENT"
        if agent_id in dev.references_below_uuid():
            agent_status = Text(agent_id, style="green")
        else:
            agent_status = Text("NOT DEPLOYED", style="bold red")

        # Kafka Producer
        prod_id = f"{dev.asset_uuid}-PRODUCER"
        if prod_id in dev.references_below_uuid():
            prod_status = Text(prod_id, style="green")
        else:
            prod_status = Text("NOT DEPLOYED", style="bold red")

        # Supervisor
        sup_id = f"{dev.asset_uuid}-SUPERVISOR"
        if sup_id in dev.references_below_uuid():
            sup_status = Text(sup_id, style="green")
        else:
            sup_status = Text("NOT DEPLOYED", style="bold red")

        table.add_row(dev.asset_uuid, status, agent_status, prod_status, sup_status)

    console.print(table)
