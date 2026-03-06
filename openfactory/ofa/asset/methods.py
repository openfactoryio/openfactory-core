""" ofa asset methods command. """

import click
from rich import box
from rich.console import Console
from rich.table import Table
from openfactory.assets import Asset
from openfactory.ofa.ksqldb import ksql
import openfactory.config as config


@click.command(name='methods')
@click.argument('asset_uuid', type=click.STRING)
def click_methods(asset_uuid: str) -> None:
    """ List all attributes from an asset. """

    asset = Asset(asset_uuid,
                  ksqlClient=ksql.client,
                  bootstrap_servers=config.KAFKA_BROKER)

    methods = asset.methods()

    console = Console()
    table = Table(
        title=f"{asset_uuid} - {asset.uns_id.value}",
        title_justify="left",
        box=box.HORIZONTALS,
        show_lines=True)

    table.add_column("Method Name", style="cyan", no_wrap=True)
    table.add_column("Description", style="")
    table.add_column("Arguments", style="")

    for method_name, info in methods.items():
        description = info.get("description", "")
        args = info.get("arguments", [])

        if args:
            args_str = "\n".join(f"{arg['name']}: {arg['description']}" for arg in args)
        else:
            args_str = "—"

        table.add_row(method_name, description, args_str)

    asset.close()
    console.print(table)
