import click

from cli.lending_wallets import lending_wallets
from cli.exchange_deposit_wallets import exchange_deposit_wallets
from cli.export_lp_contracts import export_lp_contracts
from cli.lp_traders import lp_traders
from cli.lp_deployers import lp_deployers


@click.group()
@click.version_option(version='1.0.0')
@click.pass_context
def cli(ctx):
    # Command line
    pass


cli.add_command(lending_wallets, "lending_wallets")
cli.add_command(exchange_deposit_wallets, "exchange_deposit_wallets")
cli.add_command(export_lp_contracts, "export_lp_contracts")
cli.add_command(lp_traders, "lp_traders")
cli.add_command(lp_deployers, "lp_deployers")
