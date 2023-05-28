import click

from cli.lending_wallets import lending_wallets
from cli.exchange_deposit_wallets import exchange_deposit_wallets
from cli.dex_wallets import dex_wallets
from cli.lp_owners import lp_owners


@click.group()
@click.version_option(version='1.0.0')
@click.pass_context
def cli(ctx):
    # Command line
    pass


cli.add_command(lending_wallets, "lending_wallets")
cli.add_command(exchange_deposit_wallets, "exchange_deposit_wallets")
cli.add_command(dex_wallets, "dex_wallets")
cli.add_command(lp_owners, "lp_owners")
