import click

from cli.lending_wallets import lending_wallets


@click.group()
@click.version_option(version='1.0.0')
@click.pass_context
def cli(ctx):
    # Command line
    pass


cli.add_command(lending_wallets, "lending_wallets")
