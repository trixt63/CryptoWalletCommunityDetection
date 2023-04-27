import click

from constants.network_constants import Chains
from constants.time_constants import TimeConstants

from detector.lending_wallets_exporter import LendingWalletsExporter


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-c', '--chain', default='ethereum', show_default=True, type=str, help='Network name example bsc or polygon')
@click.option('-i', '--interval', default=TimeConstants.A_DAY, type=int, help='Sleep time')
def lending_wallets(chain, interval):
    chain_name = str(chain).lower()
    if chain_name not in Chains.mapping:
        raise click.BadOptionUsage("--chain", f"Chain {chain_name} is not support.\n"
                                              f"Supported chains: {list(Chains.names.values())}")
    chain_id = Chains.mapping[chain_name]
    job = LendingWalletsExporter(chain_id=chain_id)
    job.run()
