import click

from jobs.dex_wallets.export_lp_contracts_job import ExportLPContractsJob
from constants.lp_constants import lp_factory_mapping
from constants.time_constants import TimeConstants


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-i', '--interval', type=int, default=TimeConstants.A_DAY, help="Time between each job run")
@click.option('-d', '--delay', type=int, default=0, help="Time between each job run")
@click.option('-fa', '--factory-address', type=str, help="Factory contract address")
@click.option('-s', '--start-pair-id', default=0, show_default=True, type=int, help='Start Pair ID')
@click.option('-e', '--end-pair-id', type=int, help='End Pair ID. Default the last pair ID')
def export_lp_contracts(interval, delay, factory_address, start_pair_id, end_pair_id):
    factory_address = factory_address.lower()
    lp_factory_details = lp_factory_mapping[factory_address]
    chain_id = lp_factory_details['chain_id']
    factory_abi = lp_factory_details['factory_abi']
    lp_abi = lp_factory_details['lp_abi']

    job = ExportLPContractsJob(
        interval=interval,
        delay=delay,
        chain_id=chain_id,
        factory_address=factory_address,
        factory_abi=factory_abi,
        lp_abi=lp_abi,
        start_pair_id=start_pair_id,
        end_pair_id=end_pair_id
    )

    job.run()
