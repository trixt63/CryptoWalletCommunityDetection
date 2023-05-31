import click

from constants.time_constants import TimeConstants
from constants.network_constants import Chains
from constants.blockchain_etl_constants import DBPrefix
from databases.mongodb import MongoDB
from databases.blockchain_etl import BlockchainETL
from jobs.dex_wallets.lp_owners_job import LPOwnersJob
from utils.logger_utils import get_logger

logger = get_logger('Exchange Trading Enricher')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-c', '--chain', default='bsc', show_default=True, type=str, help='Network name example bsc or polygon')
@click.option('--interval', default=TimeConstants.A_DAY, show_default=True, type=int, help='Interval to repeat execute')
def lp_owners(chain, interval):
    """Get exchange trading information."""
    chain = str(chain).lower()
    if chain not in Chains.mapping:
        raise click.BadOptionUsage("--chain", f"Chain {chain} is not support")
    chain_id = Chains.mapping[chain]
    # chain_ids = ['0x38', '0xfa']
    mongodb = MongoDB()
    blockchain_etl = BlockchainETL(db_prefix=DBPrefix.mapping[chain])

    job = LPOwnersJob(scheduler=f'^true@{interval}#true',
                      chain_id=chain_id,
                      importer=mongodb,
                      exporter=mongodb,
                      transactions_db=blockchain_etl,
                      )
    job.run()
