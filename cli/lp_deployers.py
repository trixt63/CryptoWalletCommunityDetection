import click
from web3 import Web3
from web3.middleware import geth_poa_middleware

from constants.time_constants import TimeConstants
from constants.network_constants import Chains, Networks
from constants.blockchain_etl_constants import DBPrefix
from databases.mongodb import MongoDB
from databases.blockchain_etl import BlockchainETL
from jobs.dex_wallets.lp_deployers_job import LPDeployersJob
from utils.logger_utils import get_logger

logger = get_logger('LP Deployers CLI')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-c', '--chain', default='bsc', show_default=True, type=str, help='Network name example bsc or polygon')
@click.option('--interval', default=TimeConstants.A_DAY, show_default=True, type=int, help='Interval to repeat execute')
@click.option('-s', '--start-pair-id', default=0, show_default=True, type=int, help='Start pair to get deployer')
def lp_deployers(chain, interval, start_pair_id):
    """Get exchange trading information."""
    chain = str(chain).lower()
    if chain not in Chains.mapping:
        raise click.BadOptionUsage("--chain", f"Chain {chain} is not support")

    chain_id = Chains.mapping[chain]
    mongodb = MongoDB(wallet_col='lpDeployers')
    blockchain_etl = BlockchainETL(db_prefix=DBPrefix.mapping[chain])

    provider_uri = Networks.providers.get(chain)
    _web3 = Web3(Web3.HTTPProvider(provider_uri))

    job = LPDeployersJob(interval=interval,
                         chain_id=chain_id,
                         web3=_web3,
                         importer=mongodb,
                         exporter=mongodb,
                         transactions_db=blockchain_etl,
                         start_pair_id=start_pair_id
                         )
    job.run()
