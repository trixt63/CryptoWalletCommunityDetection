import click

from constants.time_constants import TimeConstants
from databases.mongodb import MongoDB
from databases.mongodb_entity import MongoDBEntity
from jobs.dex_wallets.lp_traders_job import DexTradersCollectorJob
from utils.logger_utils import get_logger

logger = get_logger('Exchange Trading Enricher')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
# @click.option('-c', '--chain', default='bsc', show_default=True, type=str, help='Network name example bsc or polygon')
@click.option('--interval', default=TimeConstants.A_DAY, show_default=True, type=int, help='Interval to repeat execute')
def dex_wallets(interval):
    """Get exchange trading information."""
    chain_ids = ['0xfa', '0x1', '0x38']

    mongodb = MongoDB()
    mongodb_entity = MongoDBEntity()

    job = DexTradersCollectorJob(chain_ids=chain_ids, db=mongodb, klg=mongodb_entity)
    job.run()
