import json
import os
import time

import click

from constants.network_constants import Chains
from constants.time_constants import TimeConstants
from databases.mongodb import MongoDB
from jobs.dex_wallets.lp_traders_job import DexTradersCollectorJob
from utils.logger_utils import get_logger
from utils.time_utils import round_timestamp, human_readable_time

logger = get_logger('Exchange Trading Enricher')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
# @click.option('-c', '--chain', default='bsc', show_default=True, type=str, help='Network name example bsc or polygon')
@click.option('--interval', default=TimeConstants.A_HOUR, show_default=True, type=int, help='Interval to repeat execute')
def dex_wallets(interval):
    """Get exchange trading information."""
    chain_ids = ['0x38', '0x1', '0xfa']

    mongodb = MongoDB()

    job = DexTradersCollectorJob(chain_ids=chain_ids, db=mongodb)
    job.run()
