import json
import logging
import os
import time

import click

from config import BlockchainETLConfig
from constants.blockchain_etl_constants import DBPrefix
from constants.network_constants import Chains
from constants.time_constants import TimeConstants
from databases.blockchain_etl import BlockchainETL
from databases.mongodb import MongoDB
from databases.postgresql import PostgresDB
from cli_scheduler.scheduler_job import SchedulerJob
from jobs.exchange_deposit_wallets_job import ExchangeDepositWalletsJob
from utils.file_utils import init_last_synced_file, read_last_synced_file, write_last_synced_file
from utils.logger_utils import get_logger
from utils.time_utils import round_timestamp, human_readable_time

logger = get_logger('Exchange Deposit wallet')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-l', '--last-synced-file', default='last_synced.txt', show_default=True, type=str, help='')
@click.option('-s', '--start-time', default=None, show_default=True, type=int, help='Start timestamp')
@click.option('-e', '--end-time', type=int, default=None, show_default=True, help='End timestamp')
@click.option('-p', '--period', type=int, default=TimeConstants.A_HOUR, show_default=True,
              help='Time period for each worker to process')
@click.option('-w', '--max-workers', default=8, show_default=True, type=int, help='The number of workers')
@click.option('-c', '--chain', default='bsc', show_default=True, type=str, help='Network name example bsc or polygon')
@click.option('--interval', default=TimeConstants.A_DAY, show_default=True, type=int,
              help='Interval to repeat the job')
@click.option('--delay', default=0, show_default=True, type=int, help='Time (in seconds) to delay')
@click.option('--run-now', default=True, show_default=True, type=bool,
              help='False to wait until interval then run')
@click.option('--source', default=None, show_default=True, type=str, multiple=True, help='Source to get data')
def exchange_deposit_wallets(last_synced_file, start_time, end_time, period, max_workers, chain, interval, delay, run_now, source):
    """Get exchange trading information."""
    chain = str(chain).lower()
    if chain not in Chains.mapping:
        raise click.BadOptionUsage("--chain", f"Chain {chain} is not support")
    chain_id = Chains.mapping[chain]
    db_prefix = DBPrefix.mapping[chain]
    sources = list(source)

    _blockchain_etl = BlockchainETL(BlockchainETLConfig.CONNECTION_URL, db_prefix=db_prefix)

    job = ExchangeWallets(
        blockchain_etl=_blockchain_etl, chain_id=chain_id,
        start_timestamp=start_time, end_timestamp=end_time, period=period, interval=interval, delay=delay,
        max_workers=max_workers, last_synced_file=last_synced_file, sources=sources, run_now=run_now
    )
    job.run()


class ExchangeWallets(SchedulerJob):
    """Just a continual job wrapper for the ExchangeDepositWalletsJob
    """
    def __init__(
            self, blockchain_etl, chain_id,
            start_timestamp, end_timestamp, period, interval, delay, run_now,
            max_workers, last_synced_file, sources
    ):

        self.start_timestamp = start_timestamp
        # self.end_timestamp = end_timestamp
        # self.interval = interval
        scheduler = f"^{run_now}@{interval}/{delay}${end_timestamp}#false"
        super().__init__(scheduler)

        self._blockchain_etl = blockchain_etl
        self.period = period

        self.max_workers = max_workers

        self.chain_id = chain_id

        self.last_synced_file = last_synced_file
        self.sources = sources
        if not self.sources:
            self.sources = ['mongo', 'postgres']

    def _pre_start(self):
        self._db = PostgresDB()
        self.mongodb = MongoDB()

        if (self.start_timestamp is not None) or (not os.path.isfile(self.last_synced_file)):
            _DEFAULT_START_TIME = int(time.time() - TimeConstants.DAYS_30)
            init_last_synced_file(self.start_timestamp or _DEFAULT_START_TIME, self.last_synced_file)
        self.start_timestamp = read_last_synced_file(self.last_synced_file)

        self.exchange_wallets = self.get_exchange_wallets()

    def _start(self):
        # self.end_time = self.start_timestamp + self.interval
        self.next_synced_timestamp = round_timestamp(self.start_timestamp + self.interval, self.interval) + self.delay
        logger.info(f'Start execute from {human_readable_time(self.start_timestamp)} to '
                    f'{human_readable_time(self.next_synced_timestamp)}')

    def _execute(self, *args, **kwargs):
        job = ExchangeDepositWalletsJob(
            # databases & data
            transfer_event_db=self._db,
            blockchain_etl=self._blockchain_etl,
            exporter=self.mongodb,
            exchange_wallets=self.exchange_wallets,
            chain_id=self.chain_id,
            sources=self.sources,
            # time frame
            start_timestamp=self.start_timestamp,
            # end_timestamp=self.end_time,
            end_timestamp=self.next_synced_timestamp,
            # multi-workers
            period=self.period,
            batch_size=1,
            max_workers=self.max_workers
        )
        job.run()

    def _end(self):
        self.start_timestamp = self.next_synced_timestamp
        write_last_synced_file(self.last_synced_file, self.start_timestamp)
        time.sleep(3)

    def get_exchange_wallets(self):
        with open('artifacts/centralized_exchange_addresses.json') as f:
            centralized_exchanges = json.load(f)

        exchange_wallets = {}
        for exchange_id, info in centralized_exchanges.items():
            wallets = info.get('wallets', {})
            exchange_wallets.update({w.lower(): exchange_id for w in wallets.get(self.chain_id, [])})
        return exchange_wallets
