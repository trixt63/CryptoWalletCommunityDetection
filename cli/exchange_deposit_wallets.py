import json
import logging
import os
import time

import click

from config import BlockchainETLConfig
from constants.blockchain_etl_constants import DBPrefix
from constants.network_constants import Chains
from constants.time_constants import TimeConstants
from databases.arangodb_klg import ArangoDB
from databases.blockchain_etl import BlockchainETL
from databases.postgresql import PostgresDB
from jobs.cli_job import CLIJob
from jobs.exchange_deposit_wallets_job import ExchangeDepositWalletsJob
from utils.file_utils import init_last_synced_file, read_last_synced_file, write_last_synced_file
from utils.logger_utils import get_logger
from utils.time_utils import round_timestamp, human_readable_time

logger = get_logger('Exchange Trading Enricher')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-l', '--last-synced-file', default='last_synced.txt', show_default=True, type=str, help='')
@click.option('-s', '--start-time', default=None, show_default=True, type=int, help='Start timestamp')
@click.option('-e', '--end-time', type=int, default=None, show_default=True, help='End timestamp')
@click.option('-p', '--period', type=int, default=TimeConstants.A_HOUR, show_default=True, help='Time period')
@click.option('-w', '--max-workers', default=8, show_default=True, type=int, help='The number of workers')
@click.option('-c', '--chain', default='bsc', show_default=True, type=str, help='Network name example bsc or polygon')
@click.option('--interval', default=TimeConstants.A_DAY, show_default=True, type=int, help='Interval to repeat execute')
@click.option('--source', default=None, show_default=True, type=str, multiple=True, help='Source to get data')
def exchange_deposit_wallets(last_synced_file, start_time, end_time, period, max_workers, chain, interval, source):
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
        start_timestamp=start_time, end_timestamp=end_time, period=period, interval=interval,
        max_workers=max_workers, last_synced_file=last_synced_file, sources=sources
    )
    # job.export_exchange_address()
    job.run()


class ExchangeWallets(CLIJob):
    def __init__(
            self, blockchain_etl, start_timestamp, end_timestamp, period, interval, max_workers,
            chain_id, last_synced_file, sources
    ):
        self._blockchain_etl = blockchain_etl

        self.start_timestamp = start_timestamp
        self.period = period

        self.max_workers = max_workers

        self.chain_id = chain_id

        self.last_synced_file = last_synced_file
        self.sources = sources

        super().__init__(interval, end_timestamp, retry=False)

    def _pre_start(self):
        self._db = PostgresDB()
        self._klg_db = ArangoDB()
        # logger.info(f'Connect to graph: {klg_db.connection_url}')
        # self._exporter = ArangoDBExporter(klg_db)

        if (self.start_timestamp is not None) or (not os.path.isfile(self.last_synced_file)):
            _DEFAULT_START_TIME = int(time.time() - TimeConstants.DAYS_30)
            init_last_synced_file(self.start_timestamp or _DEFAULT_START_TIME, self.last_synced_file)
        self.start_timestamp = read_last_synced_file(self.last_synced_file)

        self.exchange_wallets = self.get_exchange_wallets()
        self.cex_deposit_wallets = {}

    def _start(self):
        self.end_time = self.start_timestamp + self.interval
        logger.info(f'Start execute from {human_readable_time(self.start_timestamp)} to {human_readable_time(self.end_time)}')

    def _execute(self, *args, **kwargs):
        job = ExchangeDepositWalletsJob(
            _db=self._db,
            _blockchain_etl=self._blockchain_etl,
            klg=self._klg_db,
            exchange_wallets=self.exchange_wallets,
            chain_id=self.chain_id,
            start_timestamp=self.start_timestamp, end_timestamp=self.end_time, period=self.period,
            batch_size=1, max_workers=self.max_workers,
            sources=self.sources
        )
        job.run()

    def _end(self):
        self.start_timestamp = self._get_next_synced_timestamp() - self.interval
        write_last_synced_file(self.last_synced_file, self.start_timestamp)

        time.sleep(3)

    def _get_next_synced_timestamp(self):
        # Get the next execute timestamp
        return round_timestamp(self.end_time, round_time=self.interval) + self.interval

    def get_exchange_wallets(self):
        with open('artifacts/centralized_exchange_addresses.json') as f:
            centralized_exchanges = json.load(f)

        exchange_wallets = {}
        for exchange_id, info in centralized_exchanges.items():
            wallets = info.get('wallets', {})
            exchange_wallets.update({w.lower(): exchange_id for w in wallets.get(self.chain_id, [])})
        return exchange_wallets
