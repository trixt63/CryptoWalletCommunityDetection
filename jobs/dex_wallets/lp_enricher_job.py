import time
from typing import List, Dict

from constants.time_constants import TimeConstants, SLEEP_DURATION
from databases.mongodb import MongoDB
from databases.mongodb_entity import MongoDBEntity
from databases.blockchain_etl import BlockchainETL
from jobs.cli_job import CLIJob
from models.wallet.wallet_trade_lp import WalletTradeLP
from models.project import Project
from services.crawlers.dextools_crawler import DEXToolsCrawler
from utils.logger_utils import get_logger
from utils.retry_handler import retry_handler

LP_CONTRACTS_LIMIT = 200
logger = get_logger('LP enricher job')


class LPEnricherJob(CLIJob):
    def __init__(self, chain_id: str,
                 db: MongoDB,
                 klg: MongoDBEntity,
                 blockchain_etl: BlockchainETL,
                 interval=TimeConstants.A_DAY, end_timestamp=None):
        super().__init__(interval=interval, end_timestamp=end_timestamp)
        self.chain_id = chain_id
        self._db = db
        self._klg = klg
        self._blockchain_etl = blockchain_etl

    def _start(self):
        pass

    def _execute(self, *args, **kwargs):
        pass

    def _retry(self):
        logger.warning(f'Try again after {SLEEP_DURATION} seconds ...')
        time.sleep(SLEEP_DURATION)

    def _end(self):
        pass
