import json
import time
from typing import List

from constants.time_constants import TimeConstants, SLEEP_DURATION
from constants.tag_constants import WalletTags
from databases.mongodb import MongoDB
from jobs.cli_job import CLIJob
from models.wallet import Wallet
from models.dex_transaction import DexTransaction
from services.crawlers.dextools_crawler import DEXToolsCrawler
from utils.logger_utils import get_logger

logger = get_logger('DEX traders Collector Job')


class DexTradersCollectorJob(CLIJob):
    def __init__(self, chain_ids: List[str], db: MongoDB,
                 interval=TimeConstants.A_DAY, end_timestamp=None):
        super().__init__(interval=interval, end_timestamp=end_timestamp)
        self.chain_ids = chain_ids
        self.db = db

        self.crawler = DEXToolsCrawler()
        # self.chain_id = chain_id
        # self.contract_addresses = contract_addresses

    def _pre_start(self):
        # logger.info(f'Connect to graph: {self._db.connection_url}')
        pass

    def _execute(self, *args, **kwargs):
        for chain_id in self.chain_ids:
            logger.info(f"Start crawl traders of swap contracts on chain {chain_id}")
            logger.info(f"Getting lp contracts on chain {chain_id}")
            contract_addresses = self.db.get_lp_contract_addresses(chain_id)

            logger.info(f"Got {len(contract_addresses)} lp contracts from chain {chain_id}. Start crawling")
            for _count, contract_addr in enumerate(contract_addresses):
                transactions = self.crawler.get_exchanges(chain_id=chain_id,
                                                          contract_address=contract_addr)
                for tx in transactions:
                    new_dex_trader_wallet = Wallet(address=tx.maker_address)
                    new_dex_trader_wallet.add_tags(WalletTags.dex_traders)
                    if tx.is_bot:
                        new_dex_trader_wallet.add_tags(WalletTags.bot)
                logger.info(f"Finish crawling {_count}/{len(contract_addresses)} lp contracts")

    def _export(self, wallets: List[Wallet]):
        # self._exporter.export_projects(data)
        self.db.update_wallets(wallets)

    def _retry(self):
        logger.warning(f'Try again after {SLEEP_DURATION} seconds ...')
        time.sleep(SLEEP_DURATION)
