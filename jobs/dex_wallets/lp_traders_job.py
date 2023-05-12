import time
from typing import List

from constants.time_constants import TimeConstants, SLEEP_DURATION
from constants.tag_constants import WalletTags
from databases.mongodb import MongoDB
from jobs.cli_job import CLIJob
from models.wallet.wallet import Wallet
from services.crawlers.dextools_crawler import DEXToolsCrawler
from utils.logger_utils import get_logger
from utils.retry_handler import retry_handler

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
            self.dex_wallets = dict()
            logger.info(f"Start crawl traders of swap contracts on chain {chain_id}")
            logger.info(f"Getting lp contracts on chain {chain_id}")
            lp_tokens = self.db.get_lp_contract_addresses(chain_id)

            logger.info(f"Finish get lp contract addresses. Start crawling...")
            for _count, lp_token in enumerate(lp_tokens):
                try:
                    self._get_dex_traders(chain_id, lp_token)
                    self._export(list(self.dex_wallets.values()))
                except TypeError:
                    logger.warning(f"Cannot crawl transactions of LP {lp_token['address']} from Dextools")

    def _export(self, wallets: List[Wallet]):
        # self._exporter.export_projects(data)
        self.db.update_wallets(wallets)

    def _retry(self):
        logger.warning(f'Try again after {SLEEP_DURATION} seconds ...')
        time.sleep(SLEEP_DURATION)

    @retry_handler
    def _get_dex_traders(self, chain_id, lp_token):
        transactions = self.crawler.get_exchanges(chain_id=chain_id,
                                                  contract_address=lp_token['address'])
        for tx in transactions:
            dex_wallets_addr = tx.maker_address
            if dex_wallets_addr in self.dex_wallets:
                self.dex_wallets[dex_wallets_addr].lps_traded.add(lp_token['dex'])
                if tx.is_bot:
                    self.dex_wallets[dex_wallets_addr].add_tags(WalletTags.bot)
            else:
                new_dex_trader_wallet = Wallet(address=tx.maker_address)
                new_dex_trader_wallet.add_tags(WalletTags.dex_trader)
                new_dex_trader_wallet.lps_traded.add(lp_token['dex'])
                if tx.is_bot:
                    new_dex_trader_wallet.add_tags(WalletTags.bot)
                self.dex_wallets[dex_wallets_addr] = new_dex_trader_wallet
