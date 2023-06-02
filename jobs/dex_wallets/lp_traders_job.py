import time
from typing import List, Dict

from constants.time_constants import TimeConstants, SLEEP_DURATION
from constants.tag_constants import WalletTags
from databases.mongodb import MongoDB
from databases.mongodb_entity import MongoDBEntity
from jobs.cli_job import CLIJob
from models.wallet.wallet_trade_lp import WalletTradeLP
# from models.project import Project
from services.crawlers.dextools_crawler import DEXToolsCrawler
from utils.logger_utils import get_logger
from utils.retry_handler import retry_handler

LP_PAIRS_BALANCE_THRESHOLD = 1e5  # only get LP contracts with balance above this threshold
DEXTOOLS_PAGE_NUMBER_LIMIT = 50
logger = get_logger('LP traders collector Job')


class DexTradersCollectorJob(CLIJob):
    def __init__(self, chain_id: str, db: MongoDB, klg: MongoDBEntity,
                 interval=TimeConstants.A_DAY, end_timestamp=None):
        super().__init__(interval=interval, end_timestamp=end_timestamp)
        self.chain_id = chain_id
        self.db = db
        self.klg = klg

        self.crawler = DEXToolsCrawler(page_number_limit=DEXTOOLS_PAGE_NUMBER_LIMIT)

    def _start(self):
        # self.dex_trader_wallets: Dict[str, WalletTradeLP] = dict()
        pass

    def _execute(self, *args, **kwargs):
        logger.info(f"Start crawling traders of LP contracts on chain {self.chain_id}")
        lp_contracts_data = self._get_lp_contracts()
        number_of_lps_to_crawl = len(lp_contracts_data)
        logger.info(f"Number of lp tokens to crawl: {number_of_lps_to_crawl}. Start crawling...")

        for _count, lp_contract_datum in enumerate(lp_contracts_data):
            try:
                dex_traders = self._get_dex_traders(self.chain_id, lp_contract_datum)
                self._export_wallets(wallets=dex_traders)
                logger.info(f"Exported {len(dex_traders)} traders of lp token {_count+1} / {number_of_lps_to_crawl} "
                            f"on chain {self.chain_id}")
            except TypeError:
                logger.warning(f"Cannot crawl transactions of LP {lp_contract_datum['address']} from Dextools")

    def _export_wallets(self, wallets: List[WalletTradeLP]):
        wallets_data = []
        for wallet in wallets:
            wallet_dict = wallet.to_dict()
            wallets_data.append(wallet_dict)
        self.db.update_wallets(wallets_data)

    def _retry(self):
        logger.warning(f'Try again after {SLEEP_DURATION} seconds ...')
        time.sleep(SLEEP_DURATION)

    def _end(self):
        # del self.dex_trader_wallets
        pass

    def _get_lp_contracts(self) -> List[dict]:
        lp_contracts_data = self.db.get_pair_by_balance_range(chain_id=self.chain_id, lower=LP_PAIRS_BALANCE_THRESHOLD)
        all_lp_contracts = [{'address': datum['address'],
                             'dex_id': datum['dex']}
                            for datum in lp_contracts_data]
        return all_lp_contracts

    @retry_handler
    def _get_dex_traders(self, chain_id, lp_token: dict) -> List[WalletTradeLP]:
        _dex_traders: Dict[str, WalletTradeLP] = {}

        lp_transactions = self.crawler.get_lp_transactions(chain_id=chain_id,
                                                           lp_address=lp_token['address'])
        # dex_project = Project(project_id=lp_token['dex_id'],
        #                       chain_id=chain_id,
        #                       address=lp_token['address'])
        for transaction in lp_transactions:
            dex_wallet_addr = transaction.maker_address
            if dex_wallet_addr in _dex_traders:
                _dex_traders[dex_wallet_addr].add_project(project_id=lp_token['dex_id'],
                                                          chain_id=chain_id,
                                                          address=lp_token['address'])
            else:
                new_dex_trader_wallet = WalletTradeLP(address=transaction.maker_address)
                # new_dex_trader_wallet.add_tags(WalletTags.dex_trader)
                new_dex_trader_wallet.add_project(project_id=lp_token['dex_id'],
                                                  chain_id=chain_id,
                                                  address=lp_token['address'])
                _dex_traders[dex_wallet_addr] = new_dex_trader_wallet

            if transaction.is_bot:
                _dex_traders[dex_wallet_addr].add_tags(WalletTags.bot)

        return list(_dex_traders.values())
