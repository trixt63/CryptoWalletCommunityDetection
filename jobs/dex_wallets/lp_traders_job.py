import gc
import time
from typing import List, Dict

from constants.time_constants import TimeConstants, SLEEP_DURATION
from constants.tag_constants import WalletTags
from databases.mongodb import MongoDB
from databases.mongodb_entity import MongoDBEntity
from jobs.cli_job import CLIJob
from models.wallet.wallet_trade_lp import WalletTradeLP
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

    def _start(self):
        self.crawler = DEXToolsCrawler(page_number_limit=DEXTOOLS_PAGE_NUMBER_LIMIT)
        self.lp_contracts_data: List[Dict] = list()

    def _execute(self, *args, **kwargs):
        logger.info(f"Start crawling traders of LP contracts on chain {self.chain_id}")
        self._get_lp_contracts_data()
        # lp_contracts_data = [
        #     {
        #         'address': '0x3139ffc91b99aa94da8a2dc13f1fc36f9bdc98ee',
        #         'dex': 'uniswap'
        #     }
        # ]
        number_of_lps_to_crawl = len(self.lp_contracts_data)
        logger.info(f"Number of lp tokens to crawl: {number_of_lps_to_crawl}. Start crawling...")

        for _count, lp_contract_datum in enumerate(self.lp_contracts_data):
            try:
                dex_traders = self._crawl_dex_traders(self.chain_id, lp_contract_datum)
                self._export_wallets(wallets=dex_traders)
                logger.info(f"Exported {len(dex_traders)} traders of lp token {_count+1} / {number_of_lps_to_crawl} "
                            f"on chain {self.chain_id}")
            except TypeError:
                logger.warning(f"Cannot crawl transactions of LP {lp_contract_datum['address']} from Dextools")

    def _export_wallets(self, wallets: List[WalletTradeLP]):
        wallets_data = []
        for wallet in wallets:
            wallet_dict = wallet.to_dict()
            wallet_dict['lastUpdatedAt'] = int(time.time())
            wallets_data.append(wallet_dict)
        self.db.update_wallets(wallets_data)

    def _retry(self):
        logger.warning(f'Try again after {SLEEP_DURATION} seconds ...')
        time.sleep(SLEEP_DURATION)

    def _end(self):
        del self.crawler
        gc.collect()
        
    def _get_lp_contracts_data(self):
        lp_contracts_data = self.db.get_pair_by_balance_range(chain_id=self.chain_id, lower=LP_PAIRS_BALANCE_THRESHOLD)
        self.lp_contracts_data = [{'address': datum['address'],
                                   'dex': datum['dex']}
                                  for datum in lp_contracts_data]

    @retry_handler
    def _crawl_dex_traders(self, chain_id, lp_token: dict) -> List[WalletTradeLP]:
        _dex_traders: Dict[str, WalletTradeLP] = {}

        lp_transactions = self.crawler.scrap_lp_transactions(chain_id=chain_id,
                                                             lp_address=lp_token['address'])

        for transaction in lp_transactions:
            dex_wallet_addr = transaction.maker_address
            if dex_wallet_addr in _dex_traders:
                _dex_traders[dex_wallet_addr].add_protocol(protocol_id=lp_token['dex'],
                                                           chain_id=chain_id,
                                                           address=lp_token['address'])
            else:
                new_dex_trader_wallet = WalletTradeLP(address=transaction.maker_address,
                                                      last_updated_at=int(time.time()))
                new_dex_trader_wallet.add_protocol(protocol_id=lp_token['dex'],
                                                   chain_id=chain_id,
                                                   address=lp_token['address'])
                _dex_traders[dex_wallet_addr] = new_dex_trader_wallet

            if transaction.is_bot:
                _dex_traders[dex_wallet_addr].add_tags(WalletTags.bot)

        return list(_dex_traders.values())
