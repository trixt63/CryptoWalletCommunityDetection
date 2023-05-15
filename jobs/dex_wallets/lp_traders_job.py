import time
from typing import List, Dict

from constants.time_constants import TimeConstants, SLEEP_DURATION
from constants.tag_constants import WalletTags
from databases.mongodb import MongoDB
from databases.mongodb_entity import MongoDBEntity
from constants.mongodb_entity_constants import LPConstants
from jobs.cli_job import CLIJob
from models.wallet.wallet_trade_lp import WalletTradeLP
from models.project import Project
from services.crawlers.dextools_crawler import DEXToolsCrawler
from utils.logger_utils import get_logger
from utils.retry_handler import retry_handler

LP_CONTRACTS_LIMIT = 200
logger = get_logger('DEX traders Collector Job')


class DexTradersCollectorJob(CLIJob):
    def __init__(self, chain_id: str, db: MongoDB, klg: MongoDBEntity,
                 interval=TimeConstants.A_DAY, end_timestamp=None):
        super().__init__(interval=interval, end_timestamp=end_timestamp)
        self.chain_id = chain_id
        self.db = db
        self.klg = klg

        self.crawler = DEXToolsCrawler()

    def _start(self):
        self.dex_trader_wallets: Dict[str, WalletTradeLP] = dict()

    def _execute(self, *args, **kwargs):
        logger.info(f"Start crawl traders of swap contracts on chain {self.chain_id}")
        logger.info(f"Getting lp contracts on chain {self.chain_id}")
        lp_contracts_data = self._get_lp_contracts(self.chain_id)

        logger.info(f"Finish get lp contract addresses. Start crawling...")
        for _count, lp_contract_datum in enumerate(lp_contracts_data):
            try:
                self._get_dex_trader_wallets(self.chain_id, lp_contract_datum)
                logger.info(f"Get {_count+1} lp tokens on chain {self.chain_id}")
            except TypeError:
                logger.warning(f"Cannot crawl transactions of LP {lp_contract_datum['address']} from Dextools")

        self._export_wallets(list(self.dex_trader_wallets.values()))

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
        del self.dex_trader_wallets

    def _get_lp_contracts(self, chain_id) -> List[dict]:
        lp_contracts_data = self.klg.get_lp_contracts(chain_id)
        all_lp_contracts = [{'address': datum['address'],
                              'name': LPConstants.LP_NAME_ID_MAPPINGS.get(datum['name']),
                              'number_of_calls': datum.get('numberOfThisMonthCalls', 0)}
                             for datum in lp_contracts_data]
        lp_contracts_list = sorted(all_lp_contracts, key=lambda d: d['number_of_calls'], reverse=True)
        lp_contracts_list = lp_contracts_list[:LP_CONTRACTS_LIMIT]
        return lp_contracts_list

    @retry_handler
    def _get_dex_trader_wallets(self, chain_id, lp_token: dict):
        transactions = self.crawler.get_exchanges(chain_id=chain_id,
                                                  contract_address=lp_token['address'])
        dex_project = Project(project_id=lp_token['name'],
                              chain_id=chain_id,
                              address=lp_token['address'])
        for tx in transactions:
            dex_wallet_addr = tx.maker_address
            if dex_wallet_addr in self.dex_trader_wallets:
                self.dex_trader_wallets[dex_wallet_addr].add_project(dex_project)
            else:
                new_dex_trader_wallet = WalletTradeLP(address=tx.maker_address)
                # new_dex_trader_wallet.add_tags(WalletTags.dex_trader)
                new_dex_trader_wallet.add_project(dex_project)
                self.dex_trader_wallets[dex_wallet_addr] = new_dex_trader_wallet

            if tx.is_bot:
                self.dex_trader_wallets[dex_wallet_addr].add_tags(WalletTags.bot)
