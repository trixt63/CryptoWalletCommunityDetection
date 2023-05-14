import time
from typing import List, Dict

from constants.time_constants import TimeConstants, SLEEP_DURATION
from constants.tag_constants import WalletTags
from databases.mongodb import MongoDB
from databases.mongodb_entity import MongoDBEntity
from constants.mongodb_entity_constants import LPConstants
from jobs.cli_job import CLIJob
# from models.wallet.wallet import Wallet
from models.wallet.wallet_trade_lp import WalletTradeLP
from models.project import Project
from services.crawlers.dextools_crawler import DEXToolsCrawler
from utils.logger_utils import get_logger
from utils.retry_handler import retry_handler

logger = get_logger('DEX traders Collector Job')


class DexTradersCollectorJob(CLIJob):
    def __init__(self, chain_ids: List[str], db: MongoDB, klg: MongoDBEntity,
                 interval=TimeConstants.A_DAY, end_timestamp=None):
        super().__init__(interval=interval, end_timestamp=end_timestamp)
        self.chain_ids = chain_ids
        self.db = db
        self.klg = klg

        self.crawler = DEXToolsCrawler()

    def _start(self):
        self.dex_wallets: Dict[str, WalletTradeLP] = dict()

    def _execute(self, *args, **kwargs):
        for chain_id in self.chain_ids:
            logger.info(f"Start crawl traders of swap contracts on chain {chain_id}")
            logger.info(f"Getting lp contracts on chain {chain_id}")
            # lp_tokens = self.db.get_lp_contract_addresses(chain_id)
            lp_contracts = self. _get_lp_contracts(chain_id)

            logger.info(f"Finish get lp contract addresses. Start crawling...")
            for _count, lp_token in enumerate(lp_contracts):
                try:
                    self._get_dex_traders(chain_id, lp_token)
                except TypeError:
                    logger.warning(f"Cannot crawl transactions of LP {lp_token['address']} from Dextools")
        self._export_wallets(list(self.dex_wallets.values()))

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
        del self.dex_wallets

    def _get_lp_contracts(self, chain_id):
        lp_contracts_data = self.klg.get_lp_contracts(chain_id)
        all_lp_contracts = [{'address': datum['address'],
                              'name': LPConstants.LP_NAME_ID_MAPPINGS.get(datum['name']),
                              'number_of_calls': datum.get('numberOfThisMonthCalls', 0)}
                             for datum in lp_contracts_data]
        lp_contracts_list = sorted(all_lp_contracts, key=lambda d: d['number_of_calls'], reverse=True)
        lp_contracts_list = lp_contracts_list[:200]
        return lp_contracts_list

    @retry_handler
    def _get_dex_traders(self, chain_id, lp_token):
        transactions = self.crawler.get_exchanges(chain_id=chain_id,
                                                  contract_address=lp_token['address'])
        lp_project = Project(project_id=lp_token['name'],
                             chain_id=chain_id,
                             address=lp_token['address'])
        for tx in transactions:
            dex_wallet_addr = tx.maker_address
            if dex_wallet_addr in self.dex_wallets:
                self.dex_wallets[dex_wallet_addr].add_project(lp_project)
            else:
                new_dex_trader_wallet = WalletTradeLP(address=tx.maker_address)
                new_dex_trader_wallet.add_tags(WalletTags.dex_trader)
                new_dex_trader_wallet.add_project(lp_project)
                self.dex_wallets[dex_wallet_addr] = new_dex_trader_wallet
            if tx.is_bot:
                self.dex_wallets[dex_wallet_addr].add_tags(WalletTags.bot)
