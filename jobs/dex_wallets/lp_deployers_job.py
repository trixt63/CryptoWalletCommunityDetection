import gc
import os
import time
from typing import Dict, List
from cli_scheduler.scheduler_job import SchedulerJob
from web3 import Web3

from databases.blockchain_etl import BlockchainETL
from databases.mongodb import MongoDB
from models.wallet.wallet_deploy_lp import WalletDeployLP
from utils import file_utils
from utils.logger_utils import get_logger

logger = get_logger('LP Deployers job')
DEFAULT_START_PAIR_ID = 0
PAIR_ID_BATCH_SIZE = 1000
MISSING_TX_LOG_FILE = 'missing_transactions.txt'


class LPDeployersJob(SchedulerJob):
    def __init__(
            self,
            interval: int,
            chain_id: str,
            web3: Web3,
            importer: MongoDB,
            exporter: MongoDB,
            transactions_db: BlockchainETL,
            last_synced_file="last_synced_pair_id.txt",
            start_pair_id=DEFAULT_START_PAIR_ID,
    ):
        super().__init__(scheduler=f'^true@{interval}#true')

        self.chain_id = chain_id
        self._importer = importer
        self._exporter = exporter
        self._transactions_db = transactions_db
        self._web3 = web3

        self._start_pair_id = start_pair_id
        self._LAST_SYNCED_FILE = last_synced_file
        self._MISSING_TXS_FILE = f".data/{self.chain_id}_{MISSING_TX_LOG_FILE}"

    def _pre_start(self):
        # init the file to save last crawled pair id
        if self._start_pair_id or (not os.path.isfile(self._LAST_SYNCED_FILE)):
            file_utils.init_last_synced_file(self._start_pair_id or DEFAULT_START_PAIR_ID, self._LAST_SYNCED_FILE)

    def _start(self):
        # get the last crawled pair_id & the latest pair id on the blockchain
        self._start_pair_id = file_utils.read_last_synced_file(self._LAST_SYNCED_FILE)
        self._latest_pair_id = self._importer.get_latest_pair_id(chain_id=self.chain_id)

    def _end(self):
        # log the latest pair id crawled before sleep
        file_utils.write_last_synced_file(self._LAST_SYNCED_FILE, self._latest_pair_id + 1)
        gc.collect()

    def _execute(self, *args, **kwargs):
        # get lp_addresses by pair_ids ranges
        logger.info(f"Start getting & exporting LP deployers on chain {self.chain_id}")

        for pair_id in range(self._start_pair_id, self._latest_pair_id, PAIR_ID_BATCH_SIZE):
            self._export_lp_deployers_by_pair_id(start_pair_id=pair_id,
                                                 end_pair_id=pair_id + PAIR_ID_BATCH_SIZE)
            file_utils.write_last_synced_file(self._LAST_SYNCED_FILE, pair_id + PAIR_ID_BATCH_SIZE + 1)

    def _export_lp_deployers_by_pair_id(self, start_pair_id, end_pair_id):
        _lp_deployers: Dict[str, WalletDeployLP] = dict()

        _cursor = self._importer.get_lps_by_pair_ids(chain_id=self.chain_id,
                                                     start_pair_id=start_pair_id,
                                                     end_pair_id=end_pair_id)
        lp_contracts = {datum['address']: datum['dex'] for datum in _cursor}

        for lp_addr, dex_id in lp_contracts.items():
            try:
                # get pair creation event of the addresses
                pair_created_event = self._importer.get_pair_created_event(chain_id=self.chain_id, address=lp_addr)
                tx_hash = pair_created_event['transaction_hash']
                # get transaction with to_address in lp_addresses
                transaction_ = self._transactions_db.get_transaction_by_hash(tx_hash)
                if not transaction_:
                    transaction_ = self._web3.eth.getTransaction(tx_hash)
                    lp_owner_addr = transaction_['from'].lower()
                    # save the missing blockNumber
                    missing_block_number = transaction_['blockNumber']
                    file_utils.append_log_file(f"{tx_hash}:{missing_block_number}",
                                               self._MISSING_TXS_FILE)
                else:
                    lp_owner_addr = transaction_['from_address']
                # add wallet
                # TODO: too complicated & redundant
                if lp_owner_addr in _lp_deployers:
                    _lp_deployers[lp_owner_addr].add_protocol(protocol_id=dex_id,
                                                              chain_id=self.chain_id,
                                                              address=lp_addr)
                else:
                    new_lp_owner_wallets = WalletDeployLP(lp_owner_addr)
                    new_lp_owner_wallets.add_protocol(protocol_id=dex_id,
                                                      chain_id=self.chain_id,
                                                      address=lp_addr)
                    _lp_deployers[lp_owner_addr] = new_lp_owner_wallets

            except Exception as ex:
                logger.warning(f"Error at pair {lp_addr}: {ex}")
                continue

        self._export_wallets(list(_lp_deployers.values()))

        _progress = end_pair_id / self._latest_pair_id
        logger.info(f"Exported this batch: {len(_lp_deployers)} LP deployers. "
                    f"Progress: {_progress*100:.2f}%")

    def _export_wallets(self, wallets: List[WalletDeployLP]):
        wallets_data = [wallet.to_dict() for wallet in wallets]
        for datum in wallets_data:
            datum['lastUpdatedAt'] = int(time.time())
        self._exporter.update_wallets(wallets_data)
