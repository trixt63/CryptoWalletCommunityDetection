import gc
import os
from typing import Dict
from cli_scheduler.scheduler_job import SchedulerJob
from web3 import Web3

from databases.blockchain_etl import BlockchainETL
from databases.mongodb import MongoDB
from models.wallet.wallet_own_lp import WalletOwnLP
from utils.logger_utils import get_logger

logger = get_logger('LP Deployers job')
PAIR_ID_BATCH_SIZE = 1000
MISSING_TX_FILE_NAME = 'missing_tx.txt'


class LPDeployersJob(SchedulerJob):
    def __init__(
            self,
            scheduler: str,
            chain_id: str,
            web3: Web3,
            importer: MongoDB,
            exporter: MongoDB,
            transactions_db: BlockchainETL
    ):
        self.chain_id = chain_id
        self._importer = importer
        self._exporter = exporter
        self._transactions_db = transactions_db
        self._web3 = web3

        super().__init__(scheduler=scheduler)
        # log file
        self._missing_tx_file_path = f".data/{chain_id}_{MISSING_TX_FILE_NAME}"
        if os.path.exists(self._missing_tx_file_path):
            os.remove(self._missing_tx_file_path)

    def _start(self):
        self._wallets: Dict[str, WalletOwnLP] = dict()
        # get latest pair id & batches
        self._latest_pair_id = self._importer.get_latest_pair_id(chain_id=self.chain_id)

    def _end(self):
        self._export_wallets()
        gc.collect()

    def _execute(self, *args, **kwargs):
        # get lp_addresses
        for pair_id in range(0, self._latest_pair_id, PAIR_ID_BATCH_SIZE):
            logger.info(f"Getting lp owners from pair {pair_id} to {pair_id+PAIR_ID_BATCH_SIZE} / {self._latest_pair_id}")
            _cursor = self._importer.get_lps_by_pair_ids(chain_id=self.chain_id,
                                                         start_pair_id=pair_id,
                                                         end_pair_id=pair_id+PAIR_ID_BATCH_SIZE)
            lp_contracts = {datum['address']: datum['dex'] for datum in _cursor}

            for lp_addr, dex_id in lp_contracts.items():
                try:
                    # get pair creation event of the addresses
                    pair_created_event = self._importer.get_pair_created_event(chain_id=self.chain_id, address=lp_addr)
                    tx_hash = pair_created_event['transaction_hash']
                    # get transaction with to_address in lp_addresses
                    lp_owner_addr = self._transactions_db.get_transaction_by_hash(tx_hash).get('from_address')
                    if not lp_owner_addr:
                        transaction_ = self._web3.eth.getTransaction(tx_hash)
                        lp_owner_addr = transaction_['from'].lower()
                        # save the missing blockNumber
                        missing_block_number = transaction_['blockNumber']
                        self._write_missing_tx_file(tx_hash, missing_block_number)
                    # add wallet
                    if lp_owner_addr in self._wallets:
                        self._wallets[lp_owner_addr].add_project(project_id=dex_id,
                                                                 chain_id=self.chain_id,
                                                                 address=lp_addr)
                    else:
                        new_lp_owner_wallets = WalletOwnLP(lp_owner_addr)
                        new_lp_owner_wallets.add_project(project_id=dex_id,
                                                         chain_id=self.chain_id,
                                                         address=lp_addr)
                        self._wallets[lp_owner_addr] = new_lp_owner_wallets

                except Exception as ex:
                    logger.warning(f"Error at pair {lp_addr}: {ex}")
                    continue

            logger.info(f"Got {len(self._wallets)} lp owners of "
                        f"{pair_id+PAIR_ID_BATCH_SIZE} / {self._latest_pair_id}")

            self._export_wallets()

    def _export_wallets(self):
        wallets_data = [wallet.to_dict() for wallet in self._wallets.values()]
        self._exporter.update_wallets(wallets_data)

    def _write_missing_tx_file(self, tx_hash: str, block_number: int):
        _file = open(self._missing_tx_file_path, "a+")
        _file.write(f"{tx_hash}: {block_number}\n")
        _file.close()
