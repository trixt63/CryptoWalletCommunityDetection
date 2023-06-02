import gc
import os
from typing import Dict, List
from cli_scheduler.scheduler_job import SchedulerJob
from web3 import Web3

from databases.blockchain_etl import BlockchainETL
from databases.mongodb import MongoDB
from models.wallet.wallet_deploy_lp import WalletDeployLP
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

    def _start(self):
        # missing tx file
        self._missing_tx_file_path = f".data/{self.chain_id}_{MISSING_TX_FILE_NAME}"
        if os.path.exists(self._missing_tx_file_path):
            os.remove(self._missing_tx_file_path)

    def _end(self):
        gc.collect()

    def _execute(self, *args, **kwargs):
        # get lp_addresses by pair_ids ranges
        logger.info(f"Start getting & exporting LP deployers on chain {self.chain_id}")

        self._latest_pair_id = self._importer.get_latest_pair_id(chain_id=self.chain_id)
        for pair_id in range(0, self._latest_pair_id, PAIR_ID_BATCH_SIZE):
            self._export_lp_deployers_by_pair_id(pair_id_first=pair_id,
                                                 pair_id_last=pair_id + PAIR_ID_BATCH_SIZE)

    def _export_lp_deployers_by_pair_id(self, pair_id_first, pair_id_last):
        _lp_deployers: Dict[str, WalletDeployLP] = dict()

        _cursor = self._importer.get_lps_by_pair_ids(chain_id=self.chain_id,
                                                     start_pair_id=pair_id_first,
                                                     end_pair_id=pair_id_last)
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
                    self._write_missing_tx_file(tx_hash, missing_block_number)
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

        _progress = pair_id_last / self._latest_pair_id
        logger.info(f"Exported this batch: {len(_lp_deployers)} LP deployers. "
                    f"Progress: {_progress*100:.2f}%")

    def _export_wallets(self, wallets: List[WalletDeployLP]):
        wallets_data = [wallet.to_dict() for wallet in wallets]
        self._exporter.update_wallets(wallets_data)

    def _write_missing_tx_file(self, tx_hash: str, block_number: int):
        _file = open(self._missing_tx_file_path, "a+")
        _file.write(f"{tx_hash}: {block_number}\n")
        _file.close()
