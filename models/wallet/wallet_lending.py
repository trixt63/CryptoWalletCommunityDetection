from typing import Dict

from constants.tag_constants import WalletTags
from models.wallet.wallet import Wallet
from models.protocol import Protocol


class WalletLending(Wallet):
    def __init__(self, address):
        super().__init__(address)
        self._lending_pools: Dict[str, Protocol] = dict()
        self.add_tags(WalletTags.lending_wallet)

    def add_protocol(self, protocol_id, chain_id, address):
        protocol = Protocol(protocol_id, chain_id, address)
        if protocol.protocol_id in self._lending_pools:
            self._lending_pools[protocol.protocol_id].add_deployments(protocol.deployments)
        else:
            self._lending_pools[protocol.protocol_id] = protocol

    def to_dict(self):
        returned_dict = super().to_dict()

        if len(self._lending_pools):
            lending_pools_dict = {pool_id: pool_obj.to_deployments_list()
                                  for pool_id, pool_obj in self._lending_pools.items()}
            returned_dict['lendingPools'] = lending_pools_dict

        return returned_dict
