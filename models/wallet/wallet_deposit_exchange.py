from typing import Dict

from constants.tag_constants import WalletTags
from models.wallet.wallet import Wallet
from models.protocol import Protocol


class WalletDepositExchange(Wallet):
    def __init__(self, address):
        super().__init__(address)
        self.exchanges_deposited: Dict[str, Protocol] = dict()
        self.add_tags(WalletTags.centralized_exchange_deposit_wallet)

    # def add_protocol(self, protocol: Protocol):
    def add_protocol(self, protocol_id, chain_id, address):
        protocol = Protocol(protocol_id, chain_id, address)
        if protocol.protocol_id in self.exchanges_deposited:
            self.exchanges_deposited[protocol.protocol_id].add_deployments(protocol.deployments)
        else:
            self.exchanges_deposited[protocol.protocol_id] = protocol

    def to_dict(self):
        returned_dict = super().to_dict()

        if len(self.exchanges_deposited):
            deposited_exchanges = {cex_id: chains.to_deployed_chains_list()
                                   for cex_id, chains in self.exchanges_deposited.items()}
            returned_dict['depositedExchanges'] = deposited_exchanges

        return returned_dict
