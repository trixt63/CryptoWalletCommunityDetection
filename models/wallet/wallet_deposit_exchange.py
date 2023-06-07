from typing import Dict

from constants.tag_constants import WalletTags
from models.wallet.wallet import Wallet
from models.protocol import Protocol


class WalletDepositExchange(Wallet):
    def __init__(self, address, last_updated_at):
        super().__init__(address, last_updated_at)
        self.add_tags(WalletTags.centralized_exchange_deposit_wallet)

    def to_dict(self):
        returned_dict = super().to_dict()
        exchanges = returned_dict.pop('protocols')
        returned_dict['depositedExchanges'] = {
            cex_id: [depl['chainId'] for depl in cex_deployments]
            for cex_id, cex_deployments in exchanges.items()
        }

        return returned_dict
