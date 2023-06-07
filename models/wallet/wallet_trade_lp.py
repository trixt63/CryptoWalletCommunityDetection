from typing import Dict

from models.wallet.wallet import Wallet
from models.protocol import Protocol
from constants.tag_constants import WalletTags


class WalletTradeLP(Wallet):
    def __init__(self, address, last_updated_at):
        super().__init__(address, last_updated_at)
        self.add_tags(WalletTags.lp_trader)

    def to_dict(self):
        returned_dict = super().to_dict()
        returned_dict['tradedLPs'] = returned_dict.pop('protocols')
        return returned_dict
