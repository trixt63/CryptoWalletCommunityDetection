from constants.tag_constants import WalletTags
from models.wallet.wallet import Wallet


class WalletLending(Wallet):
    def __init__(self, address, last_updated_at):
        super().__init__(address, last_updated_at)
        self.add_tags(WalletTags.lending_wallet)

    def to_dict(self):
        returned_dict = super().to_dict()
        returned_dict['lendingPools'] = returned_dict.pop('protocols')
        return returned_dict
