from models.wallet.wallet import Wallet
from constants.tag_constants import WalletTags


class WalletDeployLP(Wallet):
    def __init__(self, address, last_updated_at):
        super().__init__(address, last_updated_at)
        self.add_tags(WalletTags.lp_deployer)

    def to_dict(self):
        returned_dict = super().to_dict()
        returned_dict['deployedLPs'] = returned_dict.pop('protocols')
        return returned_dict
