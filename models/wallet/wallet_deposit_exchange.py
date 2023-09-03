from typing import Dict

from constants.tag_constants import WalletTags
from models.wallet.wallet import Wallet
from models.protocol import Protocol


class WalletDepositExchange(Wallet):
    def __init__(self, address, last_updated_at):
        super().__init__(address, last_updated_at)
        self.add_tags(WalletTags.cex_deposit_wallet)

    def to_dict(self):
        returned_dict = super().to_dict()
        exchanges = returned_dict.pop('protocols')
        returned_dict['depositedExchanges'] = {
            cex_id: [depl['chainId'] for depl in cex_deployments]
            for cex_id, cex_deployments in exchanges.items()
        }

        return returned_dict

    def to_dict_single_chain(self):
        returned_dict = super().to_dict()
        _exchanges = returned_dict.pop('protocols')

        _chains = set()  # should have only one element
        returned_dict['depositedExchanges'] = set()
        for cex_id, cex_deployments in _exchanges.items():
            returned_dict['depositedExchanges'].add(cex_id)
            _chains.add(cex_deployments[0]['chainId'])

        try:
            assert len(_chains) == 1
        except AssertionError:
            raise AssertionError("'depositWallets' should only contain data on 1 chain")

        returned_dict['depositedExchanges'] = list(returned_dict['depositedExchanges'])
        returned_dict['chainId'] = _chains.pop()

        return returned_dict
