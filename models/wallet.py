from constants.tag_constants import WalletTags


class Wallet:
    def __init__(self, address):
        self.address = address
        self.tags = list()

        self.lendings = list()
        self.exchange_deposits = set()
        self.dexes = set()

    def add_tags(self, new_tag: str):
        if new_tag not in WalletTags.all_wallet_tags:
            print(f"{new_tag} not in supported wallet tags")
            return None
        if new_tag not in self.tags:
            self.tags.append(new_tag)

    def to_dict(self):
        returned_dict = {
            'address': self.address,
            'tags': self.tags
        }

        if len(self.lendings):
            returned_dict['lendings'] = self.lendings

        if len(self.exchange_deposits):
            returned_dict['exchangeDeposits'] = list(self.exchange_deposits)

        if len(self.dexes):
            returned_dict['dexes'] = list(self.dexes)

        return returned_dict

    def __eq__(self, other):
        return self.address == other.address

    def __hash__(self):
        return hash(self.address)
