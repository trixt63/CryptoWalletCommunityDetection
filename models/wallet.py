from constants.tag_constants import WalletTags


class Wallet:
    def __init__(self, chain_id, address):
        self.chain_id = chain_id
        self.address = address
        self.tags = list()

    def add_tags(self, new_tag: str):
        if new_tag not in WalletTags.all_wallet_tags:
            print(f"{new_tag} not in supported wallet tags")
            return None
        self.tags.append(new_tag)

    def to_dict(self):
        return {
            'chainId': self.chain_id,
            'address': self.address,
            'tags': self.tags
        }
