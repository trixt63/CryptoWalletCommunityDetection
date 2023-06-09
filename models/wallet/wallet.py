from typing import Dict, List

from constants.tag_constants import WalletTags


class Wallet:
    def __init__(self, address, last_updated_at: int, tag=None):
        self.address = address
        self.last_updated_at = last_updated_at
        self.tags = list()
        if tag:
            self.add_tags(tag)

        self.protocols: Dict[str, List[Dict]] = dict()

    def add_tags(self, new_tag: str):
        # if new_tag not in WalletTags.all_wallet_tags:
        if not hasattr(WalletTags, new_tag):
            print(f"{new_tag} not in supported wallet tags")
            return None
        if new_tag not in self.tags:
            self.tags.append(new_tag)

    def add_protocol(self, protocol_id, chain_id, address):
        protocol = dict(
            chain_id=chain_id,
            address=address
        )
        if protocol_id not in self.protocols:
            self.protocols[protocol_id] = list()
        self.protocols[protocol_id].append(protocol)

    def to_dict(self):
        returned_dict = {
            'address': self.address,
            'tags': self.tags,
            'lastUpdatedAt': self.last_updated_at,
            'protocols': {
                protocol_id: [{'address': depl['address'],
                               'chainId': depl['chain_id']}
                              for depl in protocol_deployments]
                for protocol_id, protocol_deployments in self.protocols.items()
            }
        }
        return returned_dict

    def __eq__(self, other):
        return self.address == other.address

    def __hash__(self):
        return hash(self.address)

    def not_empty(self) -> bool:
        if self.protocols:
            return True
        return False
