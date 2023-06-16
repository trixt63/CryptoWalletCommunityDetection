class LPTransaction:
    """Model for DexTools' crawled transactions"""
    def __init__(self, maker_address: str, transaction_hash: str, is_bot: bool, timestamp=None):
        self.maker_address = maker_address
        self.tx_hash = transaction_hash
        self.is_bot = is_bot
        self.time = timestamp

    def __eq__(self, other):
        return self.tx_hash == other.tx_hash

    def __hash__(self):
        return hash(self.tx_hash)

    def to_dict(self):
        return {
            # 'timestamp': self._timestamp,
            'maker_address': self.maker_address,
            'transaction_hash': self.tx_hash,
            'is_bot': self.is_bot
        }