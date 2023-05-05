import hashlib


class DexTransaction:
    def __init__(self, timestamp: int, maker_address: str, transaction_hash: str, is_bot: bool):
        self._timestamp = timestamp
        self._maker_address = maker_address
        self._tx_hash = transaction_hash
        self._is_bot = is_bot

    def __eq__(self, other):
        return self._tx_hash == other._tx_hash

    def __hash__(self):
        return hash(self._tx_hash)

    def to_dict(self):
        return {
            'timestamp': self._timestamp,
            'maker_address': self._maker_address,
            'transaction_hash': self._tx_hash,
            'is_bot': self._is_bot
        }