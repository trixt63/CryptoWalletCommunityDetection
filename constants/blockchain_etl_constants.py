from src.constants.network_constants import Networks


class BlockchainETLCollections:
    transactions = 'transactions'
    blocks = 'blocks'
    collectors = 'collectors'


class BlockchainETLIndexes:
    ttl_transactions = 'ttl_transactions'
    ttl_blocks = 'ttl_blocks'


class DBPrefix:
    mapping = {
        Networks.bsc: '',
        Networks.ethereum: 'ethereum',
        Networks.fantom: 'ftm',
        Networks.polygon: 'polygon'
    }
