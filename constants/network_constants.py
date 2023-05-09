POLYGON_AAVE_ADDRESS = '0x8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf'
ETHEREUM_AAVE_ADDRESS = '0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9'

BSC_TRAVA_ADDRESS = '0x75de5f7c91a89c16714017c7443eca20c7a8c295'
ETH_TRAVA_ADDRESS = '0xd61afaaa8a69ba541bc4db9c9b40d4142b43b9a4'
FTM_TRAVA_ADDRESS = '0xd98bb590bdfabf18c164056c185fbb6be5ee643f'

BSC_VALAS_ADDRESS = '0xe29a55a6aeff5c8b1beede5bcf2f0cb3af8f91f5'

BSC_VENUS_ADDRESS = '0xfd36e2c2a6789db23113685031d7f16329158384'
BSC_CREAM_ADDRESS = '0x589de0f0ccf905477646599bb3e5c622c84cc0ba'

FTM_GEIST_ADDRESS = '0x9fad24f572045c7869117160a571b2e50b10d068'

ETH_COMPOUND_ADDRESS = '0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b'
ETH_CREAM_ADDRESS = ''


class Chains:
    bsc = '0x38'
    ethereum = '0x1'
    fantom = '0xfa'
    polygon = '0x89'
    all = [bsc, ethereum, fantom, polygon]
    mapping = {
        'bsc': bsc,
        'ethereum': ethereum,
        'fantom': fantom,
        'polygon': polygon
    }

    names = {
        bsc: 'bsc',
        ethereum: "ethereum",
        fantom: 'fantom',
        polygon: 'polygon'
    }

    abi_mapping = {
        bsc: 'bep20_abi',
        ethereum: 'erc20_abi',
        fantom: 'erc20_abi',
        polygon: 'erc20_abi'
    }

    block_time = {
        bsc: 3,
        ethereum: 12,
        fantom: 1,
        polygon: 2
    }

    def get_abi_name(self, chain_id):
        """Map chain_id with the corresponding abi key
        e.g.: '0x38' -> 'bep20_abi' or '0x1' -> 'erc20_abi'
        """
        return self.abi_mapping.get(chain_id, '')


class ChainsAnkr:
    """Chain name for Ankr API"""
    bsc = '0x38'
    eth = '0x1'
    fantom = '0xfa'
    polygon = '0x89'

    reversed_mapping = {
        bsc: 'bsc',
        eth: 'eth',
        fantom: 'fantom',
        polygon: 'polygon',
    }

    def get_chain_name(self, chain_id):
        return self.reversed_mapping.get(chain_id)


class Scans:
    mapping = {
        'bscscan': Chains.bsc,
        'etherscan': Chains.ethereum,
        'ftmscan': Chains.fantom,
        'polygonscan': Chains.polygon
    }


class Networks:
    bsc = 'bsc'
    ethereum = 'ethereum'
    fantom = 'fantom'
    polygon = 'polygon'

    providers = {
        bsc: 'https://bsc-dataseed1.binance.org/',
        ethereum: 'https://little-thrumming-butterfly.discover.quiknode.pro/aa4eaa63428402a759d52cb6d33c138698031e31/',
        fantom: 'https://rpc.ftm.tools/',
        polygon: 'https://polygon-rpc.com'
    }

    archive_node = {
        bsc: 'https://rpc.ankr.com/bsc',
        ethereum: 'https://rpc.ankr.com/eth',
        fantom: 'https://rpc.ankr.com/fantom',
        polygon: 'https://rpc.ankr.com/polygon'
    }


NATIVE_TOKEN = '0x0000000000000000000000000000000000000000'

NATIVE_TOKENS = {
    Chains.bsc: '0x0000000000000000000000000000000000000000',
    Chains.ethereum: '0x0000000000000000000000000000000000000000',
    Chains.fantom: '0x0000000000000000000000000000000000000000',
    Chains.polygon: '0x0000000000000000000000000000000000000000'
}
