from utils.file_utils import read_json
import json

BASE_PATH = "artifacts/abis/dex"


class PancakeSwapConstants:
    NAME = 'pancakeswap'
    CHAIN_ID = '0x38'
    FACTORY = '0xca143ce32fe78f1f7019d7d551a6402fc5350c73'
    FACTORY_ABI = read_json(f"{BASE_PATH}/pancake/pancakeFactory.json")
    LP_ABI = read_json("artifacts/abis/dex/pancake/lpToken.json")
    CREATION_BLOCK = 6809737
    CREATION_TX_HASH = "0x88fd2f3da662306e24c1694c1ca8042a2577cbf50958114405cce69bce4de9e3"


class SpookySwapConstants:
    NAME = 'spookyswap'
    CHAIN_ID = '0xfa'
    FACTORY = '0x152ee697f2e276fa89e96742e9bb9ab1f2e61be3'
    FACTORY_ABI = read_json(f"{BASE_PATH}/spooky/pancakeFactory.json")
    LP_ABI = read_json(f"{BASE_PATH}/spooky/lpToken.json")
    CREATION_BLOCK = 3795376
    CREATION_TX_HASH = "0x40908255286d70c76af9b3834efe218df2f64d9ad3749ff46eef63677198dd66"


class UniswapV2Constants:
    NAME = 'uniswap'
    CHAIN_ID = '0x1'
    FACTORY = '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f'
    FACTORY_ABI = read_json(f"{BASE_PATH}/uniswap_v2/uniswapv2Factory.json")
    LP_ABI = read_json(f"{BASE_PATH}/uniswap_v2/lpToken.json")
    CREATION_BLOCK = 10000835
    CREATION_TX_HASH = "0xc31d7e7e85cab1d38ce1b8ac17e821ccd47dbde00f9d57f2bd8613bff9428396"


lp_factory_mapping = {
    '0xca143ce32fe78f1f7019d7d551a6402fc5350c73': {
        'name': PancakeSwapConstants.NAME,
        'chain_id': PancakeSwapConstants.CHAIN_ID,
        'factory_abi': PancakeSwapConstants.FACTORY_ABI,
        'lp_abi': PancakeSwapConstants.LP_ABI,
        'creation_block': PancakeSwapConstants.CREATION_BLOCK,
        'creation_tx_hash': PancakeSwapConstants.CREATION_TX_HASH
    },
    '0x152ee697f2e276fa89e96742e9bb9ab1f2e61be3': {
        'name': SpookySwapConstants.NAME,
        'chain_id': SpookySwapConstants.CHAIN_ID,
        'factory_abi': SpookySwapConstants.FACTORY_ABI,
        'lp_abi': SpookySwapConstants.LP_ABI,
        'creation_block': SpookySwapConstants.CREATION_BLOCK,
        'creation_tx_hash': SpookySwapConstants.CREATION_TX_HASH
    },
    '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f': {
        'name': UniswapV2Constants.NAME,
        'chain_id': UniswapV2Constants.CHAIN_ID,
        'factory_abi': UniswapV2Constants.FACTORY_ABI,
        'lp_abi': UniswapV2Constants.LP_ABI,
        'creation_block': UniswapV2Constants.CREATION_BLOCK,
        'creation_tx_hash': UniswapV2Constants.CREATION_TX_HASH
    }
}
