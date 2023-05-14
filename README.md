# CryptoWalletCommunityDetection
My would-be graduation project

## Pipelines
### 1. Lending wallets
- Get wallets with lending activities.
- Data source: Mongo Entity
### 2. Exchange Deposit wallets:
- Wallets that deposit into hot wallets of centralized exchange platforms. 
List of platforms is in ```artifacts/centralized_exchange_addresses```
- Data source: history of transfer events & transactions
### 3. LP traders
- Wallets that trade LP tokens belong to the follow swap platforms:
  - PancakeSwap from chain BSC
  - SpookySwap from chain Fantom
  - Uniswap V2 from chain Ethereum