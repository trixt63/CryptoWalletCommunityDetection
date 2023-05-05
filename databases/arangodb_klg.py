import sys

from arango import ArangoClient
from arango.database import StandardDatabase
from arango.http import DefaultHTTPClient

from config import ArangoDBConfig
from constants.arangodb_constants import ArangoDBCollections, KnowledgeGraphModel, ArangoDBIndex
from constants.time_constants import TimeConstants
from utils.logger_utils import get_logger
from utils.parser import get_connection_elements
from utils.retry_handler import retry_handler

logger = get_logger('ArangoDB')


class ArangoDB:
    def __init__(self, connection_url=None, database=ArangoDBConfig.DATABASE):
        if not connection_url:
            connection_url = ArangoDBConfig.CONNECTION_URL
        username, password, connection_url = get_connection_elements(connection_url)

        http_client = DefaultHTTPClient()
        http_client.REQUEST_TIMEOUT = 1000

        try:
            self.connection_url = connection_url
            self.client = ArangoClient(hosts=connection_url, http_client=http_client)
        except Exception as e:
            logger.exception(f"Failed to connect to ArangoDB: {connection_url}: {e}")
            sys.exit(1)

        self._db = self._get_db(database, username, password)

        self._wallets_col = self._get_collections(ArangoDBCollections.wallets)
        self._multichain_wallets_col = self._get_collections(ArangoDBCollections.multichain_wallets)
        self._projects_col = self._get_collections(ArangoDBCollections.projects)
        self._smart_contracts_col = self._get_collections(ArangoDBCollections.smart_contracts)
        self._relationships_col = self._get_collections(ArangoDBCollections.relationships, edge=True)
        self._call_smart_contracts_col = self._get_collections(ArangoDBCollections.call_smart_contracts, edge=True)

        self._abi_col = self._get_collections(ArangoDBCollections.abi)
        self._configs_col = self._get_collections(ArangoDBCollections.configs)
        self._is_part_ofs_col = self._get_collections(ArangoDBCollections.is_part_ofs, edge=True)

    def _get_db(self, db_name, username, password):
        return self.client.db(db_name, username=username, password=password)

    def _get_graph(self, graph_name, edge_definitions=KnowledgeGraphModel.edge_definitions,
                   database: StandardDatabase = None):
        if not database:
            database = self._db
        if not database.has_graph(graph_name):
            database.create_graph(graph_name, edge_definitions=edge_definitions)
        return database.graph(graph_name)

    def _get_collections(self, collection_name, database: StandardDatabase = None, edge=False):
        if not database:
            database = self._db
        if not database.has_collection(collection_name):
            database.create_collection(collection_name, shard_count=20, edge=edge)
        return database.collection(collection_name)

    def get_wallet_addresses_and_lendings(self, chain_id: str, timestamp: int=None, batch_size=1000):
        try:
            if timestamp:
                query = f"""
                    for w in wallets
                    filter w.chainId == '{chain_id}' and
                    w.depositInUSD > 0 or w.borrowInUSD > 0 
                    and w.lastUpdatedAt > {timestamp}
                    return {{
                        'address': w.address,
                        'lendings': w.lendings
                    }}
                """
            else:
                query = f"""
                    for w in wallets
                    filter w.chainId == '{chain_id}' and
                    w.depositInUSD > 0 or w.borrowInUSD > 0 
                    return {{
                        'address': w.address,
                        'lendings': w.lendings
                    }}
                """
            cursor = self._db.aql.execute(query, batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None
