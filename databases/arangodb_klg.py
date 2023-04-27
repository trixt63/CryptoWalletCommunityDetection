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

    def _create_index(self):
        # self._smart_contracts_col.add_persistent_index(fields=['tags'], name=ArangoDBIndex.smart_contract_tags)
        self._smart_contracts_col.add_hash_index(fields=['address', 'chainId'],
                                                 name=ArangoDBIndex.smart_contract_address)
        self._smart_contracts_col.add_hash_index(fields=['chainId'], name=ArangoDBIndex.smart_contract_chain)
        self._relationships_col.add_hash_index(fields=['type'], name=ArangoDBIndex.relationship_type)

        self._call_smart_contracts_col.add_ttl_index(fields=['lastCalledAt'], expiry_time=TimeConstants.DAYS_30,
                                                     name=ArangoDBIndex.relationship_ttl)

    def get_tokens(self, projection=None):
        try:
            projection_statement = self.get_projection_statement(projection)

            query = f"""
                FOR doc IN {ArangoDBCollections.smart_contracts}
                FILTER doc.idCoingecko
                RETURN {projection_statement}
            """
            cursor = self._db.aql.execute(query, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_price(self, token, chain_id):
        key = f"{chain_id}_{token}"
        try:
            query = f"""
                FOR doc IN {ArangoDBCollections.smart_contracts}
                FILTER doc._key=='{key}'
                limit 1
                RETURN doc.price
            """
            cursor = list(self._db.aql.execute(query, batch_size=1000))
            if cursor:
                return cursor[0]
            else:
                return 0
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_all_contract(self, chain_id, limit=1000):
        query = f"""
            FOR doc IN {ArangoDBCollections.smart_contracts}
            FILTER doc.chainId == '{chain_id}'
            FILTER NOT doc.checkTag
            LIMIT {limit}
            RETURN {{
                address: doc.address,
                chainId: doc.chainId,
                tags: doc.tags
            }}
        """
        cursor = self._db.aql.execute(query, batch_size=1000)
        return cursor

    def get_new_contracts(self, chain_id: str = None):
        try:
            filter_ = 'FILTER doc.isNew == true'
            if chain_id is not None:
                filter_ += f' AND doc.chainId == "{chain_id}"'

            query = f"""
                FOR doc IN {ArangoDBCollections.smart_contracts}
                {filter_}
                RETURN doc.address
            """
            cursor = self._db.aql.execute(query, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_top_tokens(self, chain_id: str, limit=200):
        try:
            query = f"""
                FOR s IN {ArangoDBCollections.smart_contracts}
                FILTER s.tags.token==1 and s.chainId=='{chain_id}'
                sort s.marketCap desc
                limit {limit}
                return s
            """
            cursor = self._db.aql.execute(query, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_lending_wallet_addresses(self, chain_id: str):
        try:
            query = f"""
                for w in wallets
                filter w.chainId == {chain_id}
                w.depositInUSD > 0 or w.borrowInUSD > 0 
                return w.address
            """
            cursor = self._db.aql.execute(query, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None
