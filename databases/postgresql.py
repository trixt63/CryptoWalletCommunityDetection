from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import sessionmaker

from config import PostgresDBConfig
from constants.network_constants import NATIVE_TOKEN
from constants.postgres_constants import TransferEvent, WALLET_TABLES, TF_EVENTS_VIEW, AmountInOut
from utils.logger_utils import get_logger
from utils.time_execute_decorator import TimeExeTag, sync_log_time_exe

logger = get_logger('PostgreSQL')


class PostgresDB:
    def __init__(self, connection_url: str = None):
        # Set up the database connection and create the table
        if not connection_url:
            connection_url = PostgresDBConfig.CONNECTION_URL
        self.engine = create_engine(connection_url)
        self.session = sessionmaker(bind=self.engine)()

    def close(self):
        self.session.close()
        pass

    # @sync_log_time_exe(tag=TimeExeTag.database)
    def get_event_transfer_by_to_addresses(self, to_addresses, from_block, to_block):
        query = f"""
            SELECT from_address 
            FROM {PostgresDBConfig.SCHEMA}.{PostgresDBConfig.TRANSFER_EVENT_TABLE}
            WHERE to_address = ANY (ARRAY{to_addresses})
            AND block_number BETWEEN {from_block} AND {to_block}
            GROUP BY from_address
        """
        event_transfer = self.session.execute(query).all()
        # self.session.commit()
        return event_transfer
