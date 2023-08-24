import json

from multithread_processing.base_job import BaseJob

from databases.mongodb import MongoDB
from models.blocks import Blocks
from utils.logger_utils import get_logger

logger = get_logger('Wallets Grouping job')

db = MongoDB()


def get_n_n():
    data = db.get_groups_by_num_wallets(chain_id='0x38',
                                        num_user_cond={'$gt': 1, '$lte': 4},
                                        num_depo_cond=1)

    for group in data:



if __name__ == '__main__':
    get_n_n()
