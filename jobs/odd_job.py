import os
import sys
sys.path.append(os.path.dirname(sys.path[0]))

from databases.mongodb import MongoDB
from utils.logger_utils import get_logger

logger = get_logger('Odd job')


def main():
    # vi_dac_biet = '0x95106a95922a179f4b951e8c886952498afe8f19'
    mongodb = MongoDB()
    mongodb.migrate_deposit_wallets()


if __name__ == "__main__":
    main()
