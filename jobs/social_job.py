import time
from typing import List
from cli_scheduler.scheduler_job import SchedulerJob

from databases.mongodb import MongoDB
from utils.logger_utils import get_logger

logger = get_logger('Social Job')


def main():
    # vi_dac_biet = '0x95106a95922a179f4b951e8c886952498afe8f19'
    mongodb = MongoDB()
    # mongodb.update_cex_users()


if __name__ == "__main__":
    main()
