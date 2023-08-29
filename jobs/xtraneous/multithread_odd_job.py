import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(sys.path[0])))

from databases.mongodb import MongoDB
from utils.logger_utils import get_logger
from multithread_processing.base_job import BaseJob

logger = get_logger('Odd job')


class MultithreadOddJob(BaseJob):
    def __init__(self):
        self.mongodb = MongoDB()
        self.pagination = 100000
        self._first_block = int(self.mongodb.get_min(col_name='transferEvents', field_name='block_number'))
        self._last_block = int(self.mongodb.get_max(col_name='transferEvents', field_name='block_number'))

        super().__init__(
            work_iterable=range(self._first_block, self._last_block),
            max_workers=8,
            batch_size=self.pagination
        )

    def _execute_batch(self, works):
        # self.mongodb.fix_transfer_events(str(works[0]), str(works[-1]))
        logger.info(f"Add chainId for deposit wallet {works[0]} to {works[-1]}")


def main():
    job = MultithreadOddJob()
    job.run()


if __name__ == "__main__":
    main()
