import logging

from spark_driver.util import get_spark_context
from spark_driver.jobs.sum_n import sum_n_numbers


logger = logging.getLogger(__name__)

jobs = [
    (sum_n_numbers, (100000, ))
]


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    session = get_spark_context("example")

    for fn, args in jobs:
        fn(session, *args)

    session.stop()
