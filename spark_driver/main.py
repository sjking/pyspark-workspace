import logging

from spark_driver.util import get_spark_context
from spark_driver.jobs.sum_n import sum_n_numbers
from spark_driver.jobs.example1 import example1
from spark_driver.jobs.rdd import run_rdd_example
from spark_driver.jobs.movies import count_movie_sequel
from spark_driver.jobs.data_frames import gone_with_the_wind
from spark_driver.jobs.spark_sql import query_example
from spark_driver.jobs.joins import joins
from spark_driver.jobs.structured_streaming_quick import quick_example

logger = logging.getLogger(__name__)

jobs = [
    (sum_n_numbers, (100000, )),
    (example1, None),
    (run_rdd_example, None),
    (count_movie_sequel, None),
    (gone_with_the_wind, None),
    (query_example, None),
    (joins, None),
    (quick_example, None)
]


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    session = get_spark_context("SequelMoviesCount")

    for fn, args in jobs:
        if args is not None:
            fn(session, *args)
        else:
            fn(session)

    session.stop()
