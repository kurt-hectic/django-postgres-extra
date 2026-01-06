from datetime import datetime, timezone
from typing import Generator, Optional

from dateutil.relativedelta import relativedelta

from .range_strategy import PostgresRangePartitioningStrategy
from .time_category_partition import PostgresTimeCategoryPartition
from .time_partition_size import PostgresTimePartitionSize


class PostgresCurrentTimeAndCategoryPartitioningStrategy(
    PostgresRangePartitioningStrategy
):
    """Implments a time and category based partitioning strategy where each partition
    contains values for a specific time period for all of the categories.

    All buckets will be equal in size and start at the start of the
    unit. With monthly partitioning, partitions start on the 1st and
    with weekly partitioning, partitions start on monday, with hourly
    partitioning, partitions start at 00:00.
    There is one partition per category for each time period.
    """

    def __init__(
        self,
        size: PostgresTimePartitionSize,
        count: int,
        max_age: Optional[relativedelta] = None,
        categories: Optional[list] = [],
        name_format: Optional[str] = None,
    ) -> None:
        """Initializes a new instance of :see:PostgresTimePartitioningStrategy.

        Arguments:
            size:
                The size of each partition.

            count:
                The amount of partitions to create ahead
                from the current date/time.

            categories:
                List of categories. List items must be convertible to string.

            max_age:
                Maximum age of a partition. Partitions
                older than this are deleted during
                auto cleanup.

        """

        self.size = size
        self.count = count
        self.max_age = max_age
        self.categories = categories
        self.name_format = name_format

    def to_create(self) -> Generator[PostgresTimeCategoryPartition, None, None]:
        current_datetime = self.size.start(self.get_start_datetime())

        for category in self.categories:
            current_datetime_loop = current_datetime
            for _ in range(self.count):
                yield PostgresTimeCategoryPartition(
                    start_datetime=current_datetime_loop,
                    size=self.size,
                    category=category,
                    name_format=self.name_format,
                )

                current_datetime_loop += self.size.as_delta()

    def to_delete(self) -> Generator[PostgresTimeCategoryPartition, None, None]:
        if not self.max_age:
            return

        current_datetime = self.size.start(
            self.get_start_datetime() - self.max_age
        )

        while True:
            for category in self.categories:
                yield PostgresTimeCategoryPartition(
                    start_datetime=current_datetime,
                    size=self.size,
                    category=category,
                    name_format=self.name_format,
                )

            current_datetime -= self.size.as_delta()

    def get_start_datetime(self) -> datetime:
        return datetime.now(timezone.utc)
