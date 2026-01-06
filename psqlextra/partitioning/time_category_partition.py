from datetime import datetime
from typing import Optional

from .error import PostgresPartitioningError
from .range_partition import PostgresRangePartition
from .time_partition import PostgresTimePartition
from .time_partition_size import (
    PostgresTimePartitionSize,
    PostgresTimePartitionUnit,
)


class PostgresTimeCategoryPartition(PostgresRangePartition):
    """Time-based and category range table partition.

    :see:PostgresTimeandCategoryPartitioningStrategy for more info.
    """


    def __init__(
        self,
        size: PostgresTimePartitionSize,
        start_datetime: datetime,
        category: int,
        name_format: Optional[str] = None,
    ) -> None:
        end_datetime = start_datetime + size.as_delta()
        lower_category_limit = category
        upper_category_limit = category + 1   

        super().__init__(
            from_values=[start_datetime.strftime("%Y-%m-%d %H:00:00"), lower_category_limit],
            to_values=[end_datetime.strftime("%Y-%m-%d %H:00:00"), upper_category_limit],
        )

        self.size = size
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.name_format = name_format
        self.lower_category_limit = lower_category_limit

    def name(self) -> str:
        name_format = self.name_format or (PostgresTimePartition._unit_name_format.get(
            self.size.unit
        ) + "_{category}")  
        if not name_format:
            raise PostgresPartitioningError("Unknown size/unit")

        return self.start_datetime.strftime(name_format).format(category=self.lower_category_limit).lower()

    def deconstruct(self) -> dict:
        return {
            **super().deconstruct(),
            "size_unit": self.size.unit.value,
            "size_value": self.size.value,
            "category": self.lower_category_limit
        }


__all__ = ["PostgresTimeCategoryPartition"]
