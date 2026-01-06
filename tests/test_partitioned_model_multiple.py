import pytest

from django.db import models



import freezegun
import pytest

from django.db import connection, models

from psqlextra.partitioning import (
    PostgresPartitioningManager,
    partition_by_current_time,
    partition_by_current_time_and_categories
)

from . import db_introspection
from .fake_model import define_fake_partitioned_model


def _get_partitioned_table(model):
    return db_introspection.get_partitioned_table(model._meta.db_table)


def test_partitioned_model_multiple_keys():
    """Tests whether defining a partitioned model with multiple keys works
    as expected."""

    model = define_fake_partitioned_model(
        partitioning_options=dict(key=["date", "category_id"]),
        fields={
            "date": models.DateTimeField(),
            "category_id": models.IntegerField(),
        },
    )

    assert model._partitioning_meta.key == ["date", "category_id"]



@pytest.mark.postgres_version(lt=110000)
def test_partitioning_multiple_time_yearly_apply():
    """Tests whether automatically creating new partitions ahead yearly works
    as expected for multiple keys."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField(), "category_id": models.IntegerField()}, {"key": ["timestamp","category_id"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    with freezegun.freeze_time("2019-1-1"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time_and_categories(model, years=1, count=2, categories=[1,2])]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 4
    assert table.partitions[0].name == "2019_1"
    assert table.partitions[1].name == "2020_1"
    assert table.partitions[2].name == "2019_2"
    assert table.partitions[3].name == "2020_2"

    with freezegun.freeze_time("2019-12-30"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time_and_categories(model, years=1, count=3, categories=[1,2,100])]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 9
    assert table.partitions[0].name == "2019_1"
    assert table.partitions[1].name == "2020_1"
    assert table.partitions[2].name == "2021_1"
    assert table.partitions[3].name == "2019_2"
    assert table.partitions[4].name == "2020_2"
    assert table.partitions[5].name == "2021_2"
    assert table.partitions[6].name == "2019_100"
    assert table.partitions[7].name == "2020_100"
    assert table.partitions[8].name == "2021_100"