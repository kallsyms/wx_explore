from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table import (
    Entity,
    EntityProperty,
    EdmType,
    TableBatch,
)
from typing import Dict, Tuple, List

import array
import concurrent.futures
import datetime
import logging
import numpy
import zlib

from . import DataProvider
from wx_explore.common.models import (
    Projection,
    SourceField,
    DataPointSet,
)
from wx_explore.common.utils import chunk


class AzureTableBackend(DataProvider):
    """
    ATS constraints:
        * entity (row) size <= 1mb
        * up to 255 props per row (incl. partitionkey, rowkey, timestamp)
        * each prop <= 64k

    We use the following:
        * pk is (proj_id, y)
        * row is (valid_time, run_time, x_shard)
        * properties are "sf{n}" -> zlib'd bytearr value for each x in the shard

    This means:
        * Location queries are always on a single partition
        * Valid time based filtering (less than, greater than) via lexicographical compares on row
        * Minimal amount of data sent back since any given row in storage is only n_x_per_row dwords
        * X sharding also means we can store ensemble results in a single property (TODO)
    """

    logger: logging.Logger
    account_name: str
    account_key: str
    table_name: str
    n_x_per_row: int = 128

    def __init__(self, account_name, account_key, table_name):
        logging.getLogger('azure').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.ERROR)

        self.logger = logging.getLogger(self.__class__.__name__)
        self.account_name = account_name
        self.account_key = account_key
        self.table_name = table_name

    def get_fields(
            self,
            proj_id: int,
            loc: Tuple[float, float],
            valid_source_fields: List[SourceField],
            start: datetime.datetime,
            end: datetime.datetime
    ) -> List[DataPointSet]:
        start = start.replace(microsecond=0)
        end = end.replace(microsecond=0)

        parallel_hours = 3

        times = [start]
        while times[-1] + datetime.timedelta(hours=parallel_hours) < end:
            times.append(times[-1] + datetime.timedelta(hours=parallel_hours))
        times.append(end)

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(times)-1) as ex:
            return sum(
                ex.map(
                    lambda time_range: self._get_fields_worker(proj_id, loc, valid_source_fields, *time_range),
                    zip(times[:-1], times[1:]),
                ),
                [],
            )

    def _get_fields_worker(
            self,
            proj_id: int,
            loc: Tuple[float, float],
            valid_source_fields: List[SourceField],
            start: datetime.datetime,
            end: datetime.datetime
    ) -> List[DataPointSet]:
        x, y = loc
        partition = f"{proj_id}-{y}"

        nearest_row_x = ((x // self.n_x_per_row) * self.n_x_per_row)
        rel_x = x - nearest_row_x

        # This actually (ab)uses lexicographical string compares on the rowkey
        row_start = start.isoformat()
        row_end = end.isoformat()

        az_filter = f"PartitionKey eq '{partition}' and RowKey gt '{row_start}' and RowKey lt '{row_end}' and XShard eq {nearest_row_x}"
        select = ['PartitionKey', 'RowKey', 'ValidTime', 'RunTime', *(f"sf{sf.id}" for sf in valid_source_fields)]

        data_points = []

        for row in TableService(self.account_name, self.account_key).query_entities(self.table_name, az_filter, ','.join(select)):
            for sf in valid_source_fields:
                key = f"sf{sf.id}"
                if key not in row or row[key] is None:
                    continue

                raw = zlib.decompress(row[key].value)
                val = array.array("f", raw).tolist()[rel_x]

                data_point = DataPointSet(
                    values=[val],
                    metric_id=sf.metric.id,
                    valid_time=row.ValidTime,
                    source_field_id=sf.id,
                    run_time=row.RunTime,
                )

                data_points.append(data_point)

        return data_points

    def put_fields(
            self,
            proj: Projection,
            fields: Dict[Tuple[int, datetime.datetime, datetime.datetime], List[numpy.array]]
    ):
        # fields is map of (field_id, valid_time, run_time) -> [msg, ...]
        with concurrent.futures.ThreadPoolExecutor() as ex:
            ex.map(lambda y: self._put_fields_worker(proj, fields, y), range(proj.n_y))

    def _put_fields_worker(self, proj: Projection, fields: Dict[Tuple[int, datetime.datetime, datetime.datetime], List[numpy.array]], y: int):
        partition = f"{proj.id}-{y}"
        rows: Dict[Tuple[datetime.datetime, datetime.datetime, int], Dict[str, EntityProperty]] = {}

        for (field_id, valid_time, run_time), msgs in fields.items():
            for x in range(0, proj.n_x, self.n_x_per_row):
                row_key = (valid_time, run_time, x)

                if row_key not in rows:
                    rows[row_key] = {}

                for msg in msgs:
                    # XXX: this only keeps last msg per field breaking ensembles
                    rows[row_key][f"sf{field_id}"] = EntityProperty(EdmType.BINARY, zlib.compress(msg[y][x:x+self.n_x_per_row].tobytes()))

        for row_chunk in chunk(rows.items(), 100):
            with TableService(self.account_name, self.account_key).batch(self.table_name) as batch:
                for row_key, row in row_chunk:
                    valid_time, run_time, x = row_key
                    batch.insert_entity({
                        'PartitionKey': partition,
                        'RowKey': f"{valid_time.isoformat()},{run_time.isoformat()},{x}",
                        'XShard': x,
                        # These 2 are needed for reading (reconstructing a DataPointSet).
                        # Technically redundant (both are already in row key) but it makes reading a bit cleaner
                        # and storage overhead is minimal.
                        'ValidTime': valid_time,
                        'RunTime': run_time,
                        **row,
                    })

    def clean(self, oldest_time: datetime.datetime):
        earliest = oldest_time.replace(microsecond=0)

        for proj in Projection.query.all():
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as ex:
                ex.map(lambda y: self._clean_worker(earliest, proj, y), range(proj.n_y))

    def _clean_worker(self, earliest: datetime.datetime, proj: Projection, y: int):
        svc = TableService(self.account_name, self.account_key)
        to_delete = []

        for row in svc.query_entities(self.table_name, f"PartitionKey eq '{proj.id}-{y}' and RowKey lt '{earliest.isoformat()}'", 'PartitionKey,RowKey'):
            to_delete.append((row.PartitionKey, row.RowKey))

        for batch_elems in chunk(to_delete, 100):
            with svc.batch(self.table_name) as batch:
                for entity in batch_elems:
                    batch.delete_entity(*entity)

    def merge(self):
        pass
