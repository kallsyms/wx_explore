from typing import Dict, Tuple, List, Any

import array
import concurrent.futures
import datetime
import logging
import numpy
import pymongo
import pytz
import zlib

from . import DataProvider
from wx_explore.common import tracing
from wx_explore.common.models import (
    Projection,
    SourceField,
    DataPointSet,
)


class MongoBackend(DataProvider):
    logger: logging.Logger
    account_name: str
    account_key: str
    table_name: str
    n_x_per_row: int = 128

    def __init__(self, uri: str, database: str, collection: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.collection = pymongo.MongoClient(uri)[database][collection]
        self.collection.create_index([
            ('proj_id', pymongo.ASCENDING),
            ('valid_time', pymongo.ASCENDING),
            ('y', pymongo.ASCENDING),
        ])

    def get_fields(
            self,
            proj_id: int,
            loc: Tuple[float, float],
            valid_source_fields: List[SourceField],
            start: datetime.datetime,
            end: datetime.datetime
    ) -> List[DataPointSet]:
        x, y = loc

        nearest_row_x = ((x // self.n_x_per_row) * self.n_x_per_row)
        rel_x = x - nearest_row_x

        with tracing.start_span('get_fields lookup'):
            results = self.collection.find({
                'proj_id': proj_id,
                'y': y,
                'x_shard': nearest_row_x,
                'valid_time': {
                    '$gte': start,
                    '$lt': end,
                },
            })

        data_points = []

        for item in results:
            for sf in valid_source_fields:
                key = f"sf{sf.id}"
                if key not in item or item[key] is None:
                    continue

                raw = zlib.decompress(item[key])
                val = array.array("f", raw).tolist()[rel_x]

                data_point = DataPointSet(
                    values=[val],
                    metric_id=sf.metric.id,
                    valid_time=item['valid_time'].replace(tzinfo=pytz.UTC),
                    source_field_id=sf.id,
                    run_time=item['run_time'].replace(tzinfo=pytz.UTC),
                )

                data_points.append(data_point)

        return data_points

    def put_fields(
            self,
            proj: Projection,
            fields: Dict[Tuple[int, datetime.datetime, datetime.datetime], List[numpy.array]]
    ):
        # fields is map of (field_id, valid_time, run_time) -> [msg, ...]
        with concurrent.futures.ThreadPoolExecutor(1) as ex:
            ex.map(lambda y: self._put_fields_worker(proj, fields, y), range(proj.n_y))

    def _put_fields_worker(
            self,
            proj: Projection,
            fields: Dict[Tuple[int, datetime.datetime, datetime.datetime], List[numpy.array]],
            y: int
    ):
        rows: Dict[Tuple[datetime.datetime, datetime.datetime, int], Dict[str, Any]] = {}

        with tracing.start_span('put_fields transformations') as span:
            span.set_attribute("num_fields", len(fields))

            for (field_id, valid_time, run_time), msgs in fields.items():
                for x in range(0, proj.n_x, self.n_x_per_row):
                    row_key = (valid_time, run_time, x)

                    if row_key not in rows:
                        rows[row_key] = {
                            'proj_id': proj.id,
                            'valid_time': valid_time,
                            'run_time': run_time,
                            'y': y,
                            'x_shard': x,
                        }

                    for msg in msgs:
                        # XXX: this only keeps last msg per field breaking ensembles
                        rows[row_key][f"sf{field_id}"] = zlib.compress(msg[y][x:x+self.n_x_per_row].astype(numpy.float32).tobytes())

        with tracing.start_span('put_fields saving') as span:
            self.collection.insert_many(rows.values())

    def clean(self, oldest_time: datetime.datetime):
        for proj in Projection.query.all():
            self.collection.remove({
                'proj_id': proj.id,
                'valid_time': {
                    '$lt': oldest_time,
                },
            })

    def merge(self):
        pass
