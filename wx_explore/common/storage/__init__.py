from typing import Tuple, Optional, Iterable, Dict, List

import concurrent.futures
import datetime
import numpy

from wx_explore.common import tracing
from wx_explore.common.config import Config
from wx_explore.common.location import get_xy_for_coord
from wx_explore.common.models import (
    SourceField,
    Projection,
    DataPointSet,
)


class DataProvider(object):
    def get_fields(
            self,
            proj_id: int,
            loc: Tuple[float, float],
            valid_source_fields: List[SourceField],
            start: datetime.datetime,
            end: datetime.datetime
    ) -> List[DataPointSet]:
        raise NotImplementedError()

    def put_fields(
            self,
            proj: Projection,
            fields: Dict[Tuple[int, datetime.datetime, datetime.datetime], List[numpy.array]]
    ):
        raise NotImplementedError()

    def clean(self, oldest_time: datetime.datetime):
        raise NotImplementedError()

    def merge(self):
        raise NotImplementedError()


def get_provider():
    from .s3 import S3Backend
    from .azure_tables import AzureTableBackend

    if Config.DATA_PROVIDER == "S3":
        return S3Backend(
            Config.INGEST_S3_ACCESS_KEY,
            Config.INGEST_S3_SECRET_KEY,
            Config.INGEST_S3_REGION,
            Config.INGEST_S3_BUCKET,
            Config.INGEST_S3_ENDPOINT,
        )
    elif Config.DATA_PROVIDER == "AZURE_TABLES":
        return AzureTableBackend(
            Config.INGEST_AZURE_TABLE_ACCOUNT_NAME,
            Config.INGEST_AZURE_TABLE_ACCOUNT_KEY,
            Config.INGEST_AZURE_TABLE_NAME,
        )


def load_data_points(
        coords: Tuple[float, float],
        start: datetime.datetime,
        end: datetime.datetime,
        source_fields: Optional[Iterable[SourceField]] = None
) -> List[DataPointSet]:

    if source_fields is None:
        source_fields = SourceField.query.all()

    # Determine all valid source fields (fields in source_fields which cover the given coords),
    # and the x,y for projection used in any valid source field.
    valid_source_fields = []
    locs: Dict[int, Tuple[float, float]] = {}
    for sf in source_fields:
        if sf.projection_id in locs and locs[sf.projection_id] is None:
            continue

        if sf.projection_id not in locs:
            with tracing.start_span("get_xy_for_coord") as span:
                span.set_attribute("projection_id", sf.projection_id)
                loc = get_xy_for_coord(sf.projection, coords)

            # Skip if given projection does not cover coords
            if loc is None:
                continue

            locs[sf.projection_id] = loc

        valid_source_fields.append(sf)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(locs)) as ex:
        data_points: List[DataPointSet] = sum(
            ex.map(
                lambda proj_loc: get_provider().get_fields(*proj_loc, valid_source_fields, start, end),
                locs.items()
            ),
            [],
        )

    return data_points
