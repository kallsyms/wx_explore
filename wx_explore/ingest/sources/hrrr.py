#!/usr/bin/env python3
from datetime import datetime, timedelta
from typing import Optional
import logging

from wx_explore.analysis.transformations import cartesian_to_polar
from wx_explore.common import metrics
from wx_explore.common.models import (
    Source,
    SourceField,
)
from wx_explore.common.utils import datetime2unix
from wx_explore.ingest.common import get_queue, get_or_create_projection, create_files
from wx_explore.ingest.sources.source import IngestSource
from wx_explore.web.core import db

logger = logging.getLogger(__name__)


class HRRR(IngestSource):
    SOURCE_NAME = "hrrr"

    @staticmethod
    def generate_derived(grib):
        logger.info("Deriving wind")

        speed_sf = SourceField.query.filter(
            SourceField.source.has(Source.short_name == HRRR.SOURCE_NAME),
            SourceField.metric == metrics.wind_speed,
        ).first()
        direction_sf = SourceField.query.filter(
            SourceField.source.has(Source.short_name == HRRR.SOURCE_NAME),
            SourceField.metric == metrics.wind_direction,
        ).first()

        if speed_sf is None or direction_sf is None:
            raise Exception("Unable to load wind speed and/or direction source fields")

        uv_pairs = zip(
            sorted(grib.select(name='10 metre U wind component'), key=lambda m: (m.validDate, m.analDate)),
            sorted(grib.select(name='10 metre V wind component'), key=lambda m: (m.validDate, m.analDate)),
        )

        for u, v in uv_pairs:
            speed, direction = cartesian_to_polar(u.values, v.values)

            msg = u  # or v - this only matters for projection, valid/analysis dates, etc.
            projection = get_or_create_projection(msg)

            if speed_sf.projection is None or speed_sf.projection.params != msg.projparams:
                speed_sf.projection_id = projection.id

            if direction_sf.projection is None or direction_sf.projection.params != msg.projparams:
                direction_sf.projection_id = projection.id

            db.session.commit()

            create_files(projection.id, {
                (speed_sf.id, msg.validDate, msg.analDate): [speed],
                (direction_sf.id, msg.validDate, msg.analDate): [direction],
            })

    @staticmethod
    def queue(
            time_min: int = 0,
            time_max: int = 18,
            run_time: Optional[datetime] = None,
            acquire_time: Optional[datetime] = None
    ):
        if run_time is None:
            # hrrr is run each hour
            run_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0)

        if acquire_time is None:
            # first files are available about 45 mins after
            acquire_time = run_time
            acquire_time += timedelta(minutes=45)

        base_url = run_time.strftime("https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/hrrr.%Y%m%d/conus/hrrr.t%Hz.wrfsubhf{}.grib2")

        urls = [base_url.format(str(x).zfill(2)) for x in range(time_min, time_max + 1)]

        q = get_queue()
        for url in urls:
            q.put({
                "source": "hrrr",
                "run_time": datetime2unix(run_time),
                "url": url,
                "idx_url": url+".idx",
            }, schedule_at=acquire_time)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    HRRR.queue()
