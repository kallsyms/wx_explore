#!/usr/bin/env python3
from datetime import datetime, timedelta
import tempfile
import os
import logging

from wx_explore.common.utils import datetime2unix
from wx_explore.ingest import get_queue
from wx_explore.web import db


def ingest_hrrr(time_min=0, time_max=18, run_time=None):
    if run_time is None:
        run_time = datetime.utcnow()
        run_time -= timedelta(hours=1)

        # HRRR is available by the following half hour, but if we update before then we need to go back another hour
        if run_time.minute < 30:
            run_time -= timedelta(hours=1)

    base_url = run_time.strftime("https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/hrrr.%Y%m%d/conus/hrrr.t%Hz.wrfsubhf{}.grib2")

    urls = [base_url.format(str(x).zfill(2)) for x in range(time_min, time_max + 1)]

    with get_queue() as q:
        for url in urls:
            q.put({
                "source": "hrrr",
                "run_time": datetime2unix(run_time),
                "url": url,
                "idx_url": url+".idx",
            })


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_hrrr()
