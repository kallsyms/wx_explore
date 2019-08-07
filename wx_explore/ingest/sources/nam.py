#!/usr/bin/env python3
from datetime import datetime, timedelta
import tempfile
import os
import logging

from wx_explore.common.utils import datetime2unix
from wx_explore.ingest import get_queue
from wx_explore.web import db


def ingest_nam(time_min=0, time_max=60, run_time=None):
    if run_time is None:
        # nam is run every 6 hours and is available ~3 hours after
        run_time = datetime.utcnow()
        hours_since_last_run = run_time.hour % 6
        run_time -= timedelta(hours=run_time.hour - ((run_time.hour // 6) * 6))
        if hours_since_last_run < 3:
            run_time -= timedelta(hours=6)

    base_url = run_time.strftime("https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod/nam.%Y%m%d/nam.t%Hz.conusnest.hiresf{}.tm00.grib2")

    urls = [base_url.format(str(x).zfill(2)) for x in range(time_min, time_max + 1)]

    with get_queue() as q:
        for url in urls:
            q.put({
                "source": "nam",
                "run_time": datetime2unix(run_time),
                "url": url,
                "idx_url": url+".idx",
            })


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_nam()
