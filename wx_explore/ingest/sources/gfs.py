#!/usr/bin/env python3
from datetime import datetime, timedelta
from typing import Optional
import logging

from wx_explore.common.utils import datetime2unix
from wx_explore.ingest import get_queue
from wx_explore.ingest.sources.source import IngestSource


class GFS(IngestSource):
    @staticmethod
    def queue(
            time_min: int = 0,
            time_max: int = 384,
            run_time: Optional[datetime] = None,
            acquire_time: Optional[datetime] = None
    ):
        times = list(range(time_min, min(time_max+1, 120))) + list(range(120, time_max+1, 3))

        if run_time is None:
            # gfs is run every 6 hours
            run_time = datetime.utcnow()
            run_time = run_time.replace(hour=(run_time.hour//6)*6, minute=0, second=0, microsecond=0)

        if acquire_time is None:
            # the first files are available 3.5hr after
            acquire_time = run_time
            acquire_time += timedelta(hours=3, minutes=30)

        base_url = run_time.strftime("https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.%Y%m%d/%H/gfs.t%Hz.pgrb2.0p25.f{}")

        urls = [base_url.format(str(x).zfill(3)) for x in times]

        q = get_queue()
        for url in urls:
            q.put({
                "source": "gfs",
                "run_time": datetime2unix(run_time),
                "url": url,
                "idx_url": url+".idx",
            }, schedule_at=acquire_time)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    GFS.queue()
