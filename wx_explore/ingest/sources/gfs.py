#!/usr/bin/env python3
from datetime import datetime, timedelta
import tempfile
import os
import logging

from wx_explore.ingest import reduce_grib, ingest_grib_file
from wx_explore.common.models import Source
from wx_explore.web import db


def ingest_gfs(time_min=0, time_max=384, run_time=None):
    times = list(range(time_min, min(time_max+1, 120))) + list(range(120, time_max+1, 3))

    gfs_source = Source.query.filter_by(short_name="gfs").first()

    if run_time is None:
        # GFS is run every 6 hours and is available ~5 hours after
        run_time = datetime.utcnow()
        hours_since_last_run = run_time.hour % 6
        run_time -= timedelta(hours=run_time.hour - ((run_time.hour // 6) * 6))
        if hours_since_last_run < 5:
            run_time -= timedelta(hours=6)

    base_url = run_time.strftime("https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.%Y%m%d/%H/gfs.t%Hz.pgrb2.0p25.f{}")

    urls = [base_url.format(str(x).zfill(3)) for x in times]
    idx_urls = [url + ".idx" for url in urls]

    with tempfile.NamedTemporaryFile() as reduced:
        for url, idx_url in zip(urls, idx_urls):
            logging.info(f"Downloading and reducing {url}")
            reduce_grib(url, idx_url, gfs_source.fields, reduced)

        logging.info("Ingesting all")
        ingest_grib_file(reduced.name, gfs_source)

    gfs_source.last_updated = datetime.utcnow()

    db.session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_gfs()
