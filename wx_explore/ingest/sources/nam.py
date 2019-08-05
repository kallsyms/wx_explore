#!/usr/bin/env python3
from datetime import datetime, timedelta
import tempfile
import os
import logging

from wx_explore.ingest import reduce_grib, ingest_grib_file
from wx_explore.common.models import Source
from wx_explore.web import db


def ingest_nam(time_min=0, time_max=60, run_time=None):
    nam_source = Source.query.filter_by(short_name="nam").first()

    if run_time is None:
        # nam is run every 6 hours and is available ~3 hours after
        run_time = datetime.utcnow()
        hours_since_last_run = run_time.hour % 6
        run_time -= timedelta(hours=run_time.hour - ((run_time.hour // 6) * 6))
        if hours_since_last_run < 3:
            run_time -= timedelta(hours=6)

    base_url = run_time.strftime("https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod/nam.%Y%m%d/nam.t%Hz.conusnest.hiresf{}.tm00.grib2")

    urls = [base_url.format(str(x).zfill(2)) for x in range(time_min, time_max+1)]
    idx_urls = [url + ".idx" for url in urls]

    with tempfile.NamedTemporaryFile() as reduced:
        for url, idx_url in zip(urls, idx_urls):
            logging.info(f"Downloading and reducing {url}")
            reduce_grib(url, idx_url, nam_source.fields, reduced)

        logging.info("Ingesting all")
        ingest_grib_file(reduced.name, nam_source)

    nam_source.last_updated = datetime.utcnow()

    db.session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_nam()
