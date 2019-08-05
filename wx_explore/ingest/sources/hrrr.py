#!/usr/bin/env python3
from datetime import datetime, timedelta
import tempfile
import os
import logging

from wx_explore.ingest import reduce_grib, ingest_grib_file
from wx_explore.common.models import Source
from wx_explore.web import db


def ingest_hrrr(time_min=0, time_max=18, run_time=None):
    hrrr_source = Source.query.filter_by(short_name="hrrr").first()

    if run_time is None:
        run_time = datetime.utcnow()
        run_time -= timedelta(hours=1)

        # HRRR is available by the following half hour, but if we update before then we need to go back another hour
        if run_time.minute < 30:
            run_time -= timedelta(hours=1)

    base_url = run_time.strftime("https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/hrrr.%Y%m%d/conus/hrrr.t%Hz.wrfsubhf{}.grib2")

    urls = [base_url.format(str(x).zfill(2)) for x in range(time_min, time_max + 1)]
    idx_urls = [url + ".idx" for url in urls]

    with tempfile.NamedTemporaryFile() as reduced:
        for url, idx_url in zip(urls, idx_urls):
            logging.info(f"Downloading and reducing {url}")
            reduce_grib(url, idx_url, hrrr_source.fields, reduced)

        logging.info("Ingesting all")
        ingest_grib_file(reduced.name, hrrr_source)

    hrrr_source.last_updated = datetime.utcnow()

    db.session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_hrrr()
