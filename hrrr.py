#!/usr/bin/env python3
from datetime import datetime, timedelta
import tempfile
import os
import logging

from wx_explore.ingest.ingest_common import reduce_grib, ingest_grib_file
from wx_explore.web.data.models import Source


def ingest_hrrr(time_min=0, time_max=18, base_time_offset=0):
    hrrr_source = Source.query.filter_by(name="HRRR 2D Surface Data (Sub-Hourly)").first()

    base_time = datetime.utcnow()
    base_time -= timedelta(hours=base_time_offset)
    base_time -= timedelta(hours=1)

    # HRRR is available by the following half hour, but if we update before then we need to go back another hour
    if base_time.minute < 30:
        base_time -= timedelta(hours=1)

    base_url = base_time.strftime("http://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/hrrr.%Y%m%d/hrrr.t%Hz.wrfsubhf{}.grib2")

    urls = [base_url.format(str(x).zfill(2)) for x in range(time_min, time_max + 1)]
    idx_urls = [url + ".idx" for url in urls]

    tmp_fn = tempfile.mkstemp()[1]

    with open(tmp_fn, 'wb') as reduced:
        for url, idx_url in zip(urls, idx_urls):
            logging.info(f"Downloading and reducing {url}")
            reduced.write(reduce_grib(url, idx_url, hrrr_source))

    logging.info("Ingesting all")
    ingest_grib_file(tmp_fn, hrrr_source)

    os.remove(tmp_fn)

    hrrr_source.last_updated = datetime.utcnow()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_hrrr()
