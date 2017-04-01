from datetime import datetime, timedelta
import tempfile
import shutil
import os
import logging

from ingest_common import *
from models import *

logger = logging.getLogger('ingest_fcst')


def ingest_hrrr(time_min=0, time_max=19):
    hrrr_source = Source.query.filter_by(name="HRRR 2D Surface Data (Sub-Hourly)").first()

    base_time = datetime.utcnow()
    base_time -= timedelta(hours=1)

    # HRRR is available by the following half hour, but if we update before then we need to go back another hour
    if base_time.minute < 30:
        base_time -= timedelta(hours=1)

    base_url = base_time.strftime("http://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/hrrr.%Y%m%d/hrrr.t%Hz.wrfsubhf{}.grib2")

    urls = [base_url.format(str(x).zfill(2)) for x in range(time_min, time_max)]

    temp_folder = tempfile.mkdtemp()

    hrrr_source.last_updated = datetime.utcnow()

    for url in urls:
        logger.info("Ingesting HRRR url '%s'", url)
        f = download(url, out=temp_folder)

        if f:
            ingest_grib_file(f, hrrr_source)
            os.remove(f)
        else:
            logger.warning("Couldn't download file from %s", url)

    shutil.rmtree(temp_folder)