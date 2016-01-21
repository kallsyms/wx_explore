from datetime import datetime, timedelta
import tempfile
import shutil

from ingest_common import *
from models import *


def ingest_hrrr():
    hrrr_source = Source.query.filter_by(id=1).first()

    base_time = datetime.utcnow()
    base_time -= timedelta(hours=1)

    # HRRR is available by the following half hour, but if we update before then, go back another hour
    if base_time.minute < 30:
        base_time -= timedelta(hours=1)

    base_url = base_time.strftime(
        "http://nomads.ncep.noaa.gov/pub/data/nccf/nonoperational/com/hrrr/prod/hrrr.%Y%m%d/hrrr.t%Hz.wrfsubhf{}.grib2")

    urls = [base_url.format(str(x).zfill(2)) for x in range(16)]

    temp_folder = tempfile.mkdtemp()

    hrrr_source.last_updated = datetime.utcnow()

    urls = ["http://localhost:8080/hrrr.t04z.wrfsubhf15.grib2"]

    for url in urls:
        f = download(url, temp_folder)

        if f:
            ingest_grib_file(f, hrrr_source)
            os.remove(f)
        else:
            print("Couldn't download file from {}".format(url))

    shutil.rmtree(temp_folder)

ingest_hrrr()