import sys
import logging

from ingest_common import *

src = Source.query.filter_by(name="HRRR 2D Surface Data (Sub-Hourly)").first()
ingest_grib_file(sys.argv[1], src)
