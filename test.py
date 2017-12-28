import sys
import logging

from ingest_common import *

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")

src_name = sys.argv[1]
files = sys.argv[2:]

# E.g. "HRRR 2D Surface Data (Sub-Hourly)"
src = Source.query.filter_by(name=src_name).first()

for file in files:
    ingest_grib_file(file, src)
