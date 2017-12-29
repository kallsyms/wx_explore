import sys
import logging
import tempfile
import os

from ingest_common import *

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
logging.getLogger('raster2pgsql').setLevel(logging.INFO)

src_name = sys.argv[1]
files = sys.argv[2:]

# E.g. "HRRR 2D Surface Data (Sub-Hourly)"
src = Source.query.filter_by(name=src_name).first()

if src is None:
    raise Exception(f"Invalid source {src_name}")

for file in files:
    with open(file + '.idx', 'r') as index:
        ranges = get_grib_ranges(index.read(), src)

    reduced_fn = tempfile.mkstemp()[1]

    try:
        with open(file, 'rb') as src_grib:
            with open(reduced_fn, 'wb') as reduced_grib:
                for offset, length in ranges:
                    src_grib.seek(offset)
                    reduced_grib.write(src_grib.read(length))

        ingest_grib_file(reduced_fn, src)
    except:
        os.remove(reduced_fn)
        raise