#!/usr/bin/env python3
import sys
import logging
import tempfile

from wx_explore.ingest.ingest_common import ingest_grib_file
from wx_explore.ingest.reduce_grib import get_grib_ranges
from wx_explore.common.models import Source


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")

if len(sys.argv) < 3:
    print(f"Usage: {sys.argv[0]} source_short_name files...", file=sys.stderr)
    sys.exit(1)

# E.g. "hrrr"
src_name = sys.argv[1]
files = sys.argv[2:]

src = Source.query.filter_by(short_name=src_name).first()

if src is None:
    raise Exception(f"Invalid source {src_name}")

for f in files:
    with open(f + '.idx', 'r') as index:
        ranges = get_grib_ranges(index.read(), src.fields)

    with tempfile.NamedTemporaryFile() as reduced:
        with open(f, 'rb') as src_grib:
            for offset, length in ranges:
                src_grib.seek(offset)
                reduced.write(src_grib.read(length))

            reduced.flush()

            ingest_grib_file(reduced.name, src)
