from ingest_fcst import *

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
ingest_grib_file("/tmp/hrrr.t20z.wrfsubhf00.grib2", Source.query.filter_by(name="HRRR 2D Surface Data (Sub-Hourly)").first())