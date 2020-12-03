import datetime
import logging
import tempfile

from wx_explore.common import tracing
from wx_explore.common.utils import url_exists
from wx_explore.ingest.common import IngestSource
from wx_explore.ingest.grib import reduce_grib, ingest_grib_file


class GRIBSource(IngestSource):
    @staticmethod
    def ingest(source, ingest_req):
        # If this URL doesn't exist, try again in a few minutes
        if not (url_exists(ingest_req['url']) and url_exists(ingest_req['idx_url'])):
            raise FileNotFoundError(ingest_req['url'])

        with tempfile.NamedTemporaryFile() as reduced:
            with tracing.start_span('download'):
                logging.info(f"Downloading and reducing {ingest_req['url']} from {ingest_req['run_time']} {source.short_name}")
                reduce_grib(ingest_req['url'], ingest_req['idx_url'], source.fields, reduced)
            with tracing.start_span('ingest'):
                logging.info("Ingesting all")
                ingest_grib_file(reduced.name, source)

        source.last_updated = datetime.datetime.utcnow()
