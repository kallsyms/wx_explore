from pathlib import Path
import sys, logging
sys.path.append((Path(__file__).parent.parent).as_posix())

from datetime import datetime

import azure.functions as func
import logging
import json
import tempfile

from wx_explore.common import tracing
from wx_explore.common.logging import init_sentry
from wx_explore.common.models import Source
from wx_explore.common.tracing import init_tracing
from wx_explore.ingest.grib import reduce_grib, ingest_grib_file
from wx_explore.web.core import db

logger = logging.getLogger(__name__)


def main(msg: func.QueueMessage):
    init_sentry()
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('azure').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.ERROR)
    init_tracing('queue_worker')

    with tracing.start_span('queue worker'):
        ingest_req = json.loads(msg.get_body())

        with tracing.start_span('ingest item') as span:
            for k, v in ingest_req.items():
                span.set_attribute(k, v)

            try:
                source = Source.query.filter_by(short_name=ingest_req['source']).first()

                with tempfile.NamedTemporaryFile() as reduced:
                    with tracing.start_span('download'):
                        logging.info(f"Downloading and reducing {ingest_req['url']} from {ingest_req['run_time']} {source.short_name}")
                        reduce_grib(ingest_req['url'], ingest_req['idx_url'], source.fields, reduced)
                    with tracing.start_span('ingest'):
                        logging.info("Ingesting all")
                        ingest_grib_file(reduced.name, source)

                source.last_updated = datetime.utcnow()

                db.session.commit()
            except Exception:
                logger.exception("Exception while ingesting %s.")
