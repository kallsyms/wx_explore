#!/usr/bin/env python3
from datetime import datetime, timedelta

import logging
import tempfile

from wx_explore.common.logging import init_sentry
from wx_explore.common.models import Source
from wx_explore.common.utils import url_exists
from wx_explore.ingest.common import get_queue
from wx_explore.ingest.grib import reduce_grib, ingest_grib_file
from wx_explore.web.core import db

logger = logging.getLogger(__name__)


def ingest_from_queue():
    q = get_queue()
    for ingest_req in q:
        # Queue is empty for now
        if ingest_req is None:
            logger.info("Empty queue")
            break

        ingest_req = ingest_req.data

        # Expire out anything whose run time is very old (probably a bad request/URL)
        if datetime.utcfromtimestamp(ingest_req['run_time']) < datetime.utcnow() - timedelta(hours=12):
            logger.info("Expiring old request %s", ingest_req)
            continue

        # If this URL doesn't exist, try again in a few minutes
        if not (url_exists(ingest_req['url']) and url_exists(ingest_req['idx_url'])):
            logger.info("Rescheduling request %s", ingest_req)
            q.put(ingest_req, '5m')
            continue

        try:
            source = Source.query.filter_by(short_name=ingest_req['source']).first()

            with tempfile.NamedTemporaryFile() as reduced:
                logging.info(f"Downloading and reducing {ingest_req['url']} from {ingest_req['run_time']} {source.short_name}")
                reduce_grib(ingest_req['url'], ingest_req['idx_url'], source.fields, reduced)
                logging.info("Ingesting all")
                ingest_grib_file(reduced.name, source)

            source.last_updated = datetime.utcnow()

            db.session.commit()
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.exception("Exception while ingesting %s. Will retry", ingest_req)
            q.put(ingest_req, '5m')


if __name__ == "__main__":
    init_sentry()
    logging.basicConfig(level=logging.INFO)
    ingest_from_queue()
