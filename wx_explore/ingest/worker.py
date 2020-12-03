#!/usr/bin/env python3
from datetime import datetime, timedelta

import logging

from wx_explore.common import tracing
from wx_explore.common.logging import init_sentry
from wx_explore.common.models import Source
from wx_explore.common.tracing import init_tracing
from wx_explore.ingest.common import get_queue, get_source_module
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

        # Expire out anything whose valid time is very old (probably a bad request/URL)
        if datetime.utcfromtimestamp(ingest_req['valid_time']) < datetime.utcnow() - timedelta(hours=12):
            logger.info("Expiring old request %s", ingest_req)
            continue

        try:
            source = Source.query.filter_by(short_name=ingest_req['source']).first()

            with tracing.start_span('ingest item') as span:
                for k, v in ingest_req['data'].items():
                    span.set_attribute(k, v)

                get_source_module(source.short_name).ingest(source, ingest_req['data'])
        except KeyboardInterrupt:
            raise
        except Exception:
            logger.exception("Exception while ingesting %s. Will retry", ingest_req)
            q.put(ingest_req, '5m')

        db.session.commit()


if __name__ == "__main__":
    init_sentry()
    logging.basicConfig(level=logging.INFO)
    init_tracing('queue_worker')
    with tracing.start_span('queue worker'):
        ingest_from_queue()
