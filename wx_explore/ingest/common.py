from typing import Dict, Any

import binascii
import datetime
import logging
import numpy
import pygrib

from wx_explore.common.models import Projection
from wx_explore.common.queue import pq
from wx_explore.common.utils import datetime2unix
from wx_explore.ingest.sources.source import IngestSource
from wx_explore.web.core import db

logger = logging.getLogger(__name__)


def get_queue():
    return pq['ingest']


def queue_work(source_cls, valid_time: datetime.datetime, data: Dict[str, Any], **kwargs):
    get_queue().put(
        {
            "source": source_cls.SOURCE_NAME,
            "valid_time": datetime2unix(valid_time),
            "data": data,
        },
        **kwargs
    )


def get_or_create_projection(lats, lons):
    assert lats.shape == lons.shape

    # GFS (and maybe others) have lons that range 0-360 instead of -180 to 180.
    # If found, transform them to match the standard range.
    if lons.max() > 180:
        lons = numpy.vectorize(lambda n: n if 0 <= n < 180 else n-360)(lons)

    ll_hash = binascii.crc32(numpy.round([lats, lons], 8).tobytes())

    projection = Projection.query.filter_by(
        ll_hash=ll_hash,
    ).first()

    if projection is None:
        logger.info("Creating new projection with shape=%s,ll_hash=%d", lats.shape, ll_hash)

        projection = Projection(
            n_x=lats.shape[1],  # lats and lons have the same shape
            n_y=lats.shape[0],
            ll_hash=ll_hash,
            lats=lats.tolist(),
            lons=lons.tolist(),
        )
        db.session.add(projection)
        db.session.commit()

    return projection


def get_source_modules():
    from wx_explore.ingest.sources.hrrr import HRRR
    from wx_explore.ingest.sources.gfs import GFS
    from wx_explore.ingest.sources.nam import NAM
    from wx_explore.ingest.sources.metar import METAR

    return {
        c.SOURCE_NAME: c for c in (HRRR, GFS, NAM, METAR)
    }


def get_source_module(short_name: str) -> IngestSource:
    return get_source_modules()[short_name]
