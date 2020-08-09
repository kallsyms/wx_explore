import binascii
import logging
import numpy
import pygrib

from wx_explore.common.models import Projection
from wx_explore.common.queue import pq
from wx_explore.ingest.sources.source import IngestSource
from wx_explore.web.core import db

logger = logging.getLogger(__name__)


def get_queue():
    return pq['ingest']


def get_or_create_projection(msg):
    lats, lons = msg.latlons()

    # GFS (and maybe others) have lons that range 0-360 instead of -180 to 180.
    # If found, transform them to match the standard range.
    if lons.max() > 180:
        lons = numpy.vectorize(lambda n: n if 0 <= n < 180 else n-360)(lons)

    ll_hash = binascii.crc32(numpy.round([lats, lons], 8).tobytes())

    projection = Projection.query.filter_by(
        params=msg.projparams,
        ll_hash=ll_hash,
    ).first()

    if projection is None:
        logger.info("Creating new projection with params %s", msg.projparams)

        projection = Projection(
            params=msg.projparams,
            n_x=msg.values.shape[1],
            n_y=msg.values.shape[0],
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

    return {
        c.SOURCE_NAME: c for c in (HRRR, GFS, NAM)
    }


def get_source_module(short_name: str) -> IngestSource:
    return get_source_modules()[short_name]
