from shapely import wkb
from sqlalchemy import Table, MetaData
from sqlalchemy.orm import mapper
import gdal
import gdalconst
import logging
import numpy
import pygrib
import collections

from wx_explore.common.models import (
    SourceField,
    Projection,
    Location,
    FileMeta,
    FileBandMeta,
)
from wx_explore.common.utils import datetime2unix
from wx_explore.common.queue import pq
from wx_explore.ingest import reduce_grib
from wx_explore.ingest.raster2pgsql import make_options, wkblify_raster_header, wkblify_band_header, wkblify_band
from wx_explore.web import db

logger = logging.getLogger(__name__)


def get_queue():
    return pq['ingest']


def ingest_grib_file(file_path, source):
    """
    Ingests a given GRIB file into the backend
    :param file_path: Path to the GRIB file
    :param source: Source object which denotes which source this data is from
    :return: None
    """
    logger.info("Processing GRIB file '%s'", file_path)

    grib = pygrib.open(file_path)
    ds = gdal.Open(file_path, gdalconst.GA_ReadOnly)

    # Keeps all data points that we'll be inserting at the end.
    # Map of proj_id to map of {(field, valid_time, run_time) -> [msg, ...]}
    data_by_projection = collections.defaultdict(lambda: collections.defaultdict(list))

    for msg in grib:
        field = SourceField.query.filter_by(
            source_id=source.id,
            grib_name=msg.name,
        ).first()

        if field is None:
            continue

        if field.projection is None or field.projection.params != msg.projparams:
            projection = Projection.query.filter_by(
                params=msg.projparams
            ).first()

            if projection is None:
                projection = Projection(
                    params=msg.projparams,
                    lats=msg.latlons()[0].tolist(),
                    lons=msg.latlons()[1].tolist(),
                )
                db.session.add(projection)
                db.session.commit()

            field.projection_id = projection.id
            db.session.commit()

        data_by_projection[field.projection.id][(field.id, msg.validDate, msg.analDate)].append(msg)

    logger.info("Saving denormalized location/time data for all messages")

    metas = []
    vals = []

    for proj_id, fields in data_by_projection.items():
        logger.info("Processing projection %d: fields %s", proj_id, fields)
        fm = FileMeta(
            file_name=f"/tmp/{proj_id}",
            projection_id=proj_id,
        )
        db.session.add(fm)
        db.session.commit()
        with open(fm.file_name, 'wb') as denormalized_file:
            offset = 0
            for i, ((field_id, valid_time, run_time), msgs) in enumerate(fields.items()):
                metas.append(FileBandMeta(
                    file_name=denormalized_file.name,
                    band_id=i,
                    source_field_id=field_id,
                    valid_time=valid_time,
                    run_time=run_time,
                    offset=offset,
                    vals_per_loc=len(msgs),
                ))

                for msg in msgs:
                    vals.extend(msg.values.astype(numpy.float32))
                    offset += 4  # sizeof(float32)

            denormalized_file.write(numpy.stack(vals, axis=-1).tostring())
            fm.loc_size = offset

        db.session.add_all(metas)
        db.session.commit()

    logger.info("Done saving denormalized data")
