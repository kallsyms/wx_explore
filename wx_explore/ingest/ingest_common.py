from scipy.spatial import cKDTree
import collections
import hashlib
import logging
import numpy
import pickle
import pygrib

from wx_explore.common.models import (
    SourceField,
    Projection,
    FileMeta,
    FileBandMeta,
)
from wx_explore.common.queue import pq
from wx_explore.common.storage import get_s3_bucket
from wx_explore.web import db

logger = logging.getLogger(__name__)


def get_queue():
    return pq['ingest']


def get_or_create_projection(msg):
    lats, lons = msg.latlons()

    # GFS (and maybe others) have lons that range 0-360 instead of -180 to 180.
    # If found, transform them to match the standard range.
    if lons.max() > 180:
        lons = numpy.vectorize(lambda n: n if 0 <= n < 180 else n-360)(lons)

    projection = Projection.query.filter_by(
        params=msg.projparams,
        lats=lats.tolist(),
        lons=lons.tolist(),
    ).first()

    if projection is None:
        tree = cKDTree(numpy.stack([lons.ravel(), lats.ravel()], axis=-1))

        projection = Projection(
            params=msg.projparams,
            n_x=msg.values.shape[1],
            n_y=msg.values.shape[0],
            lats=lats.tolist(),
            lons=lons.tolist(),
            tree=pickle.dumps(tree),
        )
        db.session.add(projection)
        db.session.commit()

    return projection


def ingest_grib_file(file_path, source):
    """
    Ingests a given GRIB file into the backend.
    :param file_path: Path to the GRIB file
    :param source: Source object which denotes which source this data is from
    :return: None
    """
    logger.info("Processing GRIB file '%s'", file_path)

    grib = pygrib.open(file_path)

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
            projection = get_or_create_projection(msg)
            field.projection_id = projection.id
            db.session.commit()

        data_by_projection[field.projection.id][(field.id, msg.validDate, msg.analDate)].append(msg)

    logger.info("Saving denormalized location/time data for all messages")

    for proj_id, fields in data_by_projection.items():
        metas = []
        vals = []

        logger.info("Processing projection %d: fields %s", proj_id, fields)
        s3_file_name = hashlib.md5(f"{file_path}-{proj_id}".encode('utf-8')).hexdigest()
        s3_file_name = s3_file_name[:2] + '/' + s3_file_name

        fm = FileMeta(
            file_name=s3_file_name,
            projection_id=proj_id,
        )
        db.session.add(fm)
        db.session.commit()

        offset = 0
        for i, ((field_id, valid_time, run_time), msgs) in enumerate(fields.items()):
            metas.append(FileBandMeta(
                file_name=s3_file_name,
                source_field_id=field_id,
                valid_time=valid_time,
                run_time=run_time,
                offset=offset,
                vals_per_loc=len(msgs),
            ))

            for msg in msgs:
                vals.append(msg.values.astype(numpy.float32))
                offset += 4  # sizeof(float32)

        fm.loc_size = offset

        s3 = get_s3_bucket()
        s3.put_object(
            Key=s3_file_name,
            Body=numpy.stack(vals, axis=-1).tobytes(),
        )

        db.session.add_all(metas)
        db.session.commit()

    logger.info("Done saving denormalized data")
