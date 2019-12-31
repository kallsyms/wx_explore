import collections
import hashlib
import logging
import numpy
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
            projection = Projection.query.filter_by(
                params=msg.projparams,
                lats=msg.latlons()[0].tolist(),
                lons=msg.latlons()[1].tolist(),
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
