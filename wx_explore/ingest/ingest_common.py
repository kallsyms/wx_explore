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
    DataRaster,
    Location,
    PointData,
)
from wx_explore.common.utils import datetime2unix
from wx_explore.common.queue import pq
from wx_explore.ingest import reduce_grib
from wx_explore.ingest.raster2pgsql import make_options, wkblify_raster_header, wkblify_band_header, wkblify_band
from wx_explore.web import db

logger = logging.getLogger(__name__)


def get_queue():
    return pq['ingest']


def ingest_grib_file(file_path, source, save_rasters=False, save_denormalized=True):
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
    # Map of (proj_id, x, y, valid_time) -> {Map of (field_id, analysisDate) -> JSON}
    # The second layer of indirection is needed for things like ensemble forecasts where a given
    # (field_id, analysisDate) is not unique and all data values must be preserved for later stats
    data_points = collections.defaultdict(dict)

    for msg in grib:
        field = SourceField.query.filter_by(
            source_id=source.id,
            grib_name=msg.name,
        ).first()

        if field is None:
            continue

        logger.info("Processing message '%s' (field '%s')", msg, field.grib_name)

        if field.projection is None or field.projection.params != msg.projparams:
            projection = Projection.query.filter_by(
                params=msg.projparams
            ).first()

            if projection is None:
                projection = Projection(
                    params=msg.projparams,
                    latlons=list(map(numpy.ndarray.tolist, msg.latlons())),
                )
                db.session.add(projection)
                db.session.commit()

            field.projection_id = projection.id
            db.session.commit()

        if save_rasters:
            logger.info(f"Saving raster data for {field}")

            band_id = msg.messagenumber
            opts = make_options(0, band_id)
            band = ds.GetRasterBand(band_id)

            for yoff in range(band.YSize):
                raster = DataRaster(
                    source_field_id=field.id,
                    run_time=msg.analDate,
                    valid_time=msg.validDate,
                    row=yoff,
                )
                wkb_bytes = wkblify_raster_header(opts, ds, 1, (0, yoff), band.XSize, 1)
                wkb_bytes += wkblify_band_header(opts, band)
                wkb_bytes += wkblify_band(opts, band, 1, 0, yoff, (band.XSize, 1), (band.XSize, 1), file_path, band_id)

                raster.rast = wkb_bytes.decode('ascii')

                db.session.add(raster)

            db.session.commit()

            logger.info(f"Done saving raster data for {field}")

        if save_denormalized:
            grib_data = msg.values

            for y in range(grib_data.shape[0]):
                for x in range(grib_data.shape[1]):
                    if not numpy.ma.is_masked(grib_data) or not grib_data.mask[(y,x)]:
                        pts = data_points[(field.projection.id, x, y, msg.validDate)]
                        key = (field.id, datetime2unix(msg.analDate))
                        if key not in pts:
                            pts[key] = {
                                "src_field_id": field.id,
                                "run_time": datetime2unix(msg.analDate),
                                "values": [],
                            }
                        pts[key]["values"].append(float(grib_data[(y,x)]))

    if save_denormalized:
        logger.info("Saving denormalized location/time data for all layers")

        pdtemp = Table("point_data_tmp", MetaData(), *[col.copy() for col in PointData.__table__.columns], prefixes=['TEMPORARY'])
        pdtemp.create(db.session.connection())

        items = list(data_points.items())
        for batch in (items[i:i+10000] for i in range(0, len(items), 10000)):
            db.session.execute(pdtemp.insert().values([
                {
                    "projection_id": proj_id,
                    "x": x,
                    "y": y,
                    "valid_time": valid_time,
                    "values": list(pts.values()),
                }
                for (proj_id, x, y, valid_time), pts in batch
            ]))

        db.session.execute("INSERT INTO point_data SELECT * FROM point_data_tmp ON CONFLICT (projection_id, x, y, valid_time) DO UPDATE SET values = point_data.values || excluded.values")

        db.session.commit()
        pdtemp.drop(db.session.connection())
        db.session.commit()

        logger.info("Done saving denormalized data")
