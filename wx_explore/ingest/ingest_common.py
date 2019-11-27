from scipy.spatial import cKDTree
from shapely import wkb
from sqlalchemy import Table, MetaData
import gdal
import gdalconst
import json
import logging
import numpy
import pygrib
import collections

from wx_explore.common.models import (
    SourceField,
    CoordinateLookup,
    Projection,
    DataRaster,
    Location,
    LocationData,
)
from wx_explore.common.utils import datetime2unix
from wx_explore.common.queue import pq
from wx_explore.ingest import reduce_grib
from wx_explore.ingest.raster2pgsql import make_options, wkblify_raster_header, wkblify_band_header, wkblify_band
from wx_explore.web import db

logger = logging.getLogger(__name__)


def get_queue():
    return pq['ingest']


def get_location_index_map(grib_message, locations):
    """
    Generates grid coordinates for each location in locations from the given GRIB message
    :param grib_message: The GRIB message for which the coordinates should be generated
    :param locations: List of locations for which indexes should be generated
    :return: loc_id,x,y tuples for each given input location
    """
    lats, lons = grib_message.latlons()

    latmin = lats.min()
    latmax = lats.max()
    lonmin = lons.min()
    lonmax = lons.max()

    # GFS (and maybe others) have lons that range 0-360 instead of -180 to 180.
    # If found, transform them to match the standard range.
    if lonmax > 180:
        lons = numpy.vectorize(lambda n: n if 0 <= n < 180 else n-360)(lons)
        lonmin = lons.min()
        lonmax = lons.max()

    shape = grib_message.values.shape
    tree = cKDTree(numpy.dstack([lons.ravel(), lats.ravel()])[0])
    for location in locations:
        coords = wkb.loads(bytes(location.location.data))
        if lonmin <= coords.x <= lonmax and latmin <= coords.y <= latmax:
            idx = tree.query([coords.x, coords.y])[1]
            x = idx % shape[1]
            y = idx // shape[1]
            yield (location.id, x, y)


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

    # Cache coordinate lookup tables so they can be reused
    # Map of projection_id, location_id to (x,y)
    coordinate_luts = collections.defaultdict(dict)

    # Build up a big array of loc_id -> {valid_time -> {(source_field_id, run_time) -> [values]}}
    loc_time_values = collections.defaultdict(lambda: collections.defaultdict(dict))

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
                )
                db.session.add(projection)
                db.session.commit()

            field.projection_id = projection.id
            db.session.commit()

        if field.projection.id not in coordinate_luts:
            # Ensure the location->coordinate lookup table has been created in-DB for this field
            if CoordinateLookup.query.filter_by(projection_id=field.projection.id).count() == 0:
                logger.info("Generating coordinate lookup table for projection with params '%s'", field.projection.params)
                entries = []
                for loc_id, x, y in get_location_index_map(msg, Location.query.all()):
                    # Create the DB object
                    lookup_entry = CoordinateLookup()
                    lookup_entry.projection_id = field.projection.id
                    lookup_entry.location_id = loc_id
                    lookup_entry.x = x
                    lookup_entry.y = y
                    entries.append(lookup_entry)

                    # And cache locally
                    coordinate_luts[field.projection.id][loc_id] = (y, x)

                db.session.bulk_save_objects(entries)
                db.session.commit()
            else:
                # lookup table is in DB, but not cached locally yet
                for entry in CoordinateLookup.query.filter_by(projection_id=field.projection.id).all():
                    coordinate_luts[field.projection.id][entry.location_id] = (entry.y, entry.x)

            logger.info("Coordinate lookup table loaded")

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

            for loc_id, coords in coordinate_luts[field.projection.id].items():
                if not numpy.ma.is_masked(grib_data) or not grib_data.mask[coords]:
                    tv = loc_time_values[loc_id][msg.validDate]
                    key = (field.id, msg.analDate)

                    # This ensures that v['values'] always exists.
                    # For simple models, this will only have 1 element, and v['value'] will also exist.
                    # For ensemble models, this will contain all values in each ensemble run, and v['value'] will be removed.
                    if key in tv:
                        if 'value' in tv[key]:
                            del tv[key]['value']
                        tv[key]['values'].append(float(grib_data[coords]))
                    else:
                        tv[key] = {
                            "src_field_id": field.id,
                            "run_time": datetime2unix(msg.analDate),
                            "value": float(grib_data[coords]),
                            "values": [float(grib_data[coords])],
                        })

    if save_denormalized:
        logger.info("Saving denormalized location/time data for all layers")

        ldtemp = Table("location_data_tmp", MetaData(), *[col.copy() for col in LocationData.__table__.columns], prefixes=['TEMPORARY'])
        ldtemp.create(db.session.connection())

        stuff = [
            (str(loc_id), str(valid_time), json.dumps([run_field_values.values()]))
            for loc_id, loc_id_values in loc_time_values.items()
            for valid_time, run_field_values in loc_id_values.items()
        ]

        for i in range(0, len(stuff), 10000):
            db.session.execute("INSERT INTO location_data_tmp VALUES " + ",".join("('" + "','".join(s) + "')" for s in stuff[i:i+10000]))

        # XXX: For some reason this doesn't seem to be doing a bulk insert?
        # db.session.execute(ldtemp.insert(), [
        #     {
        #         "location_id": loc_id,
        #         "valid_time": valid_time,
        #         "values": values,
        #     }
        #     for loc_id, loc_id_values in loc_time_values.items()
        #     for valid_time, values in loc_id_values.items()
        # ])

        db.session.execute("INSERT INTO location_data SELECT * FROM location_data_tmp ON CONFLICT (location_id, valid_time) DO UPDATE SET values = location_data.values || excluded.values")

        db.session.commit()
        ldtemp.drop(db.session.connection())
        db.session.commit()

        logger.info("Done saving denormalized data")
