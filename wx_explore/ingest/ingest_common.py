#!/usr/bin/env python3
from scipy.spatial import cKDTree
from shapely import wkb
import gdal
import gdalconst
import logging
import numpy
import pygrib
import collections

from wx_explore.common.utils import datetime2unix
from wx_explore.ingest.raster2pgsql import make_options, wkblify_raster_header, wkblify_band_header, wkblify_band
from wx_explore.common.models import (
    SourceField,
    CoordinateLookup,
    DataRaster,
    Location,
    LocationData,
)
from wx_explore.web import db

logger = logging.getLogger(__name__)


def get_location_index_map(grib_message, locations):
    """
    Generates grid coordinates for each location in locations from the given GRIB message
    :param grib_message: The GRIB message for which the coordinates should be generated
    :param locations: List of locations for which indexes should be generated
    :return: loc_id,x,y tuples for each given input location
    """
    lats, lons = grib_message.latlons()
    shape = grib_message.values.shape
    tree = cKDTree(numpy.dstack([lons.ravel(), lats.ravel()])[0])
    for location in locations:
        coords = wkb.loads(bytes(location.location.data))
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
    coordinate_luts = collections.defaultdict(dict)

    # Build up a big array of loc_id -> {valid_time -> [values]}
    loc_time_values = collections.defaultdict(lambda: collections.defaultdict(list))

    for msg in grib:
        field = SourceField.query.filter_by(
            source_id=source.id,
            grib_name=msg.name,
        ).first()

        if field is None:
            continue

        logger.info("Processing message '%s' (field '%s')", msg, field.grib_name)

        if field.id not in coordinate_luts:
            # Ensure the zipcode->coordinate lookup table has been created in-DB for this field
            if CoordinateLookup.query.filter_by(src_field_id=field.id).count() == 0:
                logger.info("Generating coordinate lookup table for field '%s'", field.grib_name)
                entries = []
                for loc_id, x, y in get_location_index_map(msg, Location.query.all()):
                    # Create the DB object
                    lookup_entry = CoordinateLookup()
                    lookup_entry.src_field_id = field.id
                    lookup_entry.location_id = loc_id
                    lookup_entry.x = x
                    lookup_entry.y = y
                    entries.append(lookup_entry)

                    # And cache locally
                    coordinate_luts[field.id][loc_id] = (y, x)

                db.session.bulk_save_objects(entries)
                db.session.commit()
            else:
                # lookup table is in DB, but not cached locally yet
                for entry in CoordinateLookup.query.all():
                    coordinate_luts[field.id][entry.location_id] = (entry.y, entry.x)

            logger.info("Coordinate lookup table loaded")

        if save_rasters:
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

            logger.info("Done saving raster data")

        if save_denormalized:
            grib_data = msg.values

            for loc_id, coords in coordinate_luts[field.id].items():
                loc_time_values[loc_id][msg.validDate].append({
                    "src_field_id": field.id,
                    "run_time": datetime2unix(msg.analDate),
                    "value": float(grib_data[coords]),
                })

    if save_denormalized:
        logger.info("Saving denormalized location/time data for all layers")

        # TODO: multi-process lock here so we can ingest multiple things at once and not have update conflicts here

        for loc_id in loc_time_values:
            loc_data = LocationData.query.filter_by(location_id=loc_id).first()
            if not loc_data:
                loc_data = LocationData(
                    location_id=loc_id,
                    values={},
                )
                db.session.add(loc_data)

            for valid_time in loc_time_values[loc_id]:
                vt_key = datetime2unix(valid_time)
                if vt_key in loc_data.values:
                    loc_data.values[vt_key].extend(loc_time_values[loc_id][valid_time])
                else:
                    loc_data.values[vt_key] = loc_time_values[loc_id][valid_time]

        db.session.commit()

        logger.info("Done saving denormalized data")