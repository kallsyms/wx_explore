#!/usr/bin/env python3
import gdal
import gdalconst
import numpy
import pygrib
import logging
from scipy.spatial import cKDTree

from models import *
from raster2pgsql import make_options, wkblify_raster_header, wkblify_band_header, wkblify_band
from app import db

logger = logging.getLogger('ingest_common')


def get_location_index_map(grib_message, locations):
    '''
    Generates grid coordinates for each location in locations from the given GRIB message
    :param grib_message: The GRIB message for which the coordinates should be generated
    :param locations: List of locations for which indexes should be generated
    :return: x,y tuples for each given input location
    '''
    lat, lon = grib_message.latlons()
    shape = grib_message.values.shape
    tree = cKDTree(numpy.dstack([lon.ravel(), lat.ravel()])[0])
    for location in locations:
        coords = wkb.loads(bytes(location.location.data))
        idx = tree.query([coords.x, coords.y])[1]
        x = idx % shape[1]
        y = idx / shape[1]
        yield (location.id, x, y)


def ingest_grib_file(file_path, source):
    '''
    Ingests a given GRIB file into the backend
    :param file_path: Path to the GRIB file
    :param source: Source object which denotes which source this data is from
    :return: None
    '''

    logger.info("Processing GRIB file '%s'", file_path)
    grib = pygrib.open(file_path)
    locations = Location.query.with_entities(Location.id, Location.location).all()

    for field in SourceField.query.filter_by(source_id=source.id).all():
        logger.debug("Processing field '%s'", field.name)

        try:
            msg = grib.message(field.band_id)
        except:
            logger.warning("No such field '%s' in GRIB file %s", field, file_path)
            continue

        # Ensure the zipcode->coordinate lookup table has been created for this field
        if CoordinateLookup.query.filter_by(src_field_id=field.id).count() == 0:
            logger.info("Generating coordinate lookup table for field '%s'", field.name)
            entries = []
            for loc_id, x, y in get_location_index_map(msg, locations):
                lookup_entry = CoordinateLookup()
                lookup_entry.src_field_id = field.id
                lookup_entry.location_id = loc_id
                lookup_entry.x = x
                lookup_entry.y = y
                entries.append(lookup_entry)

            db.session.bulk_save_objects(entries)
            db.session.commit()

        # Store the zip->location lookup table locally to avoid tons of DB hits
        lookup_table = {}

        for entry in CoordinateLookup.query.filter_by(src_field_id=field.id).all():
            lookup_table[entry.location_id] = (entry.x, entry.y)

        raster = DataRaster()
        raster.source_field_id = field.id
        raster.time = msg.validDate

        opts = make_options(4326, field.band_id)
        ds = gdal.Open(file_path, gdalconst.GA_ReadOnly)
        band = ds.GetRasterBand(field.band_id)
        wkb = wkblify_raster_header(opts, ds, 1, (field.band_id, field.band_id + 1))
        wkb += wkblify_band_header(opts, band)
        wkb += wkblify_band(opts, band, 1, 0, 0, (ds.RasterXSize, ds.RasterYSize), (ds.RasterXSize, ds.RasterYSize),
                            file_path, field.band_id)

        raster.rast = wkb.decode('ascii')

        db.session.add(raster)

        db.session.commit()