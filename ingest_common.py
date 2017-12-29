#!/usr/bin/env python3
import gdal
import gdalconst
import pygrib
import osr
import logging

from models import *
from raster2pgsql import make_options, wkblify_raster_header, wkblify_band_header, wkblify_band
from app import db

logger = logging.getLogger('ingest_common')


def reproject_to_epsg(dataset, coord_sys=4326):
    # https://gis.stackexchange.com/questions/139906/replicating-result-of-gdalwarp-using-gdal-python-bindings
    dst_srs = osr.SpatialReference()
    dst_srs.ImportFromEPSG(coord_sys)
    dst_wkt = dst_srs.ExportToWkt()

    error_threshold = 0.125
    resampling = gdal.GRA_NearestNeighbour

    tmp_ds = gdal.AutoCreateWarpedVRT(dataset, None, dst_wkt, resampling, error_threshold)

    return tmp_ds


def ingest_grib_file(file_path, source):
    '''
    Ingests a given GRIB file into the backend
    :param file_path: Path to the GRIB file
    :param source: Source object which denotes which source this data is from
    :return: None
    '''
    logger.info("Processing GRIB file '%s'", file_path)

    grib = pygrib.open(file_path)
    ds = reproject_to_epsg(gdal.Open(file_path, gdalconst.GA_ReadOnly))

    grid_size_x = 100
    grid_size_y = 100

    for msg in grib:
        field = SourceField.query.filter_by(
            source_id=source.id,
            grib_name=msg.name).first()

        if field is None:
            continue

        logger.debug("Processing field '%s'", field.grib_name)

        band_id = msg.messagenumber
        opts = make_options(4326, band_id)
        band = ds.GetRasterBand(band_id)

        logger.debug("Processing message '%s'", msg)

        for yoff in range(0, band.YSize, grid_size_y):
            for xoff in range(0, band.XSize, grid_size_x):
                raster = DataRaster()
                raster.source_field_id = field.id
                raster.time = msg.validDate

                wkb = wkblify_raster_header(opts, ds, 1, (xoff, yoff), grid_size_x, grid_size_y)
                wkb += wkblify_band_header(opts, band)
                wkb += wkblify_band(opts, band, 1, xoff, yoff, (grid_size_x, grid_size_y), (grid_size_x, grid_size_y), file_path, band_id)

                raster.rast = wkb.decode('ascii')

                db.session.add(raster)

        db.session.commit()


def get_grib_ranges(idxs, source):
    '''
    Given an index file, return a list of tuples that denote the start and length of each chunk
    of the GRIB that should be downloaded
    :param idxs: Index file as a string
    :param source: Source that the grib is from
    :return: List of (start, length)
    '''
    offsets = []
    last = None
    for line in idxs.split('\n'):
        tokens = line.split(':')
        if len(tokens) < 7:
            continue

        _, offset, _, short_name, level, _, _ = tokens

        offset = int(offset)

        if last is not None:
            offsets.append((last, offset-last))
            last = None

        if SourceField.query.filter_by(
                source_id=source.id,
                idx_short_name=short_name,
                idx_level=level).first():
            last = offset

    return offsets


