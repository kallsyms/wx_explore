#!/usr/bin/env python3
from scipy.spatial import cKDTree
import gdal
import gdalconst
import logging
import numpy
import pygrib
import requests

from wx_explore.web.data.models import *
from wx_explore.ingest.raster2pgsql import make_options, wkblify_raster_header, wkblify_band_header, wkblify_band
from wx_explore.web import db

logger = logging.getLogger('ingest_common')


def get_location_index_map(grib_message, locations):
    '''
    Generates grid coordinates for each location in locations from the given GRIB message
    :param grib_message: The GRIB message for which the coordinates should be generated
    :param locations: List of locations for which indexes should be generated
    :return: loc_id,x,y tuples for each given input location
    '''
    lat, lon = grib_message.latlons()
    shape = grib_message.values.shape
    tree = cKDTree(numpy.dstack([lon.ravel(), lat.ravel()])[0])
    for location in locations:
        coords = wkb.loads(bytes(location.location.data))
        # TODO: make sure all of these coords are normal degrees, not a weird projection
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
    ds = gdal.Open(file_path, gdalconst.GA_ReadOnly)

    for msg in grib:
        field = SourceField.query.filter_by(
            source_id=source.id,
            grib_name=msg.name,
        ).first()

        if field is None:
            continue

        logger.info("Processing message '%s' (field '%s')", msg, field.grib_name)

        # Ensure the zipcode->coordinate lookup table has been created for this field
        if CoordinateLookup.query.filter_by(src_field_id=field.id).count() == 0:
            logger.info("Generating coordinate lookup table for field '%s'", field.name)
            entries = []
            for loc_id, x, y in get_location_index_map(msg, Location.query.all()):
                lookup_entry = CoordinateLookup()
                lookup_entry.src_field_id = field.id
                lookup_entry.location_id = loc_id
                lookup_entry.x = x
                lookup_entry.y = y
                entries.append(lookup_entry)

            db.session.bulk_save_objects(entries)
            db.session.commit()

        band_id = msg.messagenumber
        opts = make_options(0, band_id)
        band = ds.GetRasterBand(band_id)

        for yoff in range(band.YSize):
            raster = DataRaster()
            raster.source_field_id = field.id
            raster.run_time = msg.analDate
            raster.valid_time = msg.validDate
            raster.row = yoff

            wkb = wkblify_raster_header(opts, ds, 1, (0, yoff), band.XSize, 1)
            wkb += wkblify_band_header(opts, band)
            wkb += wkblify_band(opts, band, 1, 0, yoff, (band.XSize, 1), (band.XSize, 1), file_path, band_id)

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


def reduce_grib(grib_url, idx_url, source):
    for _ in range(10):
        try:
            idxs = requests.get(idx_url).text
            break
        except KeyboardInterrupt:
            raise
        except:
            continue
    else:
        raise Exception("Unable to download idx file!")

    offsets = get_grib_ranges(idxs, source)

    out = b''

    for offset, length in offsets:
        start = offset
        end = offset + length - 1

        for _ in range(3):
            try:
                out += requests.get(grib_url, headers={
                    "Range": f"bytes={start}-{end}"
                }).content
                break
            except KeyboardInterrupt:
                raise
            except:
                continue
        else:
            logger.warning(f"Couldn't get grib range from {start} to {end}. Continuing anyways...")

    return out
