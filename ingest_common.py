import numpy
import pygrib
import logging
from scipy.spatial import cKDTree
from wget import download

from models import *
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


def ingest_grib_file(file_path, source, transformers={}):
    '''
    Ingests a given GRIB file into the backend
    :param file_path: Path to the GRIB file
    :param source: Source object which denotes which source this data is from
    :param transformers: dictionary of field->functions which are used to transform the data
    :return: None
    '''

    logger.info("Processing GRIB file '%s'", file_path)
    grib = pygrib.open(file_path)
    locations = Location.query.with_entities(Location.id, Location.location).all()

    for field in SourceField.query.filter_by(source_id=source.id).all():
        logger.debug("Processing field '%s'", field.name)

        try:
            msgs = grib.select(name=field.name)
        except:
            logger.warning("No such field '%s' in GRIB file %s", field, file_path)
            continue

        # Ensure the zipcode->coordinate lookup table has been created for this field
        if CoordinateLookup.query.filter_by(src_field_id=field.id).count() == 0:
            logger.info("Generating coordinate lookup table for field '%s'", field.name)
            entries = []
            for loc_id, x, y in get_location_index_map(msgs[0], locations):
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

        # Add all points to a local array and then insert them in bulk for perf
        data_points = []

        # Create and add each datapoint for each zip in each time's GRIB message
        for msg in msgs:
            logger.info("Processing GRIB message '%s' for %s", msg.name, msg.validDate)

            vals = msg.values

            # Get the new data for each location
            for location in locations:
                data_point = DataPoint()
                data_point.src_field_id = field.id
                data_point.location_id = location.id
                data_point.time = msg.validDate
                x, y = lookup_table[location.id]

                # If a custom data transformer was specified, invoke it
                if field.name in transformers:
                    val = transformers[field.name](vals[y][x], msg)
                else:
                    val = float(vals[y][x])

                if val is not None:
                    data_point.value = val
                    data_points.append(data_point)

        # Clear all of the old data
        DataPoint.query.filter_by(src_field_id=field.id).delete()

        db.session.bulk_save_objects(data_points)
        db.session.commit()