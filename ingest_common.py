import os
import requests
import numpy
import pygrib
from scipy.spatial import cKDTree
from urlparse import urlparse

from models import *
from app import db


def download(url, folder):
    '''
    Downloads a given URL to the folder
    :param url: The URL to be downloaded
    :param folder: The folder where the downloaded file should be placed
    :return: Path of the downloaded file if the download was successful; False on error
    '''
    r = requests.get(url)
    if r.ok:
        file_name = urlparse(folder).path.split('/')[-1]
        file_path = os.path.join(folder, file_name)

        with open(file_path, 'wb') as fh:
            for block in r.iter_content(4096): # TODO: catch connection errors and retry
                fh.write(block)
        return file_path
    return False


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
        idx = tree.query([location.lon, location.lat])[1]
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

    # TODO: proper logging
    print("Processing GRIB file {}".format(file_path))
    grib = pygrib.open(file_path)
    locations = Location.query.all()

    for field in SourceField.query.filter_by(source_id=source.id).all():
        print("Processing field {}".format(field.name))

        try:
            msgs = grib.select(name=field.name)
        except:
            print("No such field {} in GRIB file {}".format(field, file_path))
            continue

        # Ensure the zipcode->coordinate lookup table has been created for this field
        if CoordinateLookup.query.filter_by(src_field_id=field.id).count() == 0:
            print("Generating coordinate lookup table")
            for loc_id, x, y in get_location_index_map(msgs[0], locations):
                lookup_entry = CoordinateLookup()
                lookup_entry.src_field_id = field.id
                lookup_entry.location_id = loc_id
                lookup_entry.x = x
                lookup_entry.y = y
                db.session.add(lookup_entry)

        db.session.commit()

        # Clear all of the old data
        DataPoint.query.filter_by(src_field_id=field.id).delete()

        # Store the zip->location lookup table locally to avoid tons of DB hits
        lookup_table = {}

        for entry in CoordinateLookup.query.filter_by(src_field_id=field.id).all():
            lookup_table[entry.location_id] = (entry.x, entry.y)

        # Default transformer is the str function/cast
        if field.name not in transformers:
            transformers[field.name] = str

        # Create and add each datapoint for each zip in each time's GRIB message
        for msg in msgs:
            print("Processing GRIB message {}".format(msg.name))
            vals = msg.values

            # Get the new data for each location
            for location in locations:
                data_point = DataPoint()
                data_point.src_field_id = field.id
                data_point.location_id = location.id
                data_point.time = msg.validDate
                x = lookup_table[location.id][0]
                y = lookup_table[location.id][1]
                data_point.value = str(transformers[field.name](vals[y][x]))

                db.session.add(data_point)

        db.session.commit()