#!/usr/bin/env python3
from geoalchemy2 import Geography, Raster
from shapely import wkb

from wx_explore.web import db


class Source(db.Model):
    """
    A specific source data may come from.
    E.g. NEXRAD L2, GFS, NAM, HRRR
    """
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128))
    src_url = db.Column(db.String(1024))
    last_updated = db.Column(db.DateTime)

    # Fields are backref'd

    def serialize(self):
        return {
            "id": self.id,
            "name": self.name,
            "src_url": self.src_url,
            "last_updated": self.last_updated,
        }

    def __repr__(self):
        return f"<Source id={self.id} name='{self.name}'>"


class Metric(db.Model):
    """
    A metric that various source fields can have values for.
    E.g. temperature, precipitation, visibility
    """
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128))
    units = db.Column(db.String(16))

    def serialize(self):
        return {
            "id": self.id,
            "name": self.name,
            "units": self.units,
        }

    def __repr__(self):
        return f"<Metric id={self.id} name='{self.name}'>"


class SourceField(db.Model):
    """
    A specific field inside of a source.
    E.g. Composite reflectivity @ entire atmosphere, 2m temps, visibility @ ground
    """
    id = db.Column(db.Integer, primary_key=True)
    source_id = db.Column(db.Integer, db.ForeignKey('source.id'))

    idx_short_name = db.Column(db.String(15))  # e.g. TMP, VIS
    idx_level = db.Column(db.String(255))  # e.g. surface, 2 m above ground
    grib_name = db.Column(db.String(255))  # e.g. 2 metre temperature

    metric_id = db.Column(db.Integer, db.ForeignKey('metric.id'))

    source = db.relationship('Source', backref='fields')
    metric = db.relationship('Metric', backref='fields')

    def serialize(self):
        return {
            "id": self.id,
            "source_id": self.source_id,
            "grib_name": self.grib_name,
            "metric_id": self.metric_id,
        }

    def __repr__(self):
        return f"<SourceField id={self.id} name='{self.name}'>"


class Location(db.Model):
    """
    A specific location that we have a lat/lon for.
    Currently just zipcodes.
    """
    id = db.Column(db.Integer, primary_key=True)
    location = db.Column(Geography('Point,4326'))
    name = db.Column(db.String(512))

    def get_coords(self):
        """
        :return: lon, lat
        """
        point = wkb.loads(bytes(self.location.data))
        return point.x, point.y

    def serialize(self):
        coords = self.get_coords()

        return {
            "id": self.id,
            "lon": coords[0],
            "lat": coords[1],
            "name": self.name,
        }

    def __repr__(self):
        return f"<Location id={self.id} name='{self.name}'>"


class CoordinateLookup(db.Model):
    """
    Table that holds a lookup from location to grid x,y for the given source field.
    """
    src_field_id = db.Column(db.Integer, db.ForeignKey('source_field.id'), primary_key=True)
    location_id = db.Column(db.Integer, db.ForeignKey('location.id'), primary_key=True)
    x = db.Column(db.Integer)
    y = db.Column(db.Integer)

    src_field = db.relationship('SourceField')
    location = db.relationship('Location')


class DataRaster(db.Model):
    """
    Table that holds the "raw" raster data.
    """
    source_field_id = db.Column(db.Integer, db.ForeignKey('source_field.id'), primary_key=True)
    valid_time = db.Column(db.DateTime, primary_key=True)
    run_time = db.Column(db.DateTime, primary_key=True)
    row = db.Column(db.Integer, primary_key=True)
    rast = db.Column(Raster)

    src_field = db.relationship('SourceField')

    def __repr__(self):
        return f"<DataRaster source_field_id={self.source_field_id} valid_time={self.valid_time} run_time={self.run_time} row={self.row}>"


class LocationTimeData(db.Model):
    """
    Table that holds denormalized data for a given location and time.

    The data stored is a json list of objects, each of which have the SourceField they come from,
    the run time of the model, and the actual field value.
    """
    location_id = db.Column(db.Integer, db.ForeignKey('location.id'), primary_key=True)
    valid_time = db.Column(db.DateTime, primary_key=True)
    values = db.Column(db.JSON)
