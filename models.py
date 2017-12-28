#!/usr/bin/env python3
from app import db
from geoalchemy2 import Geography
from shapely import wkb


class Source(db.Model):
    '''
    A specific source data may come from.
    E.g. NEXRAD L2, GFS, NAM, HRRR
    '''
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128))
    src_url = db.Column(db.String(1024))
    last_updated = db.Column(db.DateTime)

    def serialize(self):
        return {"id": self.id,
                "name": self.name,
                "src_url": self.src_url,
                "last_updated": self.last_updated}


class Metric(db.Model):
    '''
    A metric that various source fields can have values for.
    E.g. temperature, precipitation, visibility
    '''
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128))
    units = db.Column(db.String(16))

    def serialize(self):
        return {"id": self.id,
                "name": self.name,
                "units": self.units}


class SourceField(db.Model):
    '''
    A specific field inside of a source.
    E.g. Composite reflectivity @ entire atmosphere, 2m temps, visibility @ ground
    '''
    id = db.Column(db.Integer, primary_key=True)
    source_id = db.Column(db.Integer, db.ForeignKey('source.id'))
    name = db.Column(db.String(255), unique=True)
    metric_id = db.Column(db.Integer, db.ForeignKey('metric.id'))
    band_id = db.Column(db.Integer)  # The band number in the underlying gridded data

    source = db.relationship('Source', backref='fields')
    metric = db.relationship('Metric')

    def serialize(self):
        return {"id": self.id,
                "source_id": self.source_id,
                "name": self.name,
                "metric_id": self.metric_id}


class Location(db.Model):
    '''
    A specific location that we pre-compute data for.
    Currently just zipcodes.
    '''
    id = db.Column(db.Integer, primary_key=True)
    location = db.Column(Geography('Point,4326'))
    name = db.Column(db.String(512))

    def get_coords(self):
        '''
        :return: lon, lat
        '''
        point = wkb.loads(bytes(self.location.data))
        return point.x, point.y

    def serialize(self):
        coords = self.get_coords()

        return {"id": self.id,
                "lon": coords[0],
                "lat": coords[1],
                "name": self.name}


class CoordinateLookup(db.Model):
    '''
    Table that holds pre-computed coordinates for a given zipcode in a given source.
    E.g. The zipcode 11111 corresponds to the point (x,y) in HRRR files
    '''
    location_id = db.Column(db.Integer, db.ForeignKey('location.id'), primary_key=True)
    src_field_id = db.Column(db.Integer, db.ForeignKey('source_field.id'), primary_key=True)
    x = db.Column(db.Integer)
    y = db.Column(db.Integer)

    location = db.relationship('Location')
    src_field = db.relationship('SourceField')