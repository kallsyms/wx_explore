from app import db


class Source(db.Model):
    '''
    A specific source data may come from.
    E.g. NEXRAD L2, GFS, NAM, HRRR
    '''
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128))
    src_url = db.Column(db.String(1024))
    last_updated = db.Column(db.DateTime)


class Metric(db.Model):
    '''
    A metric that various source fields can have values for.
    E.g. temperature, precipitation, visibility
    '''
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128))
    units = db.Column(db.String(16))


class SourceField(db.Model):
    '''
    A specific field inside of a source.
    E.g. Composite reflectivity @ entire atmosphere, 2m temps, visibility @ ground
    '''
    id = db.Column(db.Integer, primary_key=True)
    source_id = db.Column(db.Integer, db.ForeignKey('source.id'))
    name = db.Column(db.String(64), unique=True)
    type_id = db.Column(db.Integer, db.ForeignKey('metric.id'))

    source = db.relationship('Source', backref='fields')
    type = db.relationship('Metric')


class Location(db.Model):
    '''
    A specific location that we pre-compute data for.
    Currently just zipcodes.
    '''
    id = db.Column(db.Integer, primary_key=True)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    name = db.Column(db.String(512))


class CoordinateLookup(db.Model):
    '''
    Table that holds pre-computed coordinates for a given zipcode in a given source.
    I.e. The zipcode 11111 corresponds to the point (x,y) in HRRR files
    '''
    location_id = db.Column(db.Integer, db.ForeignKey('location.id'), primary_key=True)
    src_field_id = db.Column(db.Integer, db.ForeignKey('source_field.id'), primary_key=True)
    x = db.Column(db.Integer)
    y = db.Column(db.Integer)

    location = db.relationship('Location')
    src_field = db.relationship('SourceField')


class DataPoint(db.Model):
    '''
    A specific point of data for a given source field, location, and time
    E.g. HRRR predicted visibility at noon for 11201 = 10mi
    '''
    src_field_id = db.Column(db.Integer, db.ForeignKey('source_field.id'), primary_key=True)
    location_id = db.Column(db.Integer, db.ForeignKey('location.id'), primary_key=True)
    time = db.Column(db.DateTime, primary_key=True)
    value = db.Column(db.Numeric)

    src_field = db.relationship('SourceField')
    location = db.relationship('Location', backref='data_points')