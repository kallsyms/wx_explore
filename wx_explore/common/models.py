from geoalchemy2 import Geography
from shapely import wkb
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, LargeBinary
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import deferred, relationship
from sqlalchemy.ext.declarative import declarative_base
from typing import List, Optional

import datetime
import numpy


Base = declarative_base()


class Source(Base):
    """
    A specific source data may come from.
    E.g. NEXRAD L2, GFS, NAM, HRRR
    """
    __tablename__ = "source"

    id = Column(Integer, primary_key=True)
    short_name = Column(String(8))
    name = Column(String(128))
    src_url = Column(String(1024))
    last_updated = Column(DateTime)

    # Fields are backref'd

    def serialize(self):
        return {
            "id": self.id,
            "short_name": self.short_name,
            "name": self.name,
            "src_url": self.src_url,
            "last_updated": self.last_updated,
        }

    def __repr__(self):
        return f"<Source id={self.id} short_name='{self.short_name}'>"


class Metric(Base):
    """
    A metric that various source fields can have values for.
    E.g. temperature, precipitation, visibility
    """
    __tablename__ = "metric"

    id = Column(Integer, primary_key=True)
    name = Column(String(128))
    units = Column(String(16))

    def serialize(self):
        return {
            "id": self.id,
            "name": self.name,
            "units": self.units,
        }

    def __repr__(self):
        return f"<Metric id={self.id} name='{self.name}'>"


class SourceField(Base):
    """
    A specific field inside of a source.
    E.g. Composite reflectivity @ entire atmosphere, 2m temps, visibility @ ground
    """
    __tablename__ = "source_field"

    id = Column(Integer, primary_key=True)
    source_id = Column(Integer, ForeignKey('source.id'))
    projection_id = Column(Integer, ForeignKey('projection.id'))
    metric_id = Column(Integer, ForeignKey('metric.id'))

    idx_short_name = Column(String(15))  # e.g. TMP, VIS
    idx_level = Column(String(255))  # e.g. surface, 2 m above ground
    grib_name = Column(String(255))  # e.g. 2 metre temperature

    source = relationship('Source', backref='fields')
    projection = relationship('Projection')
    metric = relationship('Metric', backref='fields')

    def serialize(self):
        return {
            "id": self.id,
            "source_id": self.source_id,
            "grib_name": self.grib_name,
            "metric_id": self.metric_id,
        }

    def __repr__(self):
        return f"<SourceField id={self.id} short_name='{self.idx_short_name}'>"


class Location(Base):
    """
    A specific location that we have a lat/lon for.
    """
    __tablename__ = "location"

    id = Column(Integer, primary_key=True)
    location = Column(Geography('Point,4326'))
    name = Column(String(512))
    population = Column(Integer)

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
            "name": self.name,
            "lon": coords[0],
            "lat": coords[1],
        }

    def __repr__(self):
        return f"<Location id={self.id} name='{self.name}'>"


class Projection(Base):
    """
    Table that holds data about the projection a given ingested file uses.
    """
    __tablename__ = "projection"

    id = Column(Integer, primary_key=True)
    params = Column(JSONB)
    n_x = Column(Integer)
    n_y = Column(Integer)
    lats = deferred(Column(JSONB))
    lons = deferred(Column(JSONB))
    tree = deferred(Column(LargeBinary))

    def shape(self):
        return (self.n_y, self.n_x)


class FileMeta(Base):
    """
    Table that holds metadata about denormalized data in a given file.

    Each file can hold any data (different fields, different sources even) as long
    as it has a single projection.
    """
    __tablename__ = "file_meta"
    file_name = Column(String(4096), primary_key=True)
    projection_id = Column(Integer, ForeignKey('projection.id'))
    ctime = Column(DateTime, default=datetime.datetime.utcnow)
    loc_size = Column(Integer)

    projection = relationship('Projection')


class FileBandMeta(Base):
    """
    Table that holds data about specific runs of denormalized data in the given file.
    """
    __tablename__ = "file_band_meta"

    # TODO: on delete of file meta, delete these
    # PKs
    file_name = Column(String, ForeignKey('file_meta.file_name'), primary_key=True)
    offset = Column(Integer, primary_key=True)  # offset within a (x,y) chunk, _not_ offset in the entire file

    # Metadata used to seek into the file
    vals_per_loc = Column(Integer)

    # Metadata
    source_field_id = Column(Integer, ForeignKey('source_field.id'))
    valid_time = Column(DateTime)
    run_time = Column(DateTime)

    file_meta = relationship('FileMeta', backref='bands', lazy='joined')
    source_field = relationship('SourceField')


class DataPointSet(object):
    """
    Non-db object which holds values and metadata for given data point (loc, time)
    """
    values: List[float]
    metric_id: int
    valid_time: datetime.datetime
    source_field_id: Optional[int]
    run_time: Optional[datetime.datetime]
    derived: bool
    synthesized: bool

    def __init__(
            self,
            values: List[float],
            metric_id: int,
            valid_time: datetime.datetime,
            source_field_id: Optional[int] = None,
            run_time: Optional[datetime.datetime] = None,
            derived: bool = False,
            synthesized: bool = False):
        self.values = values
        self.metric_id = metric_id
        self.valid_time = valid_time

        # Optional fields
        self.source_field_id = source_field_id
        self.run_time = run_time
        self.derived = derived
        self.synthesized = synthesized

    def min(self) -> float:
        return min(self.values)

    def max(self) -> float:
        return max(self.values)

    def median(self) -> float:
        return float(numpy.median(self.values))

    def mean(self) -> float:
        return float(numpy.mean(self.values))
