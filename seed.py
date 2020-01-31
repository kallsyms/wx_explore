#!/usr/bin/env python3
from sqlalchemy import PrimaryKeyConstraint, UniqueConstraint

from wx_explore.common.models import (
    Metric,
    Source,
    SourceField,
    Location,
)

from wx_explore.web import db

db.create_all()


def get_or_create(obj):
    """
    Gets the specified object from the DB using the values of primary and unique keys for lookup,
    or creates the object in the database and returns it.
    """
    typ = type(obj)
    q = db.session.query(typ)
    constraints = [c for c in typ.__table__.constraints if isinstance(c, (PrimaryKeyConstraint, UniqueConstraint))]
    for col in typ.__table__.columns:
        if getattr(obj, col.name) is not None:
            if any(ccol is col for constraint in constraints for ccol in constraint.columns):
                q = q.filter(getattr(typ, col.name) == getattr(obj, col.name))

    instance = q.first()
    if instance is not None:
        return instance
    else:
        db.session.add(obj)
        # Commit so the result is guaranteed to have an id if applicable
        db.session.commit()
        return obj


metrics = [
    Metric(
        name='2m Temperature',
        units='K',
    ),
    Metric(
        name='Visibility',
        units='m',
    ),
    Metric(
        name='Raining',
        units='',
    ),
    Metric(
        name='Ice',
        units='',
    ),
    Metric(
        name='Freezing Rain',
        units='',
    ),
    Metric(
        name='Snowing',
        units='',
    ),
    Metric(
        name='Composite Reflectivity',
        units='dbZ',
    ),
    Metric(
        name='Gust Speed',
        units='m/s',
    ),
    Metric(
        name='Humidity',
        units='kg/kg',
    ),
    Metric(
        name='Pressure',
        units='Pa',
    ),
    Metric(
        name='U-Component of Wind',
        units='m/s',
    ),
    Metric(
        name='V-Component of Wind',
        units='m/s',
    ),
    Metric(
        name='Wind Direction',
        units='deg',
    ),
    Metric(
        name='Wind Speed',
        units='m/s',
    ),
]

for i, m in enumerate(metrics):
    metrics[i] = get_or_create(m)


sources = [
    Source(
        short_name='hrrr',
        name='HRRR 2D Surface Data (Sub-Hourly)',
        src_url='http://www.nco.ncep.noaa.gov/pmb/products/hrrr/',
        last_updated=None,
    ),
    Source(
        short_name='nam',
        name='North American Model',
        src_url='https://www.nco.ncep.noaa.gov/pmb/products/nam/',
        last_updated=None,
    ),
    Source(
        short_name='gfs',
        name='Global Forecast System',
        src_url='https://www.nco.ncep.noaa.gov/pmb/products/gfs/',
        last_updated=None,
    ),
]

for i, s in enumerate(sources):
    sources[i] = get_or_create(s)


metric_meta = {
    '2m Temperature': {
        'idx_short_name': 'TMP',
        'idx_level': '2 m above ground',
        'grib_name': '2 metre temperature',
    },
    'Visibility': {
        'idx_short_name': 'VIS',
        'idx_level': 'surface',
        'grib_name': 'Visibility',
    },
    'Rain': {
        'idx_short_name': 'CRAIN',
        'idx_level': 'surface',
        'grib_name': 'Categorical rain',
    },
    'Ice': {
        'idx_short_name': 'CICEP',
        'idx_level': 'surface',
        'grib_name': 'Categorical ice pellets',
    },
    'Freezing Rain': {
        'idx_short_name': 'CFRZR',
        'idx_level': 'surface',
        'grib_name': 'Categorical freezing rain',
    },
    'Snow': {
        'idx_short_name': 'CSNOW',
        'idx_level': 'surface',
        'grib_name': 'Categorical snow',
    },
    'Composite Reflectivity': {
        'idx_short_name': 'REFC',
        'idx_level': 'entire atmosphere',
        'grib_name': 'Maximum/Composite radar reflectivity',
    },
    '2m Humidity': {
        'idx_short_name': 'SPFH',
        'idx_level': '2 m above ground',
        'grib_name': 'Specific humidity',
    },
    'Surface Pressure': {
        'idx_short_name': 'PRES',
        'idx_level': 'surface',
        'grib_name': 'Surface pressure',
    },
    '10m Wind Direction': {
        'idx_short_name': 'WDIR',
        'idx_level': '10 m above ground',
        'grib_name': '10 metre wind direction',
    },
    '10m Wind Speed': {
        'idx_short_name': 'WIND',
        'idx_level': '10 m above ground',
        'grib_name': '10 metre wind speed',
    },
    'Gust Speed': {
        'idx_short_name': 'GUST',
        'idx_level': 'surface',
        'grib_name': 'Wind speed (gust)',
    },
}

for src in sources:
    for metric in metrics:
        get_or_create(SourceField(
            source_id=src.id,
            metric_id=metric.id,
            **metric_meta[metric.name],
        ))


###
# Locations
###
import csv
from shapely import wkt
from shapely.geometry import Point

locs = []

with open("data/zipcodes/US.txt", encoding="utf8") as f:
    rd = csv.reader(f, delimiter='\t', quotechar='"')
    for row in rd:
        if not row[3]:
            continue

        name = row[2] + ', ' + row[3] + ' (' + row[1] + ')'
        lat = float(row[9])
        lon = float(row[10])
        locs.append(Location(
            name=name,
            location=wkt.dumps(Point(lon, lat)),
        ))

with open("data/cities/worldcities.csv", encoding="utf8") as f:
    f.readline()  # skip header line
    rd = csv.reader(f)
    for row in rd:
        name = row[0] + ', ' + row[7]
        lat = float(row[2])
        lon = float(row[3])
        population = None
        if row[9]:
            population = int(float(row[9]))
        locs.append(Location(
            name=name,
            location=wkt.dumps(Point(lon, lat)),
            population=population,
        ))

db.session.add_all(locs)
db.session.commit()
