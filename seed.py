#!/usr/bin/env python3

from wx_explore.common.models import (
    Metric,
    Source,
    SourceField,
    Location,
)

from wx_explore.web import db

db.create_all()

metrics = {
    'tmp': Metric(
        name='2m Temperature',
        units='C',
    ),
    'vis': Metric(
        name='Visibility',
        units='m',
    ),
    'rain': Metric(
        name='Raining',
        units='',
    ),
    'snow': Metric(
        name='Snowing',
        units='',
    ),
}

db.session.add_all(metrics.values())
db.session.commit()

sources = {
    'hrrr': Source(
        short_name='hrrr',
        name='HRRR 2D Surface Data (Sub-Hourly)',
        src_url='http://www.nco.ncep.noaa.gov/pmb/products/hrrr/',
        last_updated=None,
    ),
    'nam': Source(
        short_name='nam',
        name='North American Model',
        src_url='https://www.nco.ncep.noaa.gov/pmb/products/nam/',
        last_updated=None,
    ),
    'gfs': Source(
        short_name='gfs',
        name='Global Forecast System',
        src_url='https://www.nco.ncep.noaa.gov/pmb/products/gfs/',
        last_updated=None,
    ),
}

db.session.add_all(sources.values())
db.session.commit()

source_fields = [
    SourceField(
        source_id=sources['hrrr'].id,
        idx_short_name='TMP',
        metric_id=metrics['tmp'].id,
        idx_level='2 m above ground',
        grib_name='2 metre temperature',
    ),
    SourceField(
        source_id=sources['hrrr'].id,
        idx_short_name='VIS',
        metric_id=metrics['vis'].id,
        idx_level='surface',
        grib_name='Visibility',
    ),
    SourceField(
        source_id=sources['hrrr'].id,
        idx_short_name='CRAIN',
        metric_id=metrics['rain'].id,
        idx_level='surface',
        grib_name='Categorical rain',
    ),
    SourceField(
        source_id=sources['hrrr'].id,
        idx_short_name='CSNOW',
        metric_id=metrics['snow'].id,
        idx_level='surface',
        grib_name='Categorical snow',
    ),

    SourceField(
        source_id=sources['nam'].id,
        idx_short_name='TMP',
        metric_id=metrics['tmp'].id,
        idx_level='2 m above ground',
        grib_name='2 metre temperature',
    ),
    SourceField(
        source_id=sources['nam'].id,
        idx_short_name='VIS',
        metric_id=metrics['vis'].id,
        idx_level='surface',
        grib_name='Visibility',
    ),
    SourceField(
        source_id=sources['nam'].id,
        idx_short_name='CRAIN',
        metric_id=metrics['rain'].id,
        idx_level='surface',
        grib_name='Categorical rain',
    ),
    SourceField(
        source_id=sources['nam'].id,
        idx_short_name='CSNOW',
        metric_id=metrics['snow'].id,
        idx_level='surface',
        grib_name='Categorical snow',
    ),

    SourceField(
        source_id=sources['gfs'].id,
        idx_short_name='TMP',
        metric_id=metrics['tmp'].id,
        idx_level='2 m above ground',
        grib_name='2 metre temperature',
    ),
    SourceField(
        source_id=sources['gfs'].id,
        idx_short_name='VIS',
        metric_id=metrics['vis'].id,
        idx_level='surface',
        grib_name='Visibility',
    ),
    SourceField(
        source_id=sources['gfs'].id,
        idx_short_name='CRAIN',
        metric_id=metrics['rain'].id,
        idx_level='surface',
        grib_name='Categorical rain',
    ),
    SourceField(
        source_id=sources['gfs'].id,
        idx_short_name='CSNOW',
        metric_id=metrics['snow'].id,
        idx_level='surface',
        grib_name='Categorical snow',
    ),
]

db.session.add_all(source_fields)
db.session.commit()


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
