#!/usr/bin/env python3
import logging
logging.basicConfig(level=logging.INFO)

from wx_explore.common.models import (
    Source,
    SourceField,
    Location,
    Timezone,
)

from wx_explore.common import metrics
from wx_explore.common.db_utils import get_or_create
from wx_explore.web.core import db


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
    Source(
        short_name='rrfs',
        name='Rapid Refresh Forecast System',
        src_url='https://gsl.noaa.gov/focus-areas/unified_forecast_system/rrfs',
        last_updated=None,
    ),
]

logging.info("Creating sources")
for i, s in enumerate(sources):
    sources[i] = get_or_create(s)


metric_meta = {
    '2m Temperature': {
        'idx_short_name': 'TMP',
        'idx_level': '2 m above ground',
        'selectors': {
            'name': '2 metre temperature',
        },
    },
    'Visibility': {
        'idx_short_name': 'VIS',
        'idx_level': 'surface',
        'selectors': {
            'shortName': 'vis',
        },
    },
    'Rain': {
        'idx_short_name': 'CRAIN',
        'idx_level': 'surface',
        'selectors': {
            'shortName': 'crain',
            'stepType': 'instant',
        },
    },
    'Ice': {
        'idx_short_name': 'CICEP',
        'idx_level': 'surface',
        'selectors': {
            'shortName': 'cicep',
            'stepType': 'instant',
        },
    },
    'Freezing Rain': {
        'idx_short_name': 'CFRZR',
        'idx_level': 'surface',
        'selectors': {
            'shortName': 'cfrzr',
            'stepType': 'instant',
        },
    },
    'Snow': {
        'idx_short_name': 'CSNOW',
        'idx_level': 'surface',
        'selectors': {
            'shortName': 'csnow',
            'stepType': 'instant',
        },
    },
    'Composite Reflectivity': {
        'idx_short_name': 'REFC',
        'idx_level': 'entire atmosphere',
        'selectors': {
            'shortName': 'refc',
        },
    },
    '2m Humidity': {
        'idx_short_name': 'SPFH',
        'idx_level': '2 m above ground',
        'selectors': {
            'name': 'Specific humidity',
            'typeOfLevel': 'heightAboveGround',
            'level': 2,
        },
    },
    'Surface Pressure': {
        'idx_short_name': 'PRES',
        'idx_level': 'surface',
        'selectors': {
            'name': 'Surface pressure',
        },
    },
    '10m Wind U-component': {
        'idx_short_name': 'UGRD',
        'idx_level': '10 m above ground',
    },
    '10m Wind V-component': {
        'idx_short_name': 'VGRD',
        'idx_level': '10 m above ground',
    },
    '10m Wind Speed': {
        'idx_short_name': 'WIND',
        'idx_level': '10 m above ground',
        'selectors': {
            'shortName': 'wind',
            'typeOfLevel': 'heightAboveGround',
            'level': 10,
        },
    },
    '10m Wind Direction': {
        'idx_short_name': 'WDIR',
        'idx_level': '10 m above ground',
        'selectors': {
            'shortName': 'wdir',
            'typeOfLevel': 'heightAboveGround',
            'level': 10,
        },
    },
    'Gust Speed': {
        'idx_short_name': 'GUST',
        'idx_level': 'surface',
        'selectors': {
            'shortName': 'gust',
        },
    },
    'Cloud Cover': {
        'idx_short_name': 'TCDC',
        'idx_level': 'entire atmosphere',
        'selectors': {
            'shortName': 'tcc',
            'typeOfLevel': 'atmosphere',
        },
    },
}

logging.info("Creating source fields")
for src in sources:
    for metric in metrics.ALL_METRICS:
        get_or_create(SourceField(
            source_id=src.id,
            metric_id=metric.id,
            **metric_meta[metric.name],
        ))

# customization
nam_cloud_cover = SourceField.query.filter(
    SourceField.source.has(short_name='nam'),
    SourceField.metric == metrics.cloud_cover,
).first()
nam_cloud_cover.selectors = {'shortName': 'tcc'}

# RRFS index names use "entire atmosphere (considered as a single layer)" instead of just "entire atmosphere"
rrfs_refc = SourceField.query.filter(
    SourceField.source.has(short_name='rrfs'),
    SourceField.metric == metrics.cloud_cover,
).first()
rrfs_refc.idx_level = 'entire atmosphere (considered as a single layer)'
rrfs_cloud_cover = SourceField.query.filter(
    SourceField.source.has(short_name='rrfs'),
    SourceField.metric == metrics.cloud_cover,
).first()
rrfs_cloud_cover.idx_level = 'entire atmosphere (considered as a single layer)'

db.session.commit()


###
# Locations
###
import csv
from shapely import wkt
from shapely.geometry import Point

locs = []

logging.info("Loading locations: ZIP codes")
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

logging.info("Loading locations: world cities")
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

logging.info("Creating locations")
db.session.add_all(locs)
db.session.commit()


###
# Timezones
###
import geoalchemy2
import os
import pygeoif
import requests
import shapefile
import shutil
import tempfile
import zipfile

logging.info("Creating timezones")
with tempfile.TemporaryDirectory() as tmpdir:
    with tempfile.TemporaryFile() as tmpf:
        with requests.get('https://github.com/evansiroky/timezone-boundary-builder/releases/download/2020a/timezones-with-oceans.shapefile.zip', stream=True) as resp:
            shutil.copyfileobj(resp.raw, tmpf)

        with zipfile.ZipFile(tmpf) as z:
            z.extractall(tmpdir)

    shapefile = shapefile.Reader(os.path.join(tmpdir, 'dist/combined-shapefile-with-oceans'))

    tzs = []

    for sr in shapefile.iterShapeRecords():
        tzs.append(Timezone(
            name=sr.record.tzid,
            geom=geoalchemy2.functions.ST_Multi(pygeoif.geometry.as_shape(sr.shape).wkt),
        ))

    db.session.add_all(tzs)
    db.session.commit()
