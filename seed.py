#!/usr/bin/env python3
from wx_explore.common.models import (
    Source,
    SourceField,
    Metric,
    Location,
    Timezone,
)

from wx_explore.common import metrics
from wx_explore.common.db_utils import get_or_create
from wx_explore.ingest.common import get_source_modules
from wx_explore.web.core import db


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
    'metar': Source(
        short_name='metar',
        name='METAR',
        src_url='https://www.aviationweather.gov/metar',
        last_updated=None,
    ),
}

assert len(sources) == len(get_source_modules())

for source_name, source in sources.items():
    assert source_name in get_source_modules().keys()
    sources[source_name] = get_or_create(source)


grib_metric_meta = {
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

for src in (sources[name] for name in ('hrrr', 'nam', 'gfs')):
    for metric in metrics.ALL_METRICS:
        get_or_create(SourceField(
            source_id=src.id,
            metric_id=metric.id,
            **grib_metric_meta[metric.name],
        ))

# GRIB customization
nam_cloud_cover = SourceField.query.filter(
    SourceField.source.has(short_name='nam'),
    SourceField.metric == metrics.cloud_cover,
).first()
nam_cloud_cover.selectors = {'shortName': 'tcc'}


# METAR
metar_metric_meta = {
    '2m Temperature': {
        'idx_short_name': 'temp_c',
    },
    'Visibility': {
        'idx_short_name': 'visibility_statute_mi',
    },
    'Surface Pressure': {
        'idx_short_name': 'sea_level_pressure_mb',
    },
    '10m Wind Speed': {
        'idx_short_name': 'wind_speed_kt',
    },
    '10m Wind Direction': {
        'idx_short_name': 'wind_dir_degrees',
    },
    'Gust Speed': {
        'idx_short_name': 'wind_gust_kt',
    },
}

for metric_name, meta in metar_metric_meta.items():
    metric = Metric.query.filter(Metric.name == metric_name).first()
    get_or_create(SourceField(
        source_id=sources['metar'].id,
        metric_id=metric.id,
        **metar_metric_meta[metric.name],
    ))


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


###
# Timezones
###
import os
import osgeo.ogr
import requests
import shutil
import tempfile
import zipfile

with tempfile.TemporaryDirectory() as tmpdir:
    with tempfile.TemporaryFile() as tmpf:
        with requests.get('https://github.com/evansiroky/timezone-boundary-builder/releases/download/2020a/timezones-with-oceans.shapefile.zip', stream=True) as resp:
            shutil.copyfileobj(resp.raw, tmpf)

        with zipfile.ZipFile(tmpf) as z:
            z.extractall(tmpdir)

    shapefile = osgeo.ogr.Open(os.path.join(tmpdir, 'dist'))
    layer = shapefile.GetLayer(0)

    tzs = []

    for feature in (layer.GetFeature(i) for i in range(layer.GetFeatureCount())):
        tzs.append(Timezone(
            name=feature.GetField("tzid"),
            geom=feature.GetGeometryRef().ExportToWkt(),
        ))

    db.session.add_all(tzs)
    db.session.commit()
