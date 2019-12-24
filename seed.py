#!/usr/bin/env python3

from wx_explore.common.models import (
    Metric,
    Source,
    SourceField,
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
    )
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
]

db.session.add_all(source_fields)
db.session.commit()
