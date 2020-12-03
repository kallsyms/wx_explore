from typing import Dict, List, Optional

import collections
import datetime
import logging
import lxml.etree
import metpy.interpolate
import numpy
import pyproj
import requests

from wx_explore.common import storage, tracing
from wx_explore.common.logging import init_sentry
from wx_explore.common.models import Source, SourceField
from wx_explore.ingest.common import get_or_create_projection
from wx_explore.ingest.sources.source import IngestSource
from wx_explore.web.core import db

logger = logging.getLogger(__name__)


class METAR(IngestSource):
    SOURCE_NAME = "metar"
    TRANSFORMERS = {
        'temp_c': lambda temp: temp + 273.15,  # C to K
        'sea_level_pressure_mb': lambda mb: mb * 100,  # mb to Pa
        'wind_speed_kt': lambda kt: kt / 1.944,  # kt to m/s
        'wind_gust_kt': lambda kt: kt / 1.944,
        'visibility_statute_mi': lambda mi: mi * 1609,  # mile to meter
        'wind_dir_degrees': lambda deg: deg,
    }

    @staticmethod
    def ingest():
        logger.info("Ingesting METAR")

        run_time = datetime.datetime.utcnow()

        metar_xml = requests.get('https://www.aviationweather.gov/adds/dataserver_current/current/metars.cache.xml').content
        root = lxml.etree.fromstring(metar_xml)
        metars = root.xpath('//data/METAR')

        lats = []
        lons = []
        field_data: Dict[SourceField, List[Optional[float]]] = {}

        source = Source.query.filter(Source.short_name == METAR.SOURCE_NAME).first()

        for field in SourceField.query.filter(SourceField.source_id == source.id).all():
            field_data[field] = []

        with tracing.start_span("xml extraction") as span:
            for metar in metars:
                lat = metar.find('latitude')
                if lat is None:
                    continue
                lats.append(float(lat.text))

                lon = metar.find('longitude')
                if lon is None:
                    continue
                lons.append(float(lon.text))

                for field in field_data:
                    v = metar.find(field.idx_short_name)
                    if v is None:
                        field_data[field].append(numpy.nan)
                        continue

                    val = METAR.TRANSFORMERS[field.idx_short_name](float(v.text))
                    field_data[field].append(val)

        logger.info("Interpolating to grids")

        # Gridding and interpolation code derived from https://www.youtube.com/watch?v=M-6rLqk_XA8

        # Transform from normal lat,lon to cartesian (equidistant cylindrical)
        plats, plons = map(numpy.array, pyproj.transform(pyproj.Proj('epsg:4326'), pyproj.Proj('epsg:4087'), lats, lons))

        # Most of this is just interpolate_to_grid with the grid generation extracted
        # https://github.com/Unidata/MetPy/blob/1547ed1c7818fc7b59048f5cd52297120a556943/src/metpy/interpolate/grid.py#L292

        # set to hres=10000 for ~(4005, 1892) grid assuming the entire world was covered by metar station lat/lons
        grid_x, grid_y = metpy.interpolate.grid.generate_grid(10000, metpy.interpolate.grid.get_boundary_coords(plats, plons))
        grid_coords = metpy.interpolate.grid.generate_grid_coords(grid_x, grid_y)

        # transform grid coords back to normal lat,lon for projection creation
        grid_lat, grid_lon = pyproj.transform(pyproj.Proj('epsg:4087'), pyproj.Proj('epsg:4326'), grid_x, grid_y)
        proj = get_or_create_projection(grid_lat, grid_lon)

        to_insert = collections.defaultdict(dict)
        for field, values in field_data.items():
            if field.projection_id is None:
                field.projection_id = proj.id
            elif field.projection_id != proj.id:
                logger.warning("Projection change in %s field", field.name)

            db.session.commit()

            with tracing.start_span("point to grid interpolation") as span:
                span.set_attribute('field', str(field))

                # mask out nan values
                masked_plats, masked_plons, obs = metpy.interpolate.remove_nan_observations(plats, plons, numpy.array(values))
                masked_obs_coords = numpy.array(list(zip(masked_plats, masked_plons)))

                int_obs = metpy.interpolate.interpolate_to_points(masked_obs_coords, obs, grid_coords, interp_type='cressman').reshape(grid_x.shape)
                to_insert[proj][(field.id, run_time, run_time)] = [int_obs]

        logger.info("Saving grids")

        with tracing.start_span("save denormalized"):
            for proj, grids in to_insert.items():
                storage.get_provider().put_fields(proj, grids)

        source.last_updated = run_time
        db.session.commit()


if __name__ == "__main__":
    init_sentry()
    logging.basicConfig(level=logging.INFO)

    METAR.ingest()
