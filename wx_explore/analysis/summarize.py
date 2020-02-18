from typing import List, Iterable, Dict, Tuple, Optional, Mapping, Any

import datetime
import math
import numpy
import itertools

from wx_explore.analysis.helpers import (
    get_metric,
    group_by_time,
)
from wx_explore.common import metrics
from wx_explore.common.models import (
    Metric,
    DataPointSet,
)
from wx_explore.common.utils import (
    RangeDict,
    ContinuousTimeList,
)


def combine_models(model_data: Iterable[DataPointSet]) -> List[DataPointSet]:
    """
    Group data from all models in loc_data by metric, returning one DataPointSet
    for each metric.
    """
    combined_sets: Dict[Tuple[int, datetime.datetime], DataPointSet] = {}

    for model_data_point in model_data:
        metric = get_metric(model_data_point.source_field_id)
        if (metric.id, model_data_point.valid_time) not in combined_sets:
            combined_sets[(metric.id, model_data_point.valid_time)] = DataPointSet(
                values=[],
                metric_id=metric.id,
                valid_time=model_data_point.valid_time,
                synthesized=True
            )

        combined_sets[(metric.id, model_data_point.valid_time)].values.extend(model_data_point.values)

    return list(combined_sets.values())


def time_of_day(dt):
    TIME_RANGES = RangeDict({
        range(0, 6): 'night',
        range(6, 12): 'morning',
        range(12, 15): 'afternoon',
        range(15, 19): 'evening',
        range(19, 24): 'night',
    })

    return TIME_RANGES[dt.hour]


class TimeRangeEvent(object):
    CLASSIFICATIONS: Mapping[float, str]
    start: datetime.datetime
    end: datetime.datetime

    def __init__(self, start: datetime.datetime, end: datetime.datetime):
        self.start = start
        self.end = end

    def __contains__(self, other):
        if isinstance(other, datetime.datetime):
            return self.start <= other < self.end
        elif isinstance(other, type(self)):
            return self.start <= other.start and other.end <= self.end
        else:
            raise ValueError()

    def dict(self):
        return {
            "start": self.start,
            "end": self.end,
        }


class TemperatureEvent(object):
    time: datetime.datetime
    temperature: float

    def __init__(self, time, temperature):
        self.time = time
        self.temperature = temperature

    def dict(self):
        return {
            "temperature": self.temperature,
        }


class WindEvent(object):
    # XXX: what units are these in?
    CLASSIFICATIONS = RangeDict({
        range(0, 15): 'light',
        range(15, 30): 'moderate',
        range(30, 50): 'strong',
        range(50, 100): 'gale force',
        range(100, 999): 'hurricane force',
    })

    DIRECTION_CLASSIFICATION = RangeDict({
        range(0, 23): "N",
        range(23, 68): "NE",
        range(68, 113): "E",
        range(113, 158): "SE",
        range(158, 203): "S",
        range(203, 248): "SW",
        range(248, 293): "W",
        range(293, 338): "NW",
        range(338, 361): "N",
    })

    time: datetime.datetime
    avg_speed: float
    direction: float
    gust_speed: float

    def __init__(self, time: datetime.datetime, avg_speed: float, direction: float, gust_speed: float):
        self.time = time
        self.avg_speed = avg_speed
        self.direction = direction
        self.gust_speed = gust_speed

    @property
    def avg_speed_text(self):
        return self.CLASSIFICATIONS[self.avg_speed]

    @property
    def direction_text(self):
        return self.DIRECTION_CLASSIFICATION[self.direction]

    @property
    def gust_speed_text(self):
        return self.CLASSIFICATIONS[self.gust_speed]

    def dict(self):
        return {
            "average_speed": self.avg_speed,
            "average_speed_str": self.avg_speed_text,
            "direction": self.direction,
            "direction_str": self.direction_text,
            "gust": self.gust_speed,
            "gust_str": self.gust_speed_text,
        }


class CloudCoverEvent(TimeRangeEvent):
    # https://forecast.weather.gov/glossary.php?letter=p
    CLASSIFICATIONS = RangeDict({
        range(0, 13): 'clear',
        range(13, 38): 'mostly clear',
        range(38, 76): 'partly cloudy',
        range(76, 88): 'mostly cloudy',
        range(88, 101): 'cloudy',
    })

    cover: str

    def __init__(self, start: datetime.datetime, end: datetime.datetime, cover: str):
        super().__init__(start, end)
        self.cover = cover

    def __bool__(self) -> bool:
        return self.cover != 'clear'

    def dict(self):
        return {
            "cover": self.cover,
        }


class PrecipEvent(TimeRangeEvent):
    # dbZ
    CLASSIFICATIONS = RangeDict({
        range(-100, 15): '',
        range(15, 30): 'light',
        range(30, 40): 'moderate',
        range(40, 55): 'heavy',
        range(55, 999): 'extreme',
    })

    ptype: str
    intensity: str

    def __init__(self, start: datetime.datetime, end: datetime.datetime, ptype: str, intensity: str):
        super().__init__(start, end)
        self.ptype = ptype
        self.intensity = intensity

    def __bool__(self) -> bool:
        return self.intensity != ''

    def dict(self):
        return {
            "type": self.ptype,
            "intensity": self.intensity,
        }


class SummarizedData(object):
    """
    Represents a summarized view of metrics over the given time span
    """
    start: datetime.datetime
    end: datetime.datetime
    resolution: datetime.timedelta

    data_points: List[DataPointSet]

    # There are two different types of summarized datas:
    # Continuous metrics (one per resolution unit)
    temps: ContinuousTimeList # of TemperatureEvent
    winds: ContinuousTimeList # of WindEvent
    cloud_cover: ContinuousTimeList # of CloudCoverEvent
    precip: ContinuousTimeList # of PrecipEvent

    # And those which summarize the entire time range
    low: Optional[TemperatureEvent]
    high: Optional[TemperatureEvent]

    def __init__(
            self,
            start: datetime.datetime,
            end: datetime.datetime,
            data_points: Iterable[DataPointSet],
            resolution: datetime.timedelta = datetime.timedelta(hours=1),
    ):
        self.start = start
        self.end = end
        self.resolution = resolution

        # Bound data points to within the specified start,end
        self.data_points = sorted(filter(lambda d: start <= d.valid_time < end, data_points), key=lambda p: p.valid_time)

        # temps, winds, and cloud cover are guaranteed to have values for each time interval
        self.temps = ContinuousTimeList(start, end, resolution)
        self.winds = ContinuousTimeList(start, end, resolution)
        self.cloud_cover = ContinuousTimeList(start, end, resolution, [
            CloudCoverEvent(self.start, self.end, 'unknown') for _ in self.time_buckets()
        ])
        # but precip is generated from fields which could all be false/empty
        self.precip = ContinuousTimeList(start, end, resolution, [
            PrecipEvent(start, end, '', PrecipEvent.CLASSIFICATIONS[0]) for start, end in self.time_buckets()
        ])

        self.low = None
        self.high = None

        self.analyze()

    def points_for_metric(self, m: Metric) -> List[DataPointSet]:
        return list(filter(lambda d: d.metric_id == m.id, self.data_points))

    def analyze(self):
        for data_point in self.data_points:
            if data_point.metric_id == metrics.temp.id:
                e = TemperatureEvent(data_point.valid_time, data_point.median())
                self.temps[e.time] = e

                if self.low is None or e.temperature < self.low.temperature:
                    self.low = e
                if self.high is None or e.temperature > self.high.temperature:
                    self.high = e

        for valid_time, (wind_speed, wind_direction, gust_speed) in group_by_time([
                self.points_for_metric(metrics.wind_speed),
                self.points_for_metric(metrics.wind_direction),
                self.points_for_metric(metrics.gust_speed),
        ]):
            e = WindEvent(valid_time, wind_speed.median(), wind_direction.median(), gust_speed.median())
            self.winds[e.time] = e

        for cover, grp in itertools.groupby(self.points_for_metric(metrics.cloud_cover), key=lambda p: CloudCoverEvent.CLASSIFICATIONS[p.median()]):
            grp = list(grp)
            start = grp[0].valid_time
            end = grp[-1].valid_time
            e = CloudCoverEvent(start, end, cover)
            self.cloud_cover[e.start:e.end] = e

        raining = list(filter(lambda d: d.median() == 1, self.points_for_metric(metrics.raining)))
        for intensity, grp in itertools.groupby(
                [(time, rain, refl) for time, (rain, refl) in group_by_time([
                    raining,
                    self.points_for_metric(metrics.composite_reflectivity)])],
                key=lambda t: PrecipEvent.CLASSIFICATIONS[t[2].median()]):
            grp = list(grp)
            start = grp[0][0]
            end = grp[-1][0]
            e = PrecipEvent(start, end, 'rain', intensity)
            self.precip[e.start:e.end] = e

        snowing = list(filter(lambda d: d.median() == 1, self.points_for_metric(metrics.snowing)))
        for intensity, grp in itertools.groupby(
                [(time, snow, refl) for time, (snow, refl) in group_by_time([
                    snowing,
                    self.points_for_metric(metrics.composite_reflectivity)])],
                key=lambda t: PrecipEvent.CLASSIFICATIONS[t[2].median()]):
            grp = list(grp)
            start = grp[0][0]
            end = grp[-1][0]
            e = PrecipEvent(start, end, 'snow', intensity)
            for t, pe in self.precip.enumerate(e.start, e.end):
                if pe.ptype == 'rain':
                    pe.ptype = 'mix'
                else:
                    self.precip[t] = e

        # TODO: ice, freezing rain

    def time_buckets(self) -> Iterable[Tuple[datetime.datetime, datetime.datetime]]:
        for i in range(math.ceil((self.end - self.start).total_seconds() / self.resolution.total_seconds())):
            yield (
                self.start + (self.resolution * i),
                self.start + (self.resolution * (i+1)),
            )

    def summarize(self, rel_time=0) -> List[Dict[str, Any]]:
        components = []

        pe = self.precip[rel_time]
        if pe:
            components.append({
                "type": "precip",
                "metrics": [m.id for m in (metrics.raining, metrics.snowing)],
                "text": pe.ptype,
            })
            components.append({
                "type": "text",
                "text": f"through the {time_of_day(pe.end)}",
            })
            for we in self.winds[pe.start:]:
                if we is not None and we.avg_speed > 15:  # XXX: arbitrary
                    components.append({
                        "type": "text",
                        "text": ", with",
                    })
                    components.append({
                        "type": "wind_speed",
                        "metrics": [metrics.wind_speed.id],
                        "text": we.avg_speed_text,
                    })
                    components.append({
                        "type": "text",
                        "text": "winds",
                    })
                    if we.gust_speed > we.avg_speed + 10:  # XXX: also arbitrary
                        components.append({
                            "type": "text",
                            "text": "gusting to",
                        })
                        components.append({
                            "type": "gust_speed",
                            "metrics": [metrics.gust_speed.id],
                            "text": we.gust_speed,
                        })
                    break
            pe2 = self.precip[pe.end]
            if pe2:
                components.append({
                    "type": "text",
                    "text": ", changing into",
                })
                components.append({
                    "type": "precip",
                    "metrics": [m.id for m in (metrics.raining, metrics.snowing)],
                    "text": pe2.ptype,
                })
                components.append({
                    "type": "text",
                    "text": f"around {pe2.start.hour}",
                })
        else:
            sky_cond = self.cloud_cover[rel_time]
            end_time = time_of_day(sky_cond.end)
            if sky_cond.end.hour > 17:
                end_time = "day"
            components.append({
                "type": "cloud_cover",
                "metrics": [metrics.cloud_cover.id],
                "text": sky_cond.cover,
            })
            components.append({
                "type": "text",
                "text": f"through the {end_time}",
            })
            for pe in self.precip[rel_time:sky_cond.end]:
                if pe:
                    time_modifier = ""
                    if pe.end - pe.start <= datetime.timedelta(hours=2):
                        time_modifier = "brief "
                    components.append({
                        "type": "text",
                        "text": f", with {time_modifier}",
                    })
                    components.append({
                        "type": "precip",
                        "metrics": [m.id for m in (metrics.raining, metrics.snowing)],
                        "text": pe.ptype,
                    })
                    components.append({
                        "type": "text",
                        "text": f"starting around {pe.start.hour}",
                    })
                    break
            # TODO: sky cond + wind

        return components

    def dict(self):
        summary = self.summarize()
        text_summary = ' '.join(c['text'] for c in summary)

        return {
            "high": self.high.dict() if self.high else None,
            "low": self.low.dict() if self.low else None,
            "temps": [e.dict() if e is not None else None for e in self.temps],
            "winds": [e.dict() if e is not None else None for e in self.winds],
            "cloud_cover": [e.dict() if e is not None else None for e in self.cloud_cover],
            "precip": [e.dict() if e is not None else None for e in self.precip],
            "summary": {
                "components": summary,
                "full_text": text_summary,
            },
        }
