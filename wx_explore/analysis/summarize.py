from typing import List, Callable, Iterable, Dict, Iterator, Tuple, Optional, Mapping

import datetime
import numpy

from wx_explore.analysis.helpers import get_metric
from wx_explore.common.utils import (
    RangeDict,
    ContinuousTimeList,
)
from wx_explore.common.models import (
    Metric,
    DataPointSet,
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


def cluster(
        data_points: Iterable[DataPointSet],
        in_cluster: Callable[[float, List[float]], bool] = lambda x, _: bool(x),
        time_threshold=datetime.timedelta(hours=3),
) -> Iterator[Tuple[datetime.datetime, datetime.datetime, List[float]]]:
    """
    Clusters values in the given source field in loc_data.
    """
    first = None
    last = None
    values: List[float] = []

    for data_point in sorted(data_points, key=lambda d: d.valid_time):
        v = data_point.median()

        if not in_cluster(v, values) and last is not None and data_point.valid_time - last > time_threshold:
            yield (first, last, values)
            first = None
            last = None
            values = []

        if in_cluster(v, values) and first is None:
            first = data_point.valid_time

        if in_cluster(v, values):
            last = data_point.valid_time
            values.append(v)


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

    @classmethod
    def clusterer(cls, v: float, prior: List[float]) -> bool:
        return len(prior) == 0 or cls.CLASSIFICATIONS[v] == cls.CLASSIFICATIONS[prior[-1]]

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
            "time": self.time,
            "temperature": self.temperature,
        }


class PrecipEvent(TimeRangeEvent):
    # dbZ
    CLASSIFICATIONS = RangeDict({
        range(15, 30): 'light',
        range(30, 40): 'moderate',
        range(40, 55): 'heavy',
        range(55, 999): 'extreme',
    })

    ptype: str
    intensity: float

    @staticmethod
    def clusterer(refl: float, prior: List[float]) -> bool:
        return refl >= 15 and (len(prior) == 0 or PrecipEvent.CLASSIFICATIONS[refl] == PrecipEvent.CLASSIFICATIONS[prior[-1]])

    def __init__(self, start: datetime.datetime, end: datetime.datetime, ptype: str, intensity: float):
        super().__init__(start, end)
        self.ptype = ptype
        self.intensity = intensity

    def __bool__(self) -> bool:
        return self.intensity > 0

    def dict(self):
        return {**super().dict(), **{
            "type": self.ptype,
            "intensity": self.intensity,
            "intensity_str": self.CLASSIFICATIONS[self.intensity],
        }}


class WindEvent(TimeRangeEvent):
    # XXX: what units are these in?
    CLASSIFICATIONS = RangeDict({
        range(0, 15): 'light',
        range(15, 30): 'moderate',
        range(30, 50): 'strong',
        range(50, 100): 'gale force',
        range(100, 999): 'hurricane force',
    })

    avg_speed: float
    gust_speed: float

    def __init__(self, start: datetime.datetime, end: datetime.datetime, avg_speed: float, gust_speed: float):
        super().__init__(start, end)
        self.avg_speed = avg_speed
        self.gust_speed = gust_speed

    @property
    def avg_speed_text(self):
        return self.CLASSIFICATIONS[self.avg_speed]

    @property
    def gust_speed_text(self):
        return self.CLASSIFICATIONS[self.gust_speed]

    def dict(self):
        return {**super().dict(), **{
            "average": self.avg_speed,
            "average_str": self.avg_speed_text,
            "gust": self.gust_speed,
            "gust_str": self.gust_speed_text,
        }}


class CloudCoverEvent(TimeRangeEvent):
    # https://forecast.weather.gov/glossary.php?letter=p
    CLASSIFICATIONS = RangeDict({
        range(0, 13): 'clear',
        range(13, 38): 'mostly clear',
        range(38, 76): 'partly cloudy',
        range(76, 88): 'mostly cloudy',
        range(88, 101): 'cloudy',
    })

    cover: int

    def __init__(self, start: datetime.datetime, end: datetime.datetime, cover: int):
        super().__init__(start, end)
        self.cover = cover

    def __bool__(self) -> bool:
        return self.cover > 0

    @property
    def cover_text(self):
        return self.CLASSIFICATIONS[self.cover]

    def dict(self):
        return {**super().dict(), **{
            "cover": self.cover,
            "cover_str": self.cover_text,
        }}


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
            resolution: datetime.timedelta = datetime.timedelta(hours=1)
    ):
        self.start = start
        self.end = end
        self.resolution = resolution

        # Bound data points to within the specified start,end
        self.data_points = list(filter(lambda d: start <= d.valid_time < end, data_points))

        # temps and winds are guaranteed to have values for each time interval
        self.temps = ContinuousTimeList(start, end, resolution)
        self.winds = ContinuousTimeList(start, end, resolution)
        # but cloud cover and precip are generated from derived fields which could all be empty
        self.cloud_cover = ContinuousTimeList(start, end, resolution, [
            CloudCoverEvent(start, end, 0) for start, end in self.time_buckets()
        ])
        self.precip = ContinuousTimeList(start, end, resolution, [
            PrecipEvent(start, end, '', 0) for start, end in self.time_buckets()
        ])

        self.low = None
        self.high = None

        self.analyze()

    def analyze(self):
        # Begin analysis
        temp_metric = Metric.query.filter(Metric.name == "2m Temperature").first()
        wind_metric = Metric.query.filter(Metric.name == "10m Wind Speed").first()
        rain_metric = Metric.query.filter(Metric.name == "Rain").first()
        snow_metric = Metric.query.filter(Metric.name == "Snow").first()

        for data_point in self.data_points:
            if data_point.metric_id == temp_metric.id:
                e = TemperatureEvent(data_point.valid_time, data_point.median())
                self.temps[e.time] = e

                if self.low is None or e.temperature < self.low.temperature:
                    self.low = e
                if self.high is None or e.temperature > self.high.temperature:
                    self.high = e

        for start, end, vals in cluster(filter(lambda d: d.metric_id == rain_metric.id, self.data_points), PrecipEvent.clusterer):
            e = PrecipEvent(start, end, 'rain', numpy.median(vals))
            self.precip[e.start:e.end] = e

        for start, end, vals in cluster(filter(lambda d: d.metric_id == snow_metric.id, self.data_points), PrecipEvent.clusterer):
            e = PrecipEvent(start, end, 'snow', numpy.median(vals))
            self.precip[e.start:e.end] = e

        for start, end, vals in cluster(filter(lambda d: d.metric_id == wind_metric.id, self.data_points), WindEvent.clusterer):
            e = WindEvent(start, end, numpy.mean(vals), max(vals))
            self.winds[e.start:e.end] = e

    def time_buckets(self) -> Iterable[Tuple[datetime.datetime, datetime.datetime]]:
        for i in range((self.end - self.start).seconds // self.resolution.seconds):
            yield (
                self.start + (self.resolution * i),
                self.start + (self.resolution * (i+1)),
            )

    def text_summary(self, rel_time=0):
        summary = ""

        pe = self.precip[rel_time]
        if pe is not None:
            summary += f"{pe.ptype} through the {time_of_day(pe.end)}"
            for we in self.winds[pe.start:]:
                if we.avg_speed > 15:  # XXX: arbitrary
                    summary += f", with {we.avg_speed_text} winds gusting to {we.gust_speed}"  # units
            pe2 = self.precip[pe.end]
            if pe2:
                summary += f", changing into {pe2.ptype} around {pe2.start.hour}"
        else:
            sky_cond = self.cloud_cover[rel_time]
            end_time = time_of_day(sky_cond.end)
            if sky_cond.end.hour > 17:
                end_time = "day"
            summary += f"{sky_cond.cover_text} through the {end_time}"
            for pe in self.precip[rel_time:sky_cond.end]:
                if pe:
                    time_modifier = ""
                    if pe.end - pe.start <= datetime.timedelta(hours=2):
                        time_modifier = "brief "
                    summary += f", with {time_modifier}{pe.ptype} starting around {pe.start.hour}"
                    break
            # TODO: sky cond + wind

        return summary

    def dict(self):
        return {
            "high": self.high.dict(),
            "low": self.low.dict(),
            "temps": [e.dict() for e in self.temps],
            "winds": [e.dict() for e in self.wind],
            "cloud_cover": [e.dict() for e in self.cloud_cover],
            "precip": [e.dict() for e in self.precip],
            "text": self.text_summary(),
        }
