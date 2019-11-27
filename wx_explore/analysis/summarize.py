from typing import List, Callable
from datetime import timedelta

import collections

from wx_explore.common.utils import memoize, RangeDict, TimeRange
from wx_explore.common.models import Metric, LocationData


@memoize
def _get_metric(sfid: int) -> Metric:
    return SourceField.query(SourceField.id == sfid).first().metric


def model_aggregator(loc_data_values) -> float:
    """
    Currently uses the median model result, but this could be changed to, for example,
    weight based on how far out we are (preferring high-resolution/short-term models earlier on)
    """
    values = []
    for p in loc_data_values:
        values.extend(p['values'])
    return sorted(values)[len(values)//2]


def combine_models(loc_data: List[LocationData]) -> List[LocationData]:
    """
    Combine data from all models in loc_data into a single, unified model.
    """
    combined_data = []

    for data in model_loc_data:
        points_for_metric = collections.defaultdict(list)  # dictionary of metric id to list of data points

        for data_point in data.values:
            metric = _get_metric(data_point['src_field_id'])
            points_for_metric[metric.id].append(data_point)

        combined_points = []
        for metric_id, data_points in points_for_metric.items():
            combined_points.append({
                "value": model_aggregator(data_points),
                "values": [p['value'] for p in data_points],
            })

        combined = LocationData(
            location_id=data.location_id,
            valid_time=data.valid_time,
            values=combined_points,
        )

    return combined_data


def cluster(
        loc_data: List[LocationData],
        source_field_id: int,
        in_cluster: Callable[[float], bool]=lambda x: bool(x),
        time_threshold=timedelta(hours=3)) -> Iterator[Tuple[datetime, datetime, List[float]]]:
    """
    Clusters values in the given source field in loc_data.
    """
    first = None
    last = None
    values = []

    for data in sorted(loc_data, key=lambda d: d.valid_time):
        for data_point in data.values:
            if data_point['src_field_id'] != source_field_id:
                continue

            v = data_point['value']

            if in_cluster(v) and first is None:
                first = data.valid_time

            if in_cluster(v):
                last = data.valid_time
                values.append(v)

            if not in_cluster(v) and last is not None and data.valid_time - last > time_threshold:
                yield (first, last, values)
                first = None
                last = None
                values = []


def time_of_day(dt):
    TIME_RANGES = RangeDict({
        range(19, 6): 'night',
        range(6, 12): 'morning',
        range(12, 15): 'afternoon',
        range(15, 19): 'evening',
    })

    return TIME_RANGES[dt.hour]


class TemperatureEvent(object):
    time: datetime
    temperature: int


class PrecipEvent(TimeRange):
    precip_type: str


class WindEvent(TimeRange):
    CLASSIFICATIONS = RangeDict({
        range(0, 15): 'light',
        range(15, 30): 'moderate',
        range(30, 50): 'strong',
        range(50, 100): 'gale force',
        range(100, 999): 'hurricane force',
    })

    avg_speed: int
    gust_speed: int

    @property
    def avg_speed_text(self):
        return self.CLASSIFICATIONS[avg_speed]


class SummarizedData(object):
    high: TemperatureEvent
    low: TemperatureEvent
    precip_events: RangeDict[int, PrecipEvent]
    wind_events: RangeDict[int, WindEvent]
    cloud_cover: RangeDict[int, CloudCoverEvent]

    def __init__(self):
        self.high = TemperatureEvent()
        self.low = TemperatureEvent()
        self.precip_events = RangeDict()
        self.wind_events = RangeDict()
        self.cloud_cover = RangeDict()

    def text_summary(self, time=0):
        summary = ""

        pe = self.precip_events.get(time)
        if pe:
            summary += f"{pe.precip_type} through the {time_of_day(pe.end)}"
            for we in self.wind_events:
                if we.start >= pe.start:
                    summary += f", with {we.avg_speed_text} winds gusting to {we.gust_speed}"
            pe2 = self.precip_events.get(pe.end)
            if pe2:
                summary += f", changing into {pe2.precip_type} around {pe2.start.hour}"
        else:
            summary += f"{sky_cond.cover} through the {time_of_day(sky_cond.end)}"
            pe = self.precip_events.get_any(time, sky_cond.end + 1)
            if pe:
                time_modifier = ""
                if pe.end - pe.start < timedelta(hours=1):
                    time_modifier = "brief "
                summary += f", with {time_modifier}{pe.precip_type} starting around {pe.start.hour}"
            # TODO: sky cond + wind

        return summary
