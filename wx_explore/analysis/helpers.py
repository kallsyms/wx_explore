from typing import Iterator, List, Dict, Set, Tuple
import datetime
import functools
from wx_explore.common.models import Metric, SourceField, DataPointSet


def get_metric(sfid: int) -> Metric:
    return SourceField.query.get(sfid).metric


def group_by_time(groups: List[List[DataPointSet]]) -> Iterator[Tuple[datetime.datetime, Tuple[DataPointSet, ...]]]:
    """
    Given n lists of data points (one list per source field), return a time
    and a n-tuple of data points which have that valid_time, eliminating times
    where not all source fields have a point for that time.
    """
    pt_by_time: List[Dict[datetime.datetime, DataPointSet]] = []
    for g in groups:
        pt_by_time.append({dp.valid_time: dp for dp in g})

    common_times: Set[datetime.datetime] = functools.reduce(
        lambda acc, s: acc.intersection(s) if acc is not None else s,
        (set(d.keys()) for d in pt_by_time),
        None,
    )

    for t in sorted(common_times):
        yield (t, tuple(d[t] for d in pt_by_time))
