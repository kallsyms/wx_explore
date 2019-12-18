from wx_explore.common.utils import RangeDict, TimeRange


class PrecipEvent(TimeRange):
    precip_type: str


def classify_precip(loc_data: List[LocationData]) -> List[LocationData]:
    """
    Classifies different categorical precipitation fields (rain, freezing rain, ice, snow)
    into a single descriptor (e.g. wintery mix)
    """
