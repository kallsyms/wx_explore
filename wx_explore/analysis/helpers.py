from wx_explore.common.models import Metric, SourceField


def get_metric(sfid: int) -> Metric:
    return SourceField.query.get(sfid).metric
