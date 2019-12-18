from wx_explore.common.utils import memoize
from wx_explore.common.models import Metric, SourceField


@memoize
def get_metric(sfid: int) -> Metric:
    return SourceField.query(SourceField.id == sfid).first().metric
