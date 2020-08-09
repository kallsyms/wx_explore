import logging
import numpy

from wx_explore.common import tracing, storage
from wx_explore.common.logging import init_sentry
from wx_explore.common.tracing import init_tracing


if __name__ == "__main__":
    init_sentry()
    logging.basicConfig(level=logging.INFO)
    init_tracing('merge')
    with tracing.start_span('merge'):
        storage.get_provider().merge()
