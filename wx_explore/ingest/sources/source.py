from typing import Optional

import datetime
import pygrib


class IngestSource(object):
    SOURCE_NAME = None

    @staticmethod
    def queue(
            time_min: int,
            time_max: int,
            run_time: Optional[datetime.datetime] = None,
            acquire_time: Optional[datetime.datetime] = None
    ):
        raise NotImplementedError

    @staticmethod
    def generate_derived(grib: pygrib.open):
        return {}
