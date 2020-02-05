from typing import Optional
import datetime
import pygrib

from wx_explore.common.models import Source


class IngestSource(object):
    SOURCE_NAME = None

    @classmethod
    def get_db_source(cls):
        return Source.query.filter(Source.short_name == cls.SOURCE_NAME).first()

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
        pass
