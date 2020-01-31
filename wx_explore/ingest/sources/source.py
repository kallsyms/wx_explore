from typing import Optional
import datetime


class IngestSource(object):
    @staticmethod
    def queue(
            time_min: int,
            time_max: int,
            run_time: Optional[datetime.datetime] = None,
            acquire_time: Optional[datetime.datetime] = None
    ):
        raise NotImplementedError

    @staticmethod
    def generate_derived(file_path: str):
        pass
