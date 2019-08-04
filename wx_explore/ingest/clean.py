#!/usr/bin/env python3
from datetime import datetime, timedelta
import tempfile
import os
import logging

from wx_explore.common.models import LocationData
from wx_explore.web import db


def clean_old_datas():
    max_age = datetime.utcnow() - timedelta(days=1)

    LocationData.query.filter(LocationData.valid_time < max_age).delete()
    db.session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    clean_old_datas()
