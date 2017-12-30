#!/usr/bin/env python3
import time


def datetime2unix(dt):
    return int(time.mktime(dt.timetuple()))
