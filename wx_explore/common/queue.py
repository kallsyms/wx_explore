from psycopg2 import connect
from pq import PQ
from pydoc import locate

import os

from wx_explore.web import config

cfg = locate(os.environ.get('CONFIG')) or config.DevConfig

pq = PQ(
    connect(
        user=cfg.POSTGRES_USER,
        password=cfg.POSTGRES_PASS,
        host=cfg.POSTGRES_HOST,
        dbname=cfg.POSTGRES_DB,
    ),
    table='work_queue')

pq.create()
