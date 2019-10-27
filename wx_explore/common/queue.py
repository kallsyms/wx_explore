from psycopg2 import connect, ProgrammingError
from pq import PQ

import os

from wx_explore.web import config

cfg = config.Config

pq = PQ(
    connect(
        user=cfg.POSTGRES_USER,
        password=cfg.POSTGRES_PASS,
        host=cfg.POSTGRES_HOST,
        dbname=cfg.POSTGRES_DB,
    ),
    table='work_queue')

try:
    pq.create()
except ProgrammingError as exc:
    if exc.pgcode != '42P07':
        raise
