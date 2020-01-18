from psycopg2 import connect, ProgrammingError
from pq import PQ

from wx_explore.common.config import Config


pq = PQ(
    connect(
        user=Config.POSTGRES_USER,
        password=Config.POSTGRES_PASS,
        host=Config.POSTGRES_HOST,
        dbname=Config.POSTGRES_DB,
    ),
    table='work_queue')

try:
    pq.create()
except ProgrammingError as exc:
    if exc.pgcode != '42P07':
        raise
