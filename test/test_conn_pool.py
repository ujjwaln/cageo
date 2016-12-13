from psycopg2 import connect
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager


conn_str = 'host=127.0.0.1 dbname=ci_dev_july user=postgres password=postgres port=5432'
minconn = 1
maxconn = 8
pool = SimpleConnectionPool(minconn, maxconn, conn_str)


@contextmanager
def get_cursor():
    conn = pool.getconn()
    try:
        yield conn.cursor()
    finally:
        pool.putconn(conn)


sql = "select * from datagranule limit 10"
with get_cursor() as cur:
    cur.execute(sql)
    rows = cur.fetchall()
    print len(rows)

pool.closeall()
