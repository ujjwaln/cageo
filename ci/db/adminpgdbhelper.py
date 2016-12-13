import traceback
import psycopg2.extensions
from psycopg2 import connect
from psycopg2.extras import DictCursor
from psycopg2.pool import SimpleConnectionPool
from ci.util.logger import logger
from ci.models.spatial_reference import SRID_ALBERS
from contextlib import contextmanager


__author__ = 'ujjwal'


class LoggingCursor(psycopg2.extensions.cursor):
    def execute(self, sql, args=None):
        logger.info(self.mogrify(sql, args))
        try:
            psycopg2.extensions.cursor.execute(self, sql, args)
        except Exception, exc:
            logger.error("%s: %s" % (exc.__class__.__name__, exc))
            raise


class PGDbAccess(object):
    """
        Uses connection pool
    """
    def __init__(self, conn_str, echo):
        self.conn_string = conn_str
        self.echo = echo
        self.conn = connect(conn_str)
        self.conn.autocommit = True

    def __enter__(self):
        if self.echo:
            self.cur = self.conn.cursor(cursor_factory=LoggingCursor)
        else:
            self.cur = self.conn.cursor(cursor_factory=DictCursor)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is None:
            self.cur.close()
            self.conn.close()
        else:
            self.cur.close()
            self.conn.rollback()


class AdminPGDbHelper(object):
    """
    AdminDBHelper contains utility functions for creating the database, deleting existing
    database etc using pyscopg2 driver directly (as opposed to SQLAlchemy dependent modules)
    """
    def __init__(self, conn_str, echo=False):
        self.conn_string = conn_str
        self.echo = echo

    def insert(self, sql_string, value):
        try:
            with PGDbAccess(self.conn_string, self.echo) as pga:
                pga.cur.execute(sql_string, value)
                pga.conn.commit()

        except Exception, ex:
            logger.debug("Error while executing %s" % sql_string)
            logger.debug(traceback.print_exc())
            raise ex

    def insertMany(self, sql_string, values):
        try:
            with PGDbAccess(self.conn_string, self.echo) as pga:
                pga.cur.executemany(sql_string, values)
                pga.conn.commit()
        except Exception, ex:
            logger.debug("Error while executing %s" % sql_string)
            logger.debug(traceback.print_exc())
            raise ex

    def insertAndGetId(self, sql_string, values):
        try:
            sql = sql_string + " RETURNING id"
            with PGDbAccess(self.conn_string, self.echo) as pga:
                pga.cur.execute(sql, values)
                pga.conn.commit()
                inserted_rows = pga.cur.fetchone()

            if inserted_rows:
                _id = inserted_rows[0]
                return _id
            else:
                return None
        except Exception, ex:
            logger.debug("Error while executing %s" % sql_string)
            logger.debug(traceback.print_exc())
            raise ex

    def submit(self, sql):
        # conn = connect(self.conn_string)
        # conn.autocommit = True
        # conn.set_isolation_level(0)
        # cur = conn.cursor(cursor_factory=DictCursor)
        try:
            with PGDbAccess(self.conn_string, self.echo) as pga:
                pga.cur.execute(sql)
                pga.conn.commit()
        except Exception, ex:
            logger.debug("Error while executing %s" % sql)
            logger.debug(traceback.print_exc())
            raise ex
        return

    def query(self, sql, values=None):
        try:
            with PGDbAccess(self.conn_string, self.echo) as pga:
                if values is None:
                    pga.cur.execute(sql)
                else:
                    pga.cur.execute(sql, values)
                #pga.conn.commit()
                results = pga.cur.fetchall()
        except Exception, ex:
            logger.debug("Error while executing %s" % sql)
            logger.debug(traceback.print_exc())
            raise ex

        return results or []

    def create_database(self, dbname):
        self.submit("CREATE DATABASE %s;" % dbname)

    def drop_database(self, dbname):
        self.submit("DROP DATABASE %s" % dbname)

    def check_database_exists(self, dbname):
        sql = "SELECT 1 FROM pg_database WHERE datname = '%s'" % dbname
        results = self.query(sql)
        if len(results) > 0:
            return True
        return False

    def enable_geodatabase(self):
        self.submit("CREATE EXTENSION POSTGIS;")

    def insert_sprefs(self):
        from ci.models.spatial_reference import MODIS_SpatialReference, RAP_Spatial_Reference, \
            ALBERS_Spatial_Reference, HRAP_Spatial_Reference, GFS_Spatial_Reference, LIS_Spatial_Reference, \
            USA_CONTIG_ALBERS_Spatial_Reference

        modis_sql = MODIS_SpatialReference.sql_insert_statement
        rap_sql = RAP_Spatial_Reference.sql_insert_statement
        albers_sql = ALBERS_Spatial_Reference.sql_insert_statement
        hrap_sql = HRAP_Spatial_Reference.sql_insert_statement
        gfs_sql = GFS_Spatial_Reference.sql_insert_statement
        lis_sql = LIS_Spatial_Reference.sql_insert_statement

        self.submit(modis_sql)
        self.submit(rap_sql)
        self.submit(albers_sql)
        self.submit(hrap_sql)
        self.submit(gfs_sql)
        self.submit(lis_sql)
        self.submit(USA_CONTIG_ALBERS_Spatial_Reference.sql_insert_statement)

    def create_gist_index(self, table_name, index_name, column_name="rast"):
        sql = """
            create INDEX %s ON
            "%s" USING GIST(ST_CONVEXHULL(%s))
        """ % (index_name, table_name, column_name)

        self.submit(sql)

    def ensure_gist_index(self, table_name, index_name, column_name="rast"):
        sql = """
            do $$
            begin
            if not exists (
                        select 1 from pg_class c join pg_namespace n on n.oid=c.relnamespace
                        where c.relname='%s' and n.nspname = 'public'
            ) then
                create index %s on %s using gist(st_convexhull(%s));
            end if;
            end$$;
        """ % (index_name, index_name, table_name, column_name)

        self.submit(sql)

    def ensure_datagranule_id_index(self, table_name="rastertile", index_name="rastertile_datagranule_id_idx",
                                              column_name="datagranule_id"):
        sql = """
            do $$
            begin
            if not exists (
                        select 1 from pg_class c join pg_namespace n on n.oid=c.relnamespace
                        where c.relname='%s' and n.nspname = 'public'
            ) then
                create index %s on %s using btree(%s);
            end if;
            end$$;
        """ % (index_name, index_name, table_name, column_name)

        self.submit(sql)

    def ensure_datagranule_unique_index(self):
        sql = """
            do $$
                begin
                if not exists (
                            select 1 from pg_class c join pg_namespace n on n.oid=c.relnamespace
                            where c.relname='datagranule_starttime_endtime_level_variable_id_prov_idx'
                            and n.nspname = 'public'
                ) then

                CREATE UNIQUE INDEX datagranule_starttime_endtime_level_variable_id_prov_id_filename_idx
                ON datagranule USING btree
                (starttime, endtime, level, variable_id, provider_id, file_name);
            end if;
            end$$;
        """
        self.submit(sql)

    def get_rois_wkt(self, granule_id, threshold, radius, srid=4326, min_storm_area=1e6, max_storm_area=1e6*5):
        sql = """
          select st_astext(st_transform(st_buffer(st_centroid(geom), {radius}), {srid})) buffer,
          st_area(st_intersection(geom, st_buffer(st_centroid(geom), {radius}))) / (3.14 * {radius} * {radius}) iarea,
          st_astext(st_transform(st_centroid(geom), {srid})) center,
          st_x(st_transform(st_centroid(geom), {srid})) center_lon,
          st_y(st_transform(st_centroid(geom), {srid})) center_lat,
          st_astext(st_transform(geom, {srid})) poly
            from (
            select st_transform(((foo.gv).geom), {srid_albers}) geom, ((foo.gv).val) val
            from
            (
                select st_dumpaspolygons(
                st_union(
                    st_reclass(rast, 1, '[-100-{threshold}]:0, ({threshold}-100):1', '8BUI', NULL)
                    )
            ) gv from rastertile
            where datagranule_id={granule_id}
            ) as foo
            ) as bar where ST_Area(geom) > {min_area} and ST_Area(geom) < {max_area}
            order by iarea asc
        """ .format(radius=radius*1000, srid=srid, srid_albers=SRID_ALBERS, threshold=threshold,
                    min_area=min_storm_area, max_area=max_storm_area, granule_id=granule_id)

        rows = self.query(sql)
        # if len(rows):
        #     print len(rows)
        return rows

    def insert_plpsql_functions(self):
        sql = """
CREATE OR REPLACE FUNCTION st_xderivative4ma(IN matrix double precision[], IN nodatamode text, VARIADIC args text[])
      RETURNS double precision AS
    $BODY$
	DECLARE
	    width double precision;
	    left_x double precision;
	    right_x double precision;
	    xderivative double precision;
        _matrix double precision[][];
	BEGIN
        _matrix := matrix;
	    width := args[1]::double precision;
	    if array_upper(matrix, 1) = 3 then
		left_x := _matrix[1][2];
		right_x := _matrix[3][2];
		    if (left_x is NULL) or (right_x is NULL) THEN
			xderivative := NULL;
		    else
			xderivative := (right_x - left_x) / (2 * width);
			end if;
	    else
		xderivative = NULL;
	    end if;

	    return xderivative;
	END;
	$BODY$
      LANGUAGE plpgsql IMMUTABLE;
    --  COST 100;
    --ALTER FUNCTION st_xderivative4ma(double precision[], text, text[])
    --  OWNER TO postgres;

CREATE OR REPLACE FUNCTION st_yderivative4ma(IN matrix double precision[], IN nodatamode text, VARIADIC args text[])
  RETURNS double precision AS
$BODY$
    DECLARE
        height double precision;
        upper_y double precision;
        lower_y double precision;
        yderivative double precision;
        _matrix double precision[][];
    BEGIN
        height := args[1]::double precision;
        _matrix := matrix;
        if array_upper(matrix, 2) = 3 then
	    upper_y := matrix[2][3];
	    lower_y := matrix[2][1];
            if (upper_y is NULL) or (lower_y is NULL) THEN
	        yderivative := NULL;
            else
	        yderivative := (upper_y - lower_y) / (2 * height);
            end if;
	else
	    yderivative = NULL;
        end if;

        return yderivative;
    END;
    $BODY$
  LANGUAGE plpgsql IMMUTABLE;
--  COST 100;
--ALTER FUNCTION st_yderivative4ma(double precision[], text, text[])
--  OWNER TO postgres;


CREATE OR REPLACE FUNCTION st_shannonentropy(IN items anyarray, IN nodatamode text, VARIADIC args text[])
	RETURNS double precision
        --RETURNS text
as
$BODY$
	DECLARE
	    tmp text;
            qry text;
            r record;
            H double precision;
            p double precision;
            count integer;
            --output text;
	BEGIN
            --output := '';
            H := 0;
            p := 0;
	    count := array_upper(items, 1);
            tmp := quote_literal('{' || array_to_string(items, ',') || '}');
	    qry := 'select unnest((' || tmp || '::text)::text[]) as val, count(*) num group by val order by val';
            for r in execute qry loop
	        p := cast(r.num as double precision) / count;
                H := H + p * log(2.0, cast(p as numeric));
                --output := output || r.val || '-' || r.num || ':' || cast(p as text) || '--';
            end loop;
            return -1 * H;
	    --return output;
	END;
	$BODY$
  LANGUAGE plpgsql IMMUTABLE;
--  COST 100;
--ALTER FUNCTION st_shannonentropy(anyarray, text, text[])
--  OWNER TO postgres;

        """
        self.submit(sql)

