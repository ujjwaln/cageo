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


class PGDbHelper(object):

    def __init__(self, conn_str, echo=False):
        self.echo = echo
        self.pool = SimpleConnectionPool(1, 12, conn_str)

    def finish(self):
        self.pool.closeall()

    @contextmanager
    def _get_cursor(self):
        conn = self.pool.getconn()
        # conn.autocommit = True
        conn.set_isolation_level(0)
        try:
            if self.echo:
                cur = conn.cursor(cursor_factory=LoggingCursor)
            else:
                cur = conn.cursor(cursor_factory=DictCursor)
            yield cur
            conn.commit()
            conn.close()

        finally:
            self.pool.putconn(conn)

    def insert(self, sql_string, value):
        try:
            with self._get_cursor() as cur:
                cur.execute(sql_string, value)
        except Exception, ex:
            logger.debug("Error while executing %s" % sql_string)
            logger.debug(traceback.print_exc())
            raise ex

    def insertMany(self, sql_string, values):
        try:
            with self._get_cursor() as cur:
                cur.executemany(sql_string, values)
        except Exception, ex:
            logger.debug("Error while executing %s" % sql_string)
            logger.debug(traceback.print_exc())
            raise ex

    def insertManyOpt(self, table_name, col_names, format_string, values):
        #see http://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
        str_col_names = ",".join(c for c in col_names)
        try:
            with self._get_cursor() as cur:
                args = ",".join(cur.mogrify(format_string, val) for val in values)
                cur.execute("INSERT INTO %s (%s) VALUES %s" % (table_name, str_col_names, args))
        except Exception, ex:
            with self._get_cursor() as cur:
                for val in values:
                    try:
                        str_val = cur.mogrify(format_string, val)
                        sql = "INSERT INTO %s (%s) VALUES %s" % (table_name, str_col_names, str_val)
                        cur.execute(sql)
                    except Exception, ex:
                        logger.debug("Error while inserting %s" % (",".join(str(v) for v in val)))
                        raise ex

            logger.debug(traceback.print_exc())
            raise ex

    def insertAndGetId(self, sql_string, values):
        try:
            sql = sql_string + " RETURNING id"
            with self._get_cursor() as cur:
                cur.execute(sql, values)
                inserted_rows = cur.fetchone()

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
        try:
            with self._get_cursor() as cur:
                cur.execute(sql)
        except Exception, ex:
            logger.debug("Error while executing %s" % sql)
            logger.debug(traceback.print_exc())
            raise ex
        return

    def query(self, sql, values=None):
        try:
            with self._get_cursor() as cur:
                if values is None:
                    cur.execute(sql)
                else:
                    cur.execute(sql, values)
                results = cur.fetchall()
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

    def get_rastertiles_as_polygons(self, granule, roi, srid=4326, limit=-1, min_val=None, max_val=None):
        granule_id = granule["id"]
        granule_srid = granule["srid"]
        sql = """
            select ST_AsGeoJSON(ST_Transform((bar.foo).geom, 4326)), (bar.foo).val from
            (select st_intersection(
            ST_Transform(ST_GeomFromText('%s', 4326), %d),
            rast) as foo from "%s"
            where datagranule_id=%d
            and st_intersects(st_transform(st_geomfromtext('%s', 4326), %d),rast)
            ) as bar
            where (bar.foo).val notnull
        """ % (roi, granule_srid, "rastertile", granule_id, roi, granule_srid)

        if not min_val is None:
             sql = "%s and (bar.foo).val > %f" % (sql, min_val)

        if not max_val is None:
             sql = "%s and (bar.foo).val < %f" % (sql, max_val)

        if limit > 0:
            sql = "%s limit %d" % (sql, limit)

        return self.query(sql)

    def get_rastertiles_as_wkt(self, granule_id, roi, srid=3857, min_val=None, max_val=None, limit=-1):
        get_srid_sql = 'select st_srid(rast) from rastertile where datagranule_id=%d limit 1' % granule_id
        raster_srid_result = self.query(get_srid_sql)
        raster_srid = raster_srid_result[0][0]

        sql = """
            select ST_AsText(ST_Transform((bar.foo).geom, %d)), (bar.foo).val from
            (select st_intersection(
            ST_Transform(ST_GeomFromText('%s', 4326), %d),
            rast) as foo from "%s"
            where datagranule_id=%d
            and st_intersects(st_transform(st_geomfromtext('%s', 4326), %d),rast)
            ) as bar
            where (bar.foo).val notnull
        """ % (srid, roi, raster_srid, "rastertile", granule_id, roi, raster_srid)

        if not min_val is None:
             sql = "%s and (bar.foo).val > %f" % (sql, min_val)

        if not max_val is None:
             sql = "%s and (bar.foo).val < %f" % (sql, max_val)

        if limit > 0:
            sql = "%s limit %d" % (sql, limit)

        return self.query(sql)

    def regrid(self, granule_id, roi, ul_lat, ul_lon, scale_x, scale_y, skew_x=0, skew_y=0):
        sql = """
            select
                ST_AsGeoJSON(st_transform(y.geom, 4326)),
                y.val

            from (
                select (
                    st_pixelaspolygons(x.polyrast)).geom,
                    (st_pixelaspolygons(x.polyrast)).val

                    from (
                    select st_asraster((isect.poly).geom, rastt, '32BF', (isect.poly).val, -9999) polyrast
                    from (
                        select st_intersection(rap.rast,
                        ST_Transform(ST_GeomFromText('%s', 4326), st_srid(rast))
                    ) poly

                    from "%s" as rap
                    where datagranule_id=%d
                    and st_intersects(st_transform(st_geomfromtext('%s',4326), st_srid(rast)),rast)
                    )
                    as isect,

                    st_asraster(ST_Transform(ST_GeomFromText('%s', 4326), 4326),
                        %f,%f,%f,%f,'32BF',1,-9999,%f,%f,false) rastt
                ) as x

            ) as y
        """ % (
            roi, "rastertile", granule_id, roi, roi, scale_x, scale_y, ul_lon, ul_lat, skew_x, skew_y)

        return self.query(sql)

    def get_rastertiles_as_image(self, granule_id, roi):
        grid_x = 0
        grid_y = 0

        sql = """
        select ST_AsPNG(ST_ColorMap(
        ST_AsRaster((isect.poly).geom, 256, 256, %f, %f, '32BF', (isect.poly).val, -99, 0, 0, false),
        'fire', 'INTERPOLATE'))
        from (
        SELECT ST_DumpAsPolygons(
          ST_Clip(rast, ST_GeomFromText('%s', 4326))
          ) poly
          from rastertile where datagranule_id=%d
          and ST_Intersects(rast, ST_GeomFromText('%s', 4326))
        ) isect
        """ % (grid_x, grid_y, roi, granule_id, roi)

        return self.query(sql)

    def get_rastertiles_as_image1(self, granule_id, roi):
        sql = """
            SELECT

            ST_AsPNG(
               ST_ColorMap(
                ST_Transform(ST_Clip(ST_Union(rast), ST_GeomFromText('%s', 4326)), 3857, 'NearestNeighbor'),
                'fire',
                'INTERPOLATE'
               )
            )

            from rastertile
            where datagranule_id=%d and
            st_intersects(
                st_transform(
                    st_geomfromtext('%s', 4326),
                    st_srid(rast)
                ), rast
            )
        """ % (roi, granule_id, roi)

        return self.query(sql)

    def insert_slope_and_aspect_rasters(self, gtopo_granule_name, overwrite=False):
        if overwrite:
            rows = self.query("""
                    select datagranule.id from datagranule
                    join variable on datagranule.variable_id=variable.id
                    where variable.name='SLOPE' OR variable.name='ASPECT'
                """)

            if len(rows):
                for row in rows:
                    granule_id=row[0]
                    self.submit("""
                        delete from rastertile where datagranule_id=%d
                        """ % granule_id)

                    self.submit("""
                        delete from datagranule where id=%d
                        """ % granule_id)

        rows = self.query("select id from variable where name='SLOPE'")
        slope_variable_id = rows[0][0]

        rows = self.query("select id from variable where name='ASPECT'")
        aspect_variable_id = rows[0][0]

        rows = self.query("""
                select id, starttime, level, extent, name, srid, table_name, provider_id, endtime
                from datagranule where name='%s'
            """ % gtopo_granule_name)

        #insert slope and aspect datagranules
        slope_granule_id = self.insertAndGetId("""
                insert into datagranule (starttime, level, extent, name, srid, table_name, variable_id, provider_id, endtime, file_name)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (rows[0][1], rows[0][2], rows[0][3], "Slope_%s" % gtopo_granule_name, rows[0][5],
                  rows[0][6], slope_variable_id, rows[0][7], rows[0][8], "Slope_%s" % gtopo_granule_name))

        aspect_granule_id = self.insertAndGetId("""
                insert into datagranule (starttime, level, extent, name, srid, table_name, variable_id, provider_id, endtime, file_name)
                values (%s,%s,%s,%s,%s,%s,%s,%s, %s, %s)
            """, (rows[0][1], rows[0][2], rows[0][3], "Aspect_%s" % gtopo_granule_name, rows[0][5],
                  rows[0][6], aspect_variable_id, rows[0][7], rows[0][8], "Aspect_%s" % gtopo_granule_name))

        elev_granule_id = rows[0][0]

        mt_lat_lon_scale = 111120. #since x,y dims are latlon(4326) and z dim is meters
        sql = """
            insert into rastertile (rast, datagranule_id) (
                select st_tile(foo.rastt, 1, 50, 50, FALSE, NULL), %d from (
                  select st_slope(st_union(rast), 1, '32BF','DEGREES', %f,FALSE) rastt from rastertile
                  where datagranule_id=%d
                ) as foo
            )
        """ % (slope_granule_id, mt_lat_lon_scale, elev_granule_id)
        self.submit(sql)

        # sql = """
        #     insert into rastertile (rast, datagranule_id) (
        #         select st_tile(
        #             st_reclass(foo.rastt, 1, '[-9999--1):-1', '32BF', NULL),
        #             1, 50, 50, FALSE, -9999), %d from (
        #           select st_aspect(st_union(rast), 1, '32BF','DEGREES', FALSE) rastt from rastertile
        #           where datagranule_id=%d
        #         ) as foo
        #     )
        # """ % (aspect_granule_id, elev_granule_id)
        # self.submit(sql)

        #set -1 as nodata value for aspect raster
        # sql = """
        #     update rastertile
        #         set rast = st_setbandnodatavalue(rast, 1, -1)
        #     where datagranule_id=%d
        # """ % aspect_granule_id

        sql = """
            insert into rastertile (rast, datagranule_id) (
                select st_mapalgebra(foo.r1, foo.r2, '[rast1.val] * [rast2.val]', '32BF',
                    'INTERSECTION', NULL, NULL, NULL), %d
                from (
                  select bar.ra r1, st_reclass(bar.ra, 1, '[-10000--1]:0, [0-361]:1', '8BUI', NULL) r2
                  from (
                    select st_aspect(rast, 1, '32BF', 'DEGREES', FALSE) ra
                    from rastertile where datagranule_id=%d
                   ) bar
                ) foo
            )
        """ % (aspect_granule_id, elev_granule_id)
        self.submit(sql)

    def get_rois_geojson(self, granule_id, threshold, radius, srid=4326):
        min_storm_area = 1e5 #min area = km2
        max_storm_area = 1e7
        sql = """
          select st_asgeojson(st_transform(st_buffer(st_centroid(geom), %f), 4326)),
          st_area(st_intersection(geom, st_buffer(st_centroid(geom), %f))) / (3.14 * %f * %f) iarea
            from (
            select st_transform(((foo.gv).geom), %d) geom, ((foo.gv).val) val
            from (
            select st_dumpaspolygons(
                st_union(
                    st_reclass(rast, 1, '[-100-%f]:0, (%f-100):1', '8BUI', NULL)
                    )
            ) gv from rastertile
            where datagranule_id=%d
            ) as foo
            ) as bar where ST_Area(geom) > %f and ST_Area(geom) < %f
        """ % (radius * 1000, radius * 1000, radius * 1000, radius * 1000, SRID_ALBERS, threshold, threshold,
               granule_id, min_storm_area, max_storm_area)
        return self.query(sql)

    def get_rois_wkt(self, granule_id, threshold, radius, srid=4326, min_storm_area=1e6, max_storm_area=1e6*5):
        # sql = """
        #   select
        #   st_astext(st_transform(st_buffer(st_centroid(geom), %f), 4326)) buffer,
        #   st_area(st_intersection(geom, st_buffer(st_centroid(geom), %f))) / (3.14 * %f * %f) iarea,
        #   st_astext(st_transform(st_centroid(geom), 4326)) center,
        #   st_x(st_transform(st_centroid(geom), 4326)) center_lon,
        #   st_y(st_transform(st_centroid(geom), 4326)) center_lat,
        #   st_astext(st_transform(geom, 4326)) poly
        #     from (
        #     select st_transform(((foo.gv).geom), %d) geom, ((foo.gv).val) val
        #     from
        #     (
        #         select st_dumpaspolygons(
        #         st_union(
        #             st_reclass(rast, 1, '[-100-%f]:0, (%f-100):1', '8BUI', NULL)
        #             )
        #     ) gv from rastertile
        #     where datagranule_id=%d
        #     ) as foo
        #     ) as bar where ST_Area(geom) > %f and ST_Area(geom) < %f
        #     order by iarea asc
        # """ % (radius * 1000, radius * 1000, radius * 1000, radius * 1000, SRID_ALBERS, threshold, threshold,
        #        granule_id, min_storm_area, max_storm_area)

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

    def get_roi_data(self, roi, granule_id):
        sql = """
        SELECT ST_SummaryStats(
         ST_Clip(ST_Union(rast),
         ST_Transform(ST_GeomFromText('%s', 4326), st_srid(st_union(rast)))), TRUE)
        from rastertile where datagranule_id=%d
        """ % (roi, granule_id)

        return self.query(sql)

    def search_granules(self, provider_id, variable_id, start_time, end_time, level=-1):
        sql = """
            select id from datagranule
            where (provider_id=%d) and (variable_id=%d) and ((TIMESTAMP '%s', TIMESTAMP '%s') OVERLAPS (starttime, endtime))
        """ % (provider_id, variable_id, start_time, end_time)

        if level > -1:
            sql = "%s and level=%d" % (sql, level)

        sql = "%s order by starttime" % sql
        return self.query(sql)

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

