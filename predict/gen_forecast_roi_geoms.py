import uuid
from datetime import timedelta, datetime
from ci.db.pgdbhelper import PGDbHelper
from ci.config import get_instance
from ci.util.proj_helper import ProjHelper
from ci.models.spatial_reference import SRID_RAP


__author__ = 'ujjwal'


forecast_time = datetime(year=2014, month=8, day=7, hour=20, minute=30, second=0)
forecast_times = [forecast_time]

#get accessor to the old db
config = get_instance()
pgdb_helper = PGDbHelper(conn_str=config.pgsql_conn_str(), echo=config.logsql)
proj_helper = ProjHelper(config=config)
logger = config.logger

radius = config.ci_roi_radius
mask_name = config.mask_name

#create table
pgdb_helper.submit("drop table if exists forecast_roi_geoms;")
pgdb_helper.submit(
    """
        create table forecast_roi_geoms
        (
            id serial not null,
            roi_name text not null,
            rap_granule_id int not null,
            starttime timestamp without time zone NOT NULL,
            endtime timestamp without time zone NOT NULL,
            geom geometry not null,
            center geometry not null,
            storm_poly geometry null,
            center_lat float not null,
            center_lon float not null,
            iarea float null,
            type int not null,
            constraint rap_roi_geom_pkey primary key (id)
        );
    """)

#grab a single rap granule
sql = """
    select dg.id from datagranule dg
    join variable on variable.id=dg.variable_id
    where variable.name='CAPE0' limit 1
"""
rows = pgdb_helper.query(sql)
rap_granule_id = rows[0][0]

#select roi geoms for each rap grid cell
if mask_name:
    sql = """
        SELECT
        ST_AsEWKT(ST_Transform(ST_Buffer(a.geom, {radius}), {srid})) geom,
        ST_AsEWKT(ST_Transform(a.geom, {srid})) center,
        ST_Y(ST_Transform(a.geom, 4326)) lat,
        ST_X(ST_Transform(a.geom, 4326)) lon
        FROM (select st_centroid((st_pixelaspolygons(foo.rast, 1, False)).geom) as geom from
        (select st_union(rast) rast from rastertile where datagranule_id={granule_id}
        and st_intersects(rast, (select st_transform(geom, st_srid(rast)) from mask where name='{mask_name}' limit 1))) as foo) a
    """ . format(radius=radius * 1000, granule_id=rap_granule_id, mask_name=mask_name, srid=SRID_RAP)
else:
    sql = """
        SELECT
        ST_AsEWKT(ST_Transform(ST_Buffer(a.geom, {radius}), {srid})) geom,
        ST_AsEWKT(ST_Transform(a.geom, {srid})) center,
        ST_Y(ST_Transform(a.geom, 4326)) lat,
        ST_X(ST_Transform(a.geom, 4326)) lon
        FROM (
            select st_centroid((st_pixelaspolygons(foo.rast, 1, False)).geom) as geom
            from (
                select st_union(rast) rast from rastertile where datagranule_id={granule_id}
                 )
            as foo
        ) a
    """.format(radius=radius * 1000, granule_id=rap_granule_id, srid=SRID_RAP)

roi_rows = pgdb_helper.query(sql)

#insert roi_geoms for each rap cell, all forecast hours
for dt in forecast_times:
    rois = []
    for roi_row in roi_rows:
        roi_name = str(uuid.uuid4())
        datagranule_id = rap_granule_id
        values = (roi_name, rap_granule_id, dt, dt + timedelta(minutes=3),
                  roi_row[0], roi_row[1], roi_row[2], roi_row[3], 1)
        rois.append(values)
        for t in range(-1, -5, -1):
            starttime = dt + t * timedelta(hours=1)
            endtime = starttime + timedelta(minutes=3)
            values = (roi_name, rap_granule_id, starttime, endtime,
                      roi_row[0], roi_row[1], roi_row[2], roi_row[3], t)
            rois.append(values)

    sql = """
            insert into forecast_roi_geoms (roi_name, rap_granule_id, starttime, endtime,
                geom, center, storm_poly, center_lat, center_lon, type)
            values (%s, %s, %s, %s, ST_GeomFromEWKT(%s), ST_GeomFromEWKT(%s), NULL, %s, %s, %s)
        """

    pgdb_helper.insertMany(sql, rois)
    logger.info("Inserted %d forecast ROIs for %s, mask %s" % (len(rois), dt, config.mask_name))

pgdb_helper.submit("create index fcst_roi_geoms_center_indx on forecast_roi_geoms using gist(center)")
pgdb_helper.submit("create index fcst_roi_geoms_geom_indx on forecast_roi_geoms using gist(geom)")

