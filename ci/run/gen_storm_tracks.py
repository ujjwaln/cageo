__author__ = 'ujjwal'
import uuid
import os
from datetime import datetime, timedelta
from osgeo import ogr, osr
from ci.db.pgdbhelper import PGDbHelper
from ci.models.spatial_reference import SRID_ALBERS
from ci.ingest import config, logger


#get accessor to the db
pgdb_helper = PGDbHelper(conn_str=config.pgsql_conn_str(), echo=config.logsql)


def get_mrms_granules(start_time, end_time):
    sql = """
        select datagranule.id, datagranule.starttime, datagranule.endtime, datagranule.name from datagranule
        join provider on provider.id = datagranule.provider_id
        join variable on variable.id = datagranule.variable_id
        where provider.name like 'MRMS' and variable.name like 'REFL' and datagranule.level=14
        and (('%s', '%s') overlaps (datagranule.starttime, datagranule.endtime))
        order by datagranule.starttime asc
    """ % (start_time, end_time)

    rows = pgdb_helper.query(sql)
    return rows


def generate_mrms_image(mrms_granule, threshold):
    mrms_granule_id = mrms_granule[0]
    sql = """
        select st_astiff(st_colormap((st_union(st_reclass(rast, 1, '[-100-%f]:0, (%f-100):1', '8BUI', NULL))), 'fire',
        'INTERPOLATE')) from rastertile where datagranule_id=%d
    """ % (threshold, threshold, mrms_granule_id)

    rows = pgdb_helper.query(sql)
    start_time = mrms_granule[1]
    filename = "./images/mrms_%s_%d.tif" % (start_time.strftime("%m-%d-%H-%M-%S"), mrms_granule_id)
    if os.path.exists(filename):
        os.remove(filename)
    with open(filename, mode='wb') as f:
        data = rows[0][0]
        f.write(data)
    logger.info("saved %s" % filename)


def save_storm_polys(mrms_granules, threshold, srid=4326):
    min_storm_area = 4e6
    max_storm_area = 1e8

    pgdb_helper.submit("drop table if exists roi_polys")

    sql = """
        create table roi_polys (
            id serial not null,
            lat double precision not null,
            lon double precision not null,
            starttime timestamp without time zone not null,
            endtime timestamp without time zone not null,
            geom geometry not null,
            mrms_granule_id integer not null
        )
    """
    pgdb_helper.submit(sql)

    for mrms_granule in mrms_granules:
        granule_id = mrms_granule[0]
        start_time = mrms_granule[1]
        end_time = mrms_granule[2]

        sql = """
          select
          st_astext(st_transform(geom, {srid})) poly,
          st_x(st_transform(st_centroid(geom), {srid})) center_lon,
          st_y(st_transform(st_centroid(geom), {srid})) center_lat
            from (
                select st_transform(((foo.gv).geom), {area_srid}) geom, ((foo.gv).val) val
                from
                (
                    select st_dumpaspolygons(
                      st_union(
                        st_reclass(rast, 1, '[-100-{threshold}]:0, ({threshold}-100):1', '8BUI', NULL)
                      )
                    ) gv from rastertile where datagranule_id={granule_id}
                ) as foo
            ) as bar where ST_Area(geom) > {min_area} and ST_Area(geom) < {max_area}
        """ .format(**{
            "srid": srid,
            "area_srid": SRID_ALBERS,
            "threshold": threshold,
            "granule_id": granule_id,
            "min_area": min_storm_area,
            "max_area": max_storm_area
        })

        rows = pgdb_helper.query(sql)
        for row in rows:
            pgdb_helper.submit(
                """
                insert into roi_polys (lat, lon, starttime, endtime, geom, mrms_granule_id)
                values ('%f', '%f', '%s', '%s', ST_GeomFromText('%s', 4326), %d)
                """ %
                (row[2], row[1], start_time.strftime("%Y-%m-%d %H:%M:%S"), end_time.strftime("%Y-%m-%d %H:%M:%S"),
                 row[0], granule_id)
            )

        logger.info("Inserted Storm Polys for granule %d, time %s" % (granule_id, start_time.strftime("%Y-%m-%d %H:%M")))

    pgdb_helper.submit("create index on roi_polys using gist(geom)")


def generate_storm_tracks(mrms_granules, threshold=35, ci_lifetime_hours=4):

    active_roi_tracks = {}

    pgdb_helper.submit("drop table if exists roi_tracks")
    sql = """
        create table roi_tracks (
            id character varying not null,
            starttime timestamp without time zone not null,
            endtime timestamp without time zone not null,
            geom geometry not null
        )
    """
    pgdb_helper.submit(sql)

    for mrms_granule in mrms_granules:
        mrms_granule_id = mrms_granule[0]

        #storm_polys = get_storm_polys(granule_id=mrms_granule_id, threshold=threshold, srid=4326)
        storm_polys = pgdb_helper.query("""
                    select id,lat,lon,starttime, endtime, st_astext(geom), mrms_granule_id
                    from roi_polys where mrms_granule_id=%d order by starttime asc
                """ % mrms_granule_id)

        new_storms = []
        for rt in active_roi_tracks:
            active_roi_tracks[rt]["updated"] = False

        for row in storm_polys:
            poly = {
                "type": row[0],
                "lat": row[1],
                "lon": row[2],
                "starttime": row[3],
                "endtime": row[4],
                "geom": ogr.CreateGeometryFromWkt(row[5]),
                "granule_id": row[6]
            }

            is_new = True
            for rt in active_roi_tracks:
                if len(active_roi_tracks[rt]):
                    g1 = poly["geom"]
                    g2 = active_roi_tracks[rt]["track"][-1]["geom"]
                    intersection = g1.Intersection(g2)
                    if intersection.IsEmpty() or (intersection.GetGeometryName() <> 'POLYGON'):
                        frac = 0
                    else:
                        frac = intersection.GetArea() / g1.GetArea()

                    if frac > 0.5:
                        active_roi_tracks[rt]["track"].append(poly)
                        active_roi_tracks[rt]["updated"] = True
                        is_new = False

                        #break so that each stormy poly is added to one storm track only
                        break

            if is_new:
                new_storms.append(poly)

        rts2remove = []
        for rt in active_roi_tracks:
            if not active_roi_tracks[rt]["updated"]:
                rts2remove.append(rt)

        for rt in rts2remove:
            sql = """
                insert into roi_tracks (id, starttime, endtime, geom)
                values (%s, %s, %s, ST_GeomFromText(%s))
            """
            points = []
            start_time = datetime.max
            end_time = datetime.min
            for p in active_roi_tracks[rt]["track"]:
                if start_time > p["starttime"]:
                    start_time = p["starttime"]
                if end_time < p["endtime"]:
                    end_time = p["endtime"]

                points.append((p["lon"], p["lat"]))

            if len(points) > 1:
                str_geom = "LINESTRING(" + ",".join(["%s %s" % (x[0], x[1]) for x in points]) + ")"
                pgdb_helper.insert(sql, (rt, start_time, end_time, str_geom))
            elif len(points) == 1:
                str_geom = "POINT(%f %f)" % (points[0][0], points[0][1])
                pgdb_helper.insert(sql, (rt, start_time, end_time, str_geom))

            active_roi_tracks.pop(rt)
            
        for ns in new_storms:
            rt = str(uuid.uuid4())
            active_roi_tracks[rt] = {
                "track": [ns]
            }

        logger.info("Generated tracks for granule %d" % mrms_granule_id)


def generate_ci_events():

    #create table for roi geoms
    pgdb_helper.submit(
        """
            drop table if exists ci_events;
        """
    )

    pgdb_helper.submit(
        """
            create table ci_events
            (
                id serial not null,
                track_id varchar not null,
                starttime timestamp without time zone NOT NULL,
                endtime timestamp without time zone NOT NULL,
                geom geometry not null,
                center_lat float not null,
                center_lon float not null,
                type int not null,
                constraint ci_events_pkey primary key (id)
            );
        """
    )

    sql = """
        select id, starttime, endtime, st_astext(geom) from roi_tracks order by starttime asc
    """
    tracks = pgdb_helper.query(sql)

    for tr in tracks:
        g = ogr.CreateGeometryFromWkt(tr[3])
        if g.GetGeometryName() == "LINESTRING":
            init_point = g.GetPoint(0)
            lon = init_point[0]
            lat = init_point[1]
        elif g.GetGeometryName() == "POINT":
            init_point = g.GetPoint(0)
            lon = init_point[0]
            lat = init_point[1]
        else:
            continue

        data = {
            "track_id": tr[0],
            "starttime": tr[1],
            "endtime": tr[2],
            "geom": "POINT(%f %f)" % (lon, lat),
            "center_lat": lat,
            "center_lon": lon,
            "type": 2
        }

        sql = """
            insert into ci_events (track_id, starttime, endtime, geom, center_lat, center_lon, type)
            values (%s, %s, %s, ST_GeomFromText('POINT(%s %s)', 4326), %s, %s, %s)
            """
        values = (data["track_id"], data["starttime"], data["endtime"], data["center_lon"], data["center_lat"],
                  data["center_lat"], data["center_lon"], data["type"])

        pgdb_helper.insert(sql, values)
        logger.info("Inserted geoms for track %s" % tr[0])

    #create indexes on roi_geoms
    pgdb_helper.submit("create index ci_events_geom_index on ci_events using gist(geom)")
    pgdb_helper.submit("create index ci_events_time_index on ci_events (starttime, endtime)")


if __name__ == '__main__':
    start_date = config.start_date
    end_date = config.end_date

    start_date = datetime(year=2014, month=7, day=23, hour=13, minute=0, second=0)
    end_date = datetime(year=2014, month=7, day=23, hour=18, minute=0, second=0)

    # mrms_granule_rows = get_mrms_granules(start_date, end_date)
    # save_storm_polys(mrms_granules=mrms_granule_rows, threshold=35)
    # generate_storm_tracks(mrms_granules=mrms_granule_rows, threshold=35, ci_lifetime_hours=4)

    generate_ci_events()
    #generate_roi_geoms_from_storm_tracks()
