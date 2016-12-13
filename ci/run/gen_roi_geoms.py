import random
import uuid
import ogr
from datetime import timedelta
from multiprocessing import Pool
from ci.db.pgdbhelper import PGDbHelper
from ci.db.adminpgdbhelper import AdminPGDbHelper
from ci.models.spatial_reference import SRID_RAP
from ci.models.spatial_reference import SRID_ALBERS
from ci.config import get_instance
from ci.util.proj_helper import ProjHelper
from ci.util.common import TimeMe


__author__ = 'ujjwal'


#get accessor to the old db
config = get_instance()
pgdb_helper = PGDbHelper(conn_str=config.pgsql_conn_str(), echo=config.logsql)
#pgdb_helper = PGDbHelper(conn_str=config.pgsql_conn_str(), echo=config.logsql)

proj_helper = ProjHelper(config=config)
logger = config.logger

#create table for roi geoms
pgdb_helper.submit(
    """
    drop table if exists roi_geoms_reproj;
    drop table if exists roi_geoms;
    create table roi_geoms
    (
        id serial not null,
        roi_name text not null,
        mrms_granule_id int not null,
        starttime timestamp without time zone NOT NULL,
        endtime timestamp without time zone NOT NULL,
        geom geometry not null,
        center geometry not null,
        storm_poly geometry null,
        center_lat float not null,
        center_lon float not null,
        iarea float null,
        type int not null,
        constraint roi_geom_pkey primary key (id)
    );
    create index roi_geoms_mrms_gran_type_idx on roi_geoms(mrms_granule_id, starttime, endtime, type);
    create index roi_geoms_geom_gist_idx on roi_geoms using gist(geom);
    """
)

mask_wkt = None
shrink_dist_km = 20
if config.mask_name:
    mask_rows = pgdb_helper.query(
        """
            select st_astext(st_transform(st_buffer(st_transform(geom, 102003), %f), 4326)) from mask
            where name='%s'
        """ % (-1*shrink_dist_km*1000, config.mask_name)
    )
    mask_wkt = mask_rows[0][0]


def get_non_intersecting(roi_rows):
    if len(roi_rows) < 1:
        return []

    roi_rows_non_intersecting = [roi_rows[0]]
    for row1 in roi_rows[1:]:
        intersecting = False
        for row2 in roi_rows_non_intersecting:
            g1 = ogr.CreateGeometryFromWkt(row1[0])
            g2 = ogr.CreateGeometryFromWkt(row2[0])
            if g2.Intersects(g1):
                intersecting = True
                break
        if not intersecting:
            roi_rows_non_intersecting.append(row1)

    return roi_rows_non_intersecting


def find_random_roi_center(mrms_granule_id, ci_lifetime_hours, start_time, end_time, radius=20):
    bbox = proj_helper.get_bbox(srid=4326)
    attempts = 0
    while attempts < 10000:
        lat = random.uniform(bbox[3], bbox[2])
        lon = random.uniform(bbox[0], bbox[1])
        sql = """
            SELECT COUNT(id) FROM ROI_GEOMS
            WHERE type=1 AND ((starttime, endtime) overlaps ('%s', '%s')) AND
            ST_Intersects(
                geom, ST_Transform(ST_Buffer(ST_Transform(ST_GeomFromText('POINT(%s %s)', 4326), %s), %s), 4326)
            )
        """ % (end_time - timedelta(hours=ci_lifetime_hours), end_time, lon, lat, SRID_ALBERS, radius * 1e3)

        rows = pgdb_helper.query(sql)
        if len(rows) and rows[0][0] == 0:
            if mask_wkt:
                sql = """
                    SELECT ST_Within(ST_GeomFromText('POINT(%f %f)', 4326), ST_GeomFromText('%s', 4326))
                """ % (lon, lat, mask_wkt)
                mask_rows = pgdb_helper.query(sql)
                if mask_rows[0][0]:
                    return lon, lat
            else:
                return lon, lat

        attempts += 1

    logger.error("Could not find random roi center for granule %d" % mrms_granule_id)
    return None


def insert_ci_rois(data):

    roi_row = data["roi_row"]
    roi_start_time = data["roi_start_time"]
    roi_end_time = data["roi_end_time"]
    mrms_granule_id = data["mrms_granule_id"]

    roi_geom = roi_row[0]
    roi_iarea = roi_row[1]
    roi_center = roi_row[2]
    roi_center_lon = roi_row[3]
    roi_center_lat = roi_row[4]
    roi_storm_poly = roi_row[5]

    roi_sql = """
        select count(id) from roi_geoms
        where ST_Intersects(geom, ST_GeomFromText('%s', 4326))
        and ((starttime, endtime) overlaps ('%s', '%s')) and type=1
    """ % (roi_geom, roi_end_time - timedelta(hours=ci_lifetime_hours), roi_end_time)

    count_result = pgdb_helper.query(roi_sql)
    if count_result[0][0] == 0:
        if mask_wkt:
            roi_mask_intersection_result = pgdb_helper.query(
                """
                select ST_Intersects (ST_GeomFromText('%s', 4326), ST_GeomFromText('%s', 4326))
                """ % (mask_wkt, roi_geom)
            )
            if not roi_mask_intersection_result[0][0]:
                return False

        roi_name = str(uuid.uuid4())
        #insert type 1 roi - convective initiation
        id = pgdb_helper.insertAndGetId(
            """
                insert into roi_geoms (roi_name, mrms_granule_id, starttime, endtime, geom, center, storm_poly,
                    center_lat, center_lon, iarea, type) values (%s, %s, %s, %s, ST_GeomFromText(%s, 4326),
                    ST_GeomFromText(%s, 4326), ST_GeomFromText(%s, 4326), %s, %s, %s, %s)
            """, (roi_name, mrms_granule_id, roi_start_time, roi_end_time,
              roi_geom, roi_center, roi_storm_poly, roi_center_lat, roi_center_lon, roi_iarea, 1))

        #insert roi's for previous time intervals
        for hour in range(1, ci_lifetime_hours+1, 1):
            id = pgdb_helper.insertAndGetId(
                """
                    insert into roi_geoms (roi_name, mrms_granule_id, starttime, endtime, geom, center, storm_poly,
                    center_lat, center_lon, iarea, type) values (%s, %s, %s, %s, ST_GeomFromText(%s, 4326),
                    ST_GeomFromText(%s, 4326), ST_GeomFromText(%s, 4326), %s, %s, %s, %s)
                """, (roi_name, mrms_granule_id, roi_start_time - timedelta(hours=hour),
                      roi_end_time - timedelta(hours=hour), roi_geom, roi_center, roi_storm_poly, roi_center_lat,
                      roi_center_lon, roi_iarea, -1*hour))
        return True
    return False


def insert_non_ci_rois(data):
    roi_start_time = data["roi_start_time"]
    roi_end_time = data["roi_end_time"]
    mrms_granule_id = data["mrms_granule_id"]
    radius = data["radius"]

    #insert random roi
    lonlat = find_random_roi_center(mrms_granule_id, ci_lifetime_hours, roi_start_time, roi_end_time)
    ran_roi_name = str(uuid.uuid4())

    #logger.info("Find randmom roi in %f sec" % tm3.interval)
    if lonlat:
        id = pgdb_helper.insertAndGetId(
            """
              insert into roi_geoms (roi_name, mrms_granule_id, starttime, endtime, geom, center, storm_poly,
              center_lat, center_lon, iarea, type) values (
                %s, %s, %s, %s, ST_Transform(ST_Buffer(ST_Transform(ST_GeomFromText('POINT(%s %s)', 4326), %s),
                %s), 4326), ST_GeomFromText('POINT(%s %s)', 4326), %s, %s, %s, %s, %s)
            """, (ran_roi_name, mrms_granule_id, roi_start_time, roi_end_time, lonlat[0],
                  lonlat[1], SRID_ALBERS, radius * 1e3, lonlat[0], lonlat[1], None, lonlat[1], lonlat[0], 0, 0))

        #insert random roi
        for hour in range(1, ci_lifetime_hours + 1, 1):
            id = pgdb_helper.insertAndGetId(
                """
                  insert into roi_geoms (roi_name, mrms_granule_id, starttime, endtime, geom, center,
                  storm_poly, center_lat, center_lon, iarea, type) values (%s, %s, %s, %s,
                  ST_Transform(ST_Buffer(ST_Transform(ST_GeomFromText('POINT(%s %s)', 4326), %s), %s), 4326),
                  ST_GeomFromText('POINT(%s %s)', 4326), %s, %s, %s, %s, %s)
                """, (ran_roi_name, mrms_granule_id, roi_start_time-timedelta(hours=hour),
                      roi_end_time-timedelta(hours=hour), lonlat[0], lonlat[1], SRID_ALBERS,
                      radius * 1e3, lonlat[0], lonlat[1], None, lonlat[1], lonlat[0], 0, -4-1*hour))


def generate_mrms_rois(mrms_granule, radius, threshold, ci_lifetime_hours, min_storm_area=1e6, max_storm_area=5e6,
                       disjoint=True):

    mrms_granule_id = mrms_granule[0] #mrms_granule.id
    mrms_granule_endtime = mrms_granule[2] #mrms_granule.endtime
    mrms_granule_starttime = mrms_granule[1]

    roi_start_time = mrms_granule_starttime
    roi_end_time = mrms_granule_endtime

    roi_rows = []
    try:
        roi_rows = pgdb_helper.get_rois_wkt(granule_id=mrms_granule_id, radius=radius, threshold=threshold,
                                    srid=4326, min_storm_area=min_storm_area, max_storm_area=max_storm_area)
    except Exception as ex:
        logger.error("Failed while executing get rois wkt for granule id %s" % mrms_granule_id)

    #if disjoint is true then ensure rois are non intersecting
    if disjoint:
        roi_rows_insert = get_non_intersecting(roi_rows)
    else:
        roi_rows_insert = roi_rows

    ci_data = []
    for r in roi_rows_insert:
        ci_data.append({
            "roi_row": r,
            "roi_start_time": roi_start_time,
            "roi_end_time": roi_end_time,
            "mrms_granule_id": mrms_granule_id,
            "radius": radius
        })

    non_ci_data = []
    for d in ci_data:
        status = insert_ci_rois(d) #satus true if d was inserted
        if status:
            non_ci_data.append(d)

    for d in non_ci_data:
        insert_non_ci_rois(d)


if __name__ == '__main__':

    with TimeMe() as tm:
        #set statement timeout
        sql = "SET statement_timeout TO '5min';"
        pgdb_helper.submit(sql)

        start_date = config.start_date
        end_date = config.end_date
        ci_lifetime_hours = config.ci_lifetime_hours
        ci_roi_radius = config.ci_roi_radius
        ci_threshold_dbz = config.ci_threshold_dbz

        sql = """
            select datagranule.id, datagranule.starttime, datagranule.endtime, datagranule.name from datagranule
            join provider on provider.id = datagranule.provider_id
            join variable on variable.id = datagranule.variable_id
            where provider.name like 'MRMS' and variable.name like 'REFL' and datagranule.level=14
            and (('%s', '%s') overlaps (datagranule.starttime, datagranule.endtime))
            order by datagranule.starttime asc
        """ % (start_date, end_date)
        mrms_granule_rows = pgdb_helper.query(sql)

        if len(mrms_granule_rows):
            #generate first set of roi ids with very small and large storm areas so as to catch most storms
            min_storm_area = 1e3 #min area = km2
            max_storm_area = 1e9
            try:
                generate_mrms_rois(mrms_granule=mrms_granule_rows[0], radius=ci_roi_radius, threshold=ci_threshold_dbz,
                               ci_lifetime_hours=ci_lifetime_hours, min_storm_area=min_storm_area,
                               max_storm_area=max_storm_area, disjoint=False)
            except Exception, ex:
                logger.critical("Error inserting rois for mrms granule %d" % mrms_granule_rows[0][0])

            min_storm_area = 1e6 #min area = km2
            max_storm_area = 1e7 #max area = km2
            for mrms_granule in mrms_granule_rows[1:]:
                try:
                    generate_mrms_rois(mrms_granule=mrms_granule, radius=ci_roi_radius,
                        threshold=ci_threshold_dbz, ci_lifetime_hours=ci_lifetime_hours,
                        min_storm_area=min_storm_area, max_storm_area=max_storm_area, disjoint=True)
                except Exception, ex:
                    logger.critical("Error inserting rois for mrms granule %d" % mrms_granule[0])

            #delete rois created from the first granule so we only have initiations
            first_granule_id = mrms_granule_rows[0][0]
            sql = """
                delete from roi_geoms where mrms_granule_id=%d
            """ % first_granule_id
            pgdb_helper.submit(sql)
        else:
            logger.critical("Did not find any MRMS Granules")

    logger.info("Finished gen_roi_geoms in %d s" % tm.interval)

    ##### Reproject roi_geoms #############
    sql = """
    drop table if exists roi_geoms_reproj;
    select id, roi_name, mrms_granule_id, starttime, endtime, st_transform(geom, {0}) geom,
        st_transform(center, {0}) center, st_transform(storm_poly, {0}) storm_poly,
    center_lat, center_lon, iarea, type
    into roi_geoms_reproj
    from roi_geoms;

    ALTER TABLE roi_geoms_reproj
      ADD CONSTRAINT roi_geom_reproj_pkey PRIMARY KEY(id);
    create index roi_geoms_reproj_gist_idx on roi_geoms_reproj using gist(geom);
    """.format(SRID_RAP)
    pgdb_helper.submit(sql)

    logger.info("Reprojected roi_geoms")
