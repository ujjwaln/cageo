from multiprocessing import Pool
from ci.db.pgdbhelper import PGDbHelper
from ci.config import get_instance
from ci.util.common import TimeMe
from ci.models.spatial_reference import SRID_RAP
import hashlib


__author__ = 'ujjwal'


#get accessor to the old db
config = get_instance()
logger = config.logger

start_date = config.start_date
end_date = config.end_date
pgdb_helper = PGDbHelper(conn_str=config.pgsql_conn_str(), echo=config.logsql)

RASTERTILE_TABLE = "rastertile_reproj"
cache = {}


def __insert_stats(roi_id, roi_name, variable_id, stats_rows):

    i = 0
    count = 0
    sum = 0
    mean = 0
    stddev = 0
    min = 0
    max = 0

    for stats_row in stats_rows:
        if len(stats_row):
            str_vals = stats_row[1:-1].split(',')
            if len(str_vals[0]) and len(str_vals[1]) and len(str_vals[2]) and \
                len(str_vals[3]) and len(str_vals[4]) and len(str_vals[5]):

                count = (int(str_vals[0]) + i * count) / (i + 1)
                sum = (float(str_vals[1]) + i * sum) / (i + 1)
                mean = (float(str_vals[2]) + i * mean) / (i + 1)
                stddev = (float(str_vals[3]) + i * stddev) / (i + 1)
                min = (float(str_vals[4]) + i * min) / (i + 1)
                max = (float(str_vals[5]) + i * max) / (i + 1)

                i += 1

    if i > 0:
        pgdb_helper.insert(
            """
            insert into roi_stats
                (roi_id, roi_name, count, sum, mean, stddev, min, max, variable_id)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (roi_id, roi_name, count, sum, mean, stddev, min, max, variable_id)
        )
        logger.info("Inserted stats for %d" % roi_id)
    else:
        logger.warn("Could not generate ROI stats for %d" % roi_id)


def insert_roi_stats(roi_dg):
    roi_id = roi_dg["roi_id"]
    roi_name = roi_dg["roi_name"]
    dg_ids = roi_dg["dg_ids"]
    #geom_wkt = roi_dg["geom_wkt"]

    sql = """
        select baz.datagranule_id, dg.variable_id, st_summarystats(baz.clip_ras, true)
        from (
         select bar.datagranule_id, st_clip(bar.ras, (select geom from roi_geoms_reproj where id=%d limit 1)) clip_ras
         from (
          select rt.datagranule_id, st_union(rt.rast) ras from rastertile_reproj rt
          where rt.datagranule_id in (%s) and st_intersects((select geom from roi_geoms_reproj where id=%d limit 1), rt.rast)
          group by rt.datagranule_id
         ) bar
        ) baz
        join datagranule dg on dg.id=baz.datagranule_id
        order by dg.variable_id
    """ % (roi_id, ",".join([str(i) for i in dg_ids]), roi_id)

    results = pgdb_helper.query(sql)
    variable_id = -1
    stats_rows = []

    for row in results:
        if variable_id <> row[1]:
            if len(stats_rows):
                __insert_stats(roi_id, roi_name, variable_id, stats_rows)
            stats_rows = [row[2]]
        else:
            stats_rows.append(row[2])

        variable_id = row[1]

    if len(stats_rows):
        __insert_stats(roi_id, roi_name, variable_id, stats_rows)


if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(description="Generate ROI Stats, --variable_id will process for given variable_id")
    parser.add_argument('--variable_id', type=int)
    args = parser.parse_args()

    sql = "select id from variable where name like 'REFL'"
    rows = pgdb_helper.query(sql)
    refl_variable_id = rows[0][0]

    roi_dg_pairs = {}

    if args.variable_id is not None:
        sql = """
            select rg.id, rg.roi_name, dg.id from roi_geoms_reproj rg
            inner join datagranule dg on (rg.starttime, rg.endtime) overlaps (dg.starttime, dg.endtime)
            where variable_id = %d and st_within(st_transform(rg.center, dg.srid), dg.extent)
        """ % args.variable_id
        logger.info("Generating roi stats for variable_id %d" % args.variable_id)
    else:
        # delete and recreate roi_stats table
        pgdb_helper.submit(
            """
                drop table if exists roi_stats;
            """
        )
        pgdb_helper.submit(
            """
                create table roi_stats
                (
                    id serial not null,
                    roi_id int not null,
                    roi_name text not null,
                    count int not null,
                    sum float not null,
                    mean float not null,
                    stddev float not null,
                    min float not null,
                    max float not null,
                    variable_id int not null
                )
            """)

        #select overlapping roi_geoms and non mrms datagranules
        sql = """
            select rg.id, rg.roi_name, dg.id from roi_geoms_reproj rg
            inner join datagranule dg on (rg.starttime, rg.endtime) overlaps (dg.starttime, dg.endtime)
            where dg.variable_id <> %d and st_within(st_transform(rg.center, dg.srid), dg.extent)
        """ % refl_variable_id
        logger.info("Generating roi stats for all variables")

    if config.mask_name is not None:
        sql += """
            and st_intersects(rg.geom,
                (select st_transform(geom, st_srid(rg.geom)) from mask where name='%s' limit 1)
            )
        """ % config.mask_name

    sql += " order by rg.id asc"
    rows = pgdb_helper.query(sql)

    for row in rows:
        key = row[0]
        if key in roi_dg_pairs:
            roi_dg_pairs[key]["dg_ids"].append(row[2])
        else:
            roi_dg_pairs[key] = {
                "roi_id": row[0],
                "roi_name": row[1],
                "dg_ids": [row[2]] #list
            }

    logger.info("%d rois for calculating stats" % len(roi_dg_pairs))
    parallel = config.parallel

    with TimeMe() as tm:
        if parallel:
            n_proc = config.nprocs
            p = Pool(n_proc)
            p.map(insert_roi_stats, roi_dg_pairs.values())
            p.close()
            p.join()
        else:
            for key in roi_dg_pairs:
                insert_roi_stats(roi_dg_pairs[key])

    logger.info("Inserted_roi_stats roi_id in %s sec" % tm.interval)