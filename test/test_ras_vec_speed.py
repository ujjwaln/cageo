from datetime import datetime, timedelta
from ci.models.gdal_raster import GDALRaster
from ci.ingest import config, base_ingestor, proj_helper
from ci.config import get_instance
from ci.db.pgdbhelper import PGDbHelper
from ci.models.spatial_reference import SRID_ALBERS
from ci.util.common import TimeMe
from numpy import random


conf = get_instance()
pgdb_helper = PGDbHelper(conn_str=conf.pgsql_conn_str(), echo=conf.logsql)

provider_name = "GTOPO30"
variable_name = "ELEV"

srid = 4326
band_num = 1
dtime = datetime(year=1979, month=1, day=1, hour=0, minute=0, second=0)

block_size = (20, 20)


def ingest_gtopo_file(fmt):

    df = config.datafiles["GTOPO30_ELEV"]
    if isinstance(df["wildcard"], list):
        files = []
        for wc in df["wildcard"]:
            files += base_ingestor.get_ingest_files(df["folder"], df["wildcard"])
    else:
        files = base_ingestor.get_ingest_files(df["folder"], df["wildcard"])

    gtopo_file = files[0]

    ras = GDALRaster(gtopo_file, srid)
    ras.nodata_value = -9999
    bbox = proj_helper.get_bbox(srid)

    if fmt == 'ras':
        granule_name = "GTOPO30Elev_ras"
        level = 0
        base_ingestor.ingest(ras=ras, provider_name=provider_name, variable_name=variable_name,
                             granule_name=granule_name, table_name=granule_name, srid=srid, level=level,
                             block_size=block_size, dynamic=False, start_time=dtime, end_time=datetime.max,
                             subset_bbox=bbox, overwrite=True)

        pgdb_helper.submit(
            """
            drop if exists index rastertile_geom_gist_idx;
            create index rastertile_geom_gist_idx on rastertile using gist(st_convexhull(rast));
            """
        )

    if fmt == 'vec':
        granule_name = "GTOPO30Elev_vec"
        level = 1
        base_ingestor.ingest_vector(ras=ras, provider_name=provider_name, variable_name=variable_name,
                             granule_name=granule_name, table_name=granule_name, srid=srid, level=level,
                             block_size=block_size, start_time=dtime, end_time=datetime.max,
                             subset_bbox=bbox, overwrite=True)


def test_speed():
    level = 0
    sql = """
        select dg.id, st_xmin(extent), st_xmax(extent), st_ymin(extent), st_ymax(extent)
        from datagranule dg
        join variable on variable.id=dg.variable_id
        where level=%s and variable.name=%s
    """
    rows = pgdb_helper.query(sql, (level, variable_name))
    id = rows[0][0]
    xmin = rows[0][1]
    xmax = rows[0][2]
    ymin = rows[0][3]
    ymax = rows[0][4]

    rnd_x = random.rand(10, 1)
    rnd_y = random.rand(10, 1)
    xs = xmin + (xmax-xmin)*rnd_x
    ys = ymin + (ymax-ymin)*rnd_y

    radius = 20 * 1e3
    for i in range(0, len(xs)):
        vec_sql = """
            select count(value), stddev(value), avg(value) from {table_name}
            where st_contains(
                st_transform(st_buffer(st_transform(st_geomfromtext('POINT({x} {y})', 4326), {srid_albers}), {radius}), 4326),
                geom
            )
        """.format(table_name="GTOPO30Elev_vec", x=xs[i][0], y=ys[i][0], srid_albers=SRID_ALBERS, radius=radius)

        with TimeMe() as tm:
            rows = pgdb_helper.query(vec_sql)
        config.logger.info("VEC- %d vals, in %f seconds" % (rows[0][0], tm.interval))

        ras_sql = """
            select st_summarystats(
                st_clip(
                    st_union(rast), 1,
                    st_transform(st_buffer(st_transform(st_geomfromtext('POINT({x} {y})', 4326), {srid_albers}), {radius}), 4326),
                    NULL, True
                )
            ) from rastertile
            where datagranule_id={granule_id}
            and st_intersects(rast,
            st_transform(st_buffer(st_transform(st_geomfromtext('POINT({x} {y})', 4326), {srid_albers}), {radius}), 4326))
        """.format(x=xs[i][0], y=ys[i][0], srid_albers=SRID_ALBERS, radius=radius, granule_id=id)

        with TimeMe() as tm:
            rows = pgdb_helper.query(ras_sql)

        if rows[0][0] and len(rows[0][0]):
            cnt = ((rows[0][0].split(','))[0])[1:]
        else:
            cnt = 0

        config.logger.info("RAS- %s vals in %f seconds" % (cnt, tm.interval))


if __name__ == "__main__":
    fmt = 'ras'
    #ingest_gtopo_file(fmt)
    test_speed()