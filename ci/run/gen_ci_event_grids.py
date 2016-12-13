from datetime import datetime, timedelta
from gdal import gdalconst
from ci.models.spatial_reference import RAP_Spatial_Reference
from ci.models.array_raster import ArrayRaster
from ci.db.pgdbhelper import PGDbHelper
from ci.ingest import proj_helper, config, logger, base_ingestor

__author__ = 'ujjwal'

nodata = 0
scale = 13000, -13000
bbox = proj_helper.get_bbox(RAP_Spatial_Reference.epsg)
ul = (bbox[0] - scale[0] * 0.5, bbox[2] + scale[0] * 0.5)
size = int((bbox[1]-bbox[0]) / scale[0]), int((bbox[3]-bbox[2]) / scale[1])


def save_raster(lats, lons, t_start, t_end):
    x, y = proj_helper.latlon2xy1(lats, lons, RAP_Spatial_Reference.proj4)
    data = [1 for i in range (0, len(x))]
    array_raster = ArrayRaster(ds_name="", data_array=None, size=size, ul=ul, scale=scale, skew=(0, 0),
                               srid=RAP_Spatial_Reference.epsg, gdal_datatype=gdalconst.GDT_Int16, nodata_value=999)

    array_raster.set_data_with_xy(x=x, y=y, data=data, stat="count")

    level = 0
    block_size = 50, 50 #array_raster.size # 100, 100
    variable_name = "CI_COUNT"
    provider_name = "MRMS"
    granule_name = "%s_%s_%s" % (provider_name, variable_name, dtime.strftime("%Y%d%m%H%M"))

    base_ingestor.ingest(ras=array_raster, provider_name=provider_name, variable_name=variable_name,
                         granule_name=granule_name, table_name=granule_name, srid=RAP_Spatial_Reference.epsg,
                         level=level, block_size=block_size, dynamic=False, start_time=t_start, end_time=t_end,
                         subset_bbox=bbox, overwrite=True)

    logger.info("Inserted %s" % granule_name)


start_time = datetime(year=2014, month=7, day=23, hour=13, minute=0, second=0)
end_time = datetime(year=2014, month=7, day=23, hour=18, minute=0, second=0)

dtime = start_time
tstep = timedelta(hours=1)
pgdb_helper = PGDbHelper(conn_str=config.pgsql_conn_str(), echo=True)

while dtime < end_time:
    sql = """
        select id, track_id, starttime, endtime, center_lat, center_lon, type from ci_events
        where starttime >= %s and starttime < %s order by starttime
    """
    values = (dtime, dtime + tstep)
    rows = pgdb_helper.query(sql, values)

    if len(rows):
        lats = []
        lons = []
        for row in rows:
            lats.append(row[4])
            lons.append(row[5])

        save_raster(lats, lons, dtime, dtime+tstep)
    else:
        logger.info("No CI Events for %s" % dtime)

    dtime = dtime + tstep
