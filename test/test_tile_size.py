import gzip
import os
from datetime import datetime, timedelta
from ci.models.gdal_raster import GDALRaster
from ci.util.nc_file_helper import nc_get_1d_vars_as_list
from ci.ingest import config, base_ingestor, proj_helper
from ci.config import get_instance
from ci.db.pgdbhelper import PGDbHelper
from ci.util.common import TimeMe

block_sizes = []
for i in range(1, 50):
    block_sizes.append((i*20, i*20))

conf = get_instance()
pgdb_helper = PGDbHelper(conn_str=conf.pgsql_conn_str(), echo=conf.logsql)


def cb(x):
    if x < 35:
        return 0
    else:
        return 1


def process_mrms_file(mrms_file):

    provider_name = "MRMS"
    variable_name = "REFL"

    ext_parts = os.path.splitext(mrms_file)
    ext = ext_parts[1]
    remove_after_process = False

    if ext == ".gz":
        nc_file_name = ext_parts[0]
        nc_file_copy = os.path.join("./", os.path.basename(nc_file_name))
        if os.path.exists(nc_file_copy):
            mrms_file = nc_file_copy
        else:
            with open(nc_file_copy, 'wb') as nc_file:
                gz_file = gzip.open(mrms_file, 'rb')
                gz_bytes = gz_file.read()

                nc_file.write(gz_bytes)
                gz_file.close()

                mrms_file = nc_file_copy
                remove_after_process = True

    vars = nc_get_1d_vars_as_list(mrms_file, ["Ht", "time"])
    heights = vars["Ht"]
    times = vars["time"]
    srid = 4326

    #dtime = datetime(year=2014, month=8, day=18, hour=19, minute=0, second=0)
    dtime = datetime.utcfromtimestamp(times[0])
    bbox = proj_helper.get_bbox(srid)
    start_time = dtime
    end_time = dtime + timedelta(minutes=2)

    for block_size in block_sizes:
        level = block_size[0] #put various tiles in various levels
        granule_name = "%s_%s %s_%d" % (provider_name, variable_name, dtime.strftime("%Y%m%d %H:%M"), level)
        table_name = "%s_%s_%s_%d" % (provider_name, variable_name, dtime.strftime("%Y%m%d%H%M"), level)

        bottom_up_data = True
        ras = GDALRaster(mrms_file, srid, bottom_up_data)
        l = 14
        ras.set_band_num(l+1)

        #explicitly override the noddata_value since netcdf file is not correct
        ras.nodata_value = -999
        ras.nodata_range = (-999, 0)
        #ras.reclassifier_callback = cb

        base_ingestor.ingest(ras=ras, provider_name=provider_name, variable_name=variable_name,
                             granule_name=granule_name, table_name=granule_name, srid=srid, level=level,
                             block_size=block_size, dynamic=False, subset_bbox=bbox, start_time=start_time,
                             end_time=end_time, overwrite=True, threshold=34)

    if remove_after_process:
        os.remove(mrms_file)


def ingest_mrms_files():
    df = config.datafiles["MRMS_MREFL"]
    if isinstance(df["wildcard"], list):
        files = []
        for wc in df["wildcard"]:
            files += base_ingestor.get_ingest_files(df["folder"], wc)
    else:
        files = base_ingestor.get_ingest_files(df["folder"], df["wildcard"])

    for f in files:
        process_mrms_file(f)
        break


def ingest_gtopo_file():
    provider_name = "GTOPO30"
    variable_name = "ELEV"
    df = config.datafiles["GTOPO30_ELEV"]

    if isinstance(df["wildcard"], list):
        files = []
        for wc in df["wildcard"]:
            files += base_ingestor.get_ingest_files(df["folder"], df["wildcard"])
    else:
        files = base_ingestor.get_ingest_files(df["folder"], df["wildcard"])

    gtopo_file = files[0]
    srid = 4326
    band_num = 1
    dtime = datetime(year=1979, month=1, day=1, hour=0, minute=0, second=0)

    for block_size in block_sizes:
        level = block_size[0]
        granule_name = "GTOPO30Elev_%d" % level

        ras = GDALRaster(gtopo_file, srid)
        ras.nodata_value = -9999
        bbox = proj_helper.get_bbox(srid)

        base_ingestor.ingest(ras=ras, provider_name=provider_name, variable_name=variable_name,
                             granule_name=granule_name, table_name=granule_name, srid=srid, level=level,
                             block_size=block_size, dynamic=False, start_time=dtime, end_time=datetime.max,
                             subset_bbox=bbox, overwrite=True)


def test_speed_ras(block_size):
    provider_name = "GTOPO30"
    variable_name = "ELEV"

    level = block_size[0]
    sql = """
        select datagranule.id from datagranule join variable on variable.id=datagranule.variable_id
        where level=%s and variable.name=%s
    """
    dgid_rows = pgdb_helper.query(sql, (level, variable_name))
    dgid = dgid_rows[0][0]

    min_storm_area = 1e6 #min area = km2
    max_storm_area = 1e6 * 10
    radius = 30
    threshold = 25

    with TimeMe() as tm:
        roi_rows = pgdb_helper.get_rois_wkt(granule_id=dgid, radius=radius, threshold=threshold,
            srid=4326, min_storm_area=min_storm_area, max_storm_area=max_storm_area)

    conf.logger.info("dg_id=%d, block_size=%d, time=%f, count=%d" % (dgid, block_size[0], tm.interval, len(roi_rows)))


if __name__ == "__main__":
    ingest_gtopo_file()
    # for b in block_sizes:
    #     test_speed(b)
