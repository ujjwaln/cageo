import gzip
import os
from datetime import datetime, timedelta
from multiprocessing import Pool
from ci.models.gdal_raster import GDALRaster
from ci.util.nc_file_helper import nc_get_1d_vars_as_list
from ci.ingest import config, logger, base_ingestor, proj_helper


__author__ = 'ujjwal'


def cb(x):
    if x < 35:
        return 0
    else:
        return 1


def process_file(mrms_file):
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
    provider_name = "MRMS"
    variable_name = "REFL"
    srid = 4326

    #dtime = datetime(year=2014, month=8, day=18, hour=19, minute=0, second=0)
    dtime = datetime.utcfromtimestamp(times[0])
    #logger.info("write to postgis - %s" % mrms_file)
    block_size = (50, 50)

    level = 14
    bottom_up_data = True
    ras = GDALRaster(mrms_file, srid, bottom_up_data)
    ras.set_band_num(level+1)

    #explicitly override the noddata_value since netcdf file is not correct
    ras.nodata_value = -999
    ras.nodata_range = (-999, 0)
    #ras.reclassifier_callback = cb
    threshold = 34 #only tiles containing > 34dbz values will be inserted

    granule_name = "%s_%s %s_%d" % (provider_name, variable_name, dtime.strftime("%Y%m%d %H:%M"), level)
    table_name = "%s_%s_%s_%d" % (provider_name, variable_name, dtime.strftime("%Y%m%d%H%M"), level)

    bbox = proj_helper.get_bbox(srid)

    start_time = dtime
    end_time = dtime + timedelta(minutes=2)

    base_ingestor.ingest(ras=ras, provider_name=provider_name, variable_name=variable_name, granule_name=granule_name,
       table_name=granule_name, srid=srid, level=level, block_size=block_size, dynamic=False,
       subset_bbox=bbox, start_time=start_time, end_time=end_time, overwrite=True, threshold=threshold)

    if remove_after_process:
        os.remove(mrms_file)


if __name__ == "__main__":
    df = config.datafiles["MRMS_MREFL"]
    if isinstance(df["wildcard"], list):
        files = []
        for wc in df["wildcard"]:
            files += base_ingestor.get_ingest_files(df["folder"], wc)
    else:
        files = base_ingestor.get_ingest_files(df["folder"], df["wildcard"])

    parallel = config.parallel
    if parallel:
        n_proc = config.nprocs
        pool_size = min(n_proc, len(files))
        logger.info("Using pool size %d" % pool_size)

        p = Pool(pool_size)
        p.map(process_file, files)

        p.close()
        p.join()
    else:
        for f in files:
            process_file(f)
