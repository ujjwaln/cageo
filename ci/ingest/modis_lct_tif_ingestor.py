from datetime import datetime
from ci.models.gdal_raster import GDALRaster
from ci.models.spatial_reference import SRID_MODIS
from ci.ingest import config, logger, proj_helper, base_ingestor

__author__ = 'ujjwal'

provider_name = "MODIS"


if __name__ == "__main__":
    df = config.datafiles["MODIS_LCT_TIF"]
    tif_files = base_ingestor.get_ingest_files(df["folder"], df["wildcard"])
    tif_file = tif_files[0]

    start_date = datetime(year=2014, month=6, day=1)
    end_date = datetime(year=2014, month=9, day=1)

    srid = SRID_MODIS
    level = 0
    block_size = (500, 500)
    variable_name = "LCT"
    bbox = proj_helper.get_bbox(srid)

    granule_name = "%s_%s %s_%d" % (provider_name, variable_name, start_date.strftime("%Y%m%d"), level)
    table_name = "%s_%s_%s_%d" % (provider_name, variable_name, start_date.strftime("%Y%m%d"), level)
    ras = GDALRaster(tif_file, srid)
    ras.nodata_value = 0
    base_ingestor.ingest(ras=ras, provider_name=provider_name, variable_name=variable_name, granule_name=granule_name,
           table_name=granule_name, srid=srid, level=level, block_size=block_size, dynamic=False,
           subset_bbox=bbox, start_time=start_date, end_time=end_date, overwrite=True)

    variable_name = "WATERBODY"
    granule_name = "%s_%s %s_%d" % (provider_name, variable_name, start_date.strftime("%Y%m%d"), level)
    table_name = "%s_%s_%s_%d" % (provider_name, variable_name, start_date.strftime("%Y%m%d"), level)
    water_ras = GDALRaster(tif_file, srid)
    water_ras.nodata_value = 255
    water_ras.nodata_range = [0.5, 256] #only 0 (water)
    water_ras.reclassifier = {0: 1}

    base_ingestor.ingest(ras=water_ras, provider_name=provider_name, variable_name=variable_name,
        granule_name=granule_name, table_name=granule_name, srid=srid, level=level, block_size=block_size,
        dynamic=False, subset_bbox=bbox, start_time=start_date, end_time=end_date, overwrite=True)
