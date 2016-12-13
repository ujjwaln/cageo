import os
from datetime import datetime, timedelta
from ci.models.gdal_raster import GDALRaster
from ci.models.spatial_reference import SRID_MODIS
from ci.ingest import config, logger, base_ingestor, proj_helper

__author__ = 'ujjwal'


provider_name = "MODIS"
variable_name = "NDVI"


if __name__ == "__main__":
    df = config.datafiles["MODIS_NDVI_TIF"]
    if isinstance(df["wildcard"], list):
        files = []
        for wc in df["wildcard"]:
            files += base_ingestor.get_ingest_files(df["folder"], wc)
    else:
        files = base_ingestor.get_ingest_files(df["folder"], df["wildcard"])

    for tif_file in files:
        srid = SRID_MODIS
        level = 0
        block_size = (50, 50)
        doy = int(os.path.splitext(os.path.basename(tif_file))[0])
        jan1 = datetime(year=2014, month=1, day=1)
        start_date = jan1 + timedelta(days=doy)
        end_date = jan1 + timedelta(days=doy+15, hours=23, minutes=59, seconds=59)

        ras = GDALRaster(tif_file, srid)
        bbox = proj_helper.get_bbox(srid)

        granule_name = "MODIS_NDVI_%s" % (start_date.strftime("%Y-%m-%d"))
        base_ingestor.ingest(ras=ras, provider_name=provider_name, variable_name=variable_name,
            granule_name=granule_name, table_name=granule_name, srid=srid, level=level, block_size=block_size,
            dynamic=False, subset_bbox=bbox, start_time=start_date, end_time=end_date, overwrite=False)
