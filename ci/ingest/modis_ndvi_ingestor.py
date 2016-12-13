from datetime import datetime
from ci.models.gdal_raster import GDALRaster
from ci.util.gdal_helper import get_sds, get_metadata
from ci.models.spatial_reference import SRID_MODIS
from ci.ingest import config, logger, proj_helper, base_ingestor

__author__ = 'ujjwal'

provider_name = "MODIS"
variable_name = "NDVI"

if __name__ == "__main__":
    df = config.datafiles["MODIS_NDVI"]
    if isinstance(df["wildcard"], list):
        files = []
        for wc in df["wildcard"]:
            files += base_ingestor.get_ingest_files(df["folder"], wc)
    else:
        files = base_ingestor.get_ingest_files(df["folder"], df["wildcard"])

    for hdf_file in files:
        sds = get_sds(hdf_file, "250m 16 days NDVI")
        vars = ["RANGEBEGINNINGDATE", "RANGEENDINGDATE"]
        datevalues = get_metadata(hdf_file, vars)
        start_date = datetime.strptime(datevalues["RANGEBEGINNINGDATE"], "%Y-%m-%d")
        end_date = datetime.strptime(datevalues["RANGEENDINGDATE"], "%Y-%m-%d")

        granule_name = "MODIS_NDVI_%s" % (start_date.strftime("%Y-%m-%d"))

        srid = SRID_MODIS
        level = 0
        block_size = (100, 100)

        ras = GDALRaster(sds, srid)
        bbox = proj_helper.get_bbox(srid)

        base_ingestor.ingest(ras=ras, provider_name=provider_name, variable_name=variable_name,
            granule_name=granule_name, table_name=granule_name, srid=srid, level=level, block_size=block_size,
            dynamic=False, subset_bbox=bbox, start_time=start_date, end_time=end_date, overwrite=False)
