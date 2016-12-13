from datetime import datetime
from ci.models.gdal_raster import GDALRaster
from ci.util.gdal_helper import get_sds, get_metadata
from ci.models.spatial_reference import SRID_MODIS
from ci.ingest import config, logger, proj_helper, base_ingestor

__author__ = 'ujjwal'


provider_name = "MODIS"
variable_name = "LCT"


if __name__ == "__main__":
    df = config.datafiles["MODIS_LCT"]
    if isinstance(df["wildcard"], list):
        files = []
        for wc in df["wildcard"]:
            files += base_ingestor.get_ingest_files(df["folder"], wc)
    else:
        files = base_ingestor.get_ingest_files(df["folder"], df["wildcard"])

    for hdf_file in files:
        sds = get_sds(hdf_file, "Land_Cover_Type_2")

        vars = ["RANGEBEGINNINGDATE", "RANGEENDINGDATE"]
        datevalues = get_metadata(hdf_file, vars)
        start_date = datetime.strptime(datevalues["RANGEBEGINNINGDATE"], "%Y-%m-%d")

        #end_date = datetime.strptime(datevalues["RANGEENDINGDATE"], "%Y-%m-%d")
        end_date = datetime(year=2019, month=1, day=1)

        srid = SRID_MODIS
        level = 0
        block_size = (50, 50)
        granule_name = "%s_%s %s_%d" % (provider_name, variable_name, start_date.strftime("%Y%m%d"), level)
        table_name = "%s_%s_%s_%d" % (provider_name, variable_name, start_date.strftime("%Y%m%d"), level)

        ras = GDALRaster(sds, srid)
        #ras.nodata_range = [0.5, 256] #only 0 (water)

        bbox = proj_helper.get_bbox(srid)
        base_ingestor.ingest(ras=ras, provider_name=provider_name, variable_name=variable_name, granule_name=granule_name,
               table_name=granule_name, srid=srid, level=level, block_size=block_size, dynamic=False,
               subset_bbox=bbox, start_time=start_date, end_time=end_date, overwrite=True)

    #also ingest same data as land water mask
    for hdf_file in files:
        sds = get_sds(hdf_file, "Land_Cover_Type_2")
        provider_name = "MODIS"
        variable_name = "WATERBODY"

        vars = ["RANGEBEGINNINGDATE", "RANGEENDINGDATE"]
        datevalues = get_metadata(hdf_file, vars)
        start_date = datetime.strptime(datevalues["RANGEBEGINNINGDATE"], "%Y-%m-%d")

        #end_date = datetime.strptime(datevalues["RANGEENDINGDATE"], "%Y-%m-%d")
        end_date = datetime(year=2015, month=1, day=1)

        srid = 96842
        level = 0
        block_size = (50, 50)
        granule_name = "%s_%s %s_%d" % (provider_name, variable_name, start_date.strftime("%Y%m%d"), level)
        table_name = "%s_%s_%s_%d" % (provider_name, variable_name, start_date.strftime("%Y%m%d"), level)

        ras = GDALRaster(sds, srid)
        ras.nodata_range = [0.5, 256] #only 0 (water)
        ras.reclassifier = {0: 1}

        alabama_bbox = proj_helper.get_bbox(srid)
        base_ingestor.ingest(ras=ras, provider_name=provider_name, variable_name=variable_name, granule_name=granule_name,
               table_name=granule_name, srid=srid, level=level, block_size=block_size, dynamic=False,
               subset_bbox=alabama_bbox, start_time=start_date, end_time=end_date, overwrite=True)
