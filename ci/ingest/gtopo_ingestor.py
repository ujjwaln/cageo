from datetime import datetime
from ci.models.gdal_raster import GDALRaster
from ci.db.pgdbhelper import PGDbHelper
from ci.ingest import logger, proj_helper, config, base_ingestor

__author__ = 'ujjwal'

provider_name = "GTOPO30"
variable_name = "ELEV"

if __name__ == "__main__":
    df = config.datafiles["GTOPO30_ELEV"]
    if isinstance(df["wildcard"], list):
        files = []
        for wc in df["wildcard"]:
            files += base_ingestor.get_ingest_files(df["folder"], df["wildcard"])
    else:
        files = base_ingestor.get_ingest_files(df["folder"], df["wildcard"])

    for gtopo_file in files:
        granule_name = "GTOPO30Elev"
        srid = 4326
        band_num = 1
        block_size = (50, 50)
        dtime = datetime(year=1979, month=1, day=1, hour=0, minute=0, second=0)
        level = 0

        ras = GDALRaster(gtopo_file, srid)
        ras.nodata_value = -9999
        bbox = proj_helper.get_bbox(srid)

        base_ingestor.ingest(ras=ras, provider_name=provider_name, variable_name=variable_name,
                             granule_name=granule_name, table_name=granule_name, srid=srid, level=level,
                             block_size=block_size, dynamic=False, start_time=dtime, end_time=datetime.max,
                             subset_bbox=bbox, overwrite=True)

        #create slope and aspect rasters
        pgdb_helper = PGDbHelper(conn_str=config.pgsql_conn_str())
        pgdb_helper.insert_slope_and_aspect_rasters(granule_name, overwrite=True)
