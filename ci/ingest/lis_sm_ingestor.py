import os
from datetime import datetime, timedelta
from ci.models.spatial_reference import LIS_Spatial_Reference
from ci.util.gdal_helper import find_band_num, get_band_metadata
from ci.models.gdal_raster import GDALRaster
from ci.ingest import config, logger, proj_helper, base_ingestor

__author__ = 'ujjwal'


provider_name = "LIS"


variables = {
    "SMOIS0": {
        "GRIB_ELEMENT": "SOILM",
        "GRIB_SHORT_NAME": "0-10-DBLY"
    },
    "SMOIS1": {
        "GRIB_ELEMENT": "SOILM",
        "GRIB_SHORT_NAME": "10-40-DBLY"
    },
    "TSOIL0": {
        "GRIB_ELEMENT": "TSOIL",
        "GRIB_SHORT_NAME": "0-10-DBLY"
    }
}


def process_file(lis_file):

    srid = LIS_Spatial_Reference.epsg
    bbox = proj_helper.get_bbox(srid)

    logger.info("Ingesting file %s" % lis_file)
    for variable in variables:
        logger.info("Processing variable %s" % variable)
        band_num = find_band_num(lis_file, filterr=variables[variable])

        if band_num is None:
            #raise Exception("Could not find band for %s" % variable)
            logger.error("Could not find band for %s" % variable)
        else:
            vars = ["GRIB_REF_TIME", "GRIB_VALID_TIME"]
            datevalues = get_band_metadata(lis_file, band_num, vars)
            startdate_utc_str = (datevalues["GRIB_REF_TIME"].split())[0]
            start_date = datetime.utcfromtimestamp(float(startdate_utc_str))

            #enddate_utc_str = (datevalues["GRIB_VALID_TIME"].split())[0]
            #end_date = datetime.fromtimestamp(float(enddate_utc_str))

            end_date = start_date + timedelta(hours=1)

            block_size = (100, 100)
            ras = GDALRaster(lis_file, srid)
            ras.set_band_num(band_num)

            variable_name = variable

            level = 0
            granule_name = "%s_%s %s_%d" % (provider_name, variable, start_date.strftime("%Y%m%d %H:%M"), level)
            table_name = "%s_%s_%s_%d" % (provider_name, variable, start_date.strftime("%Y%m%d%H%M"), level)

            base_ingestor.ingest(ras=ras, provider_name=provider_name, variable_name=variable_name,
                granule_name=granule_name,table_name=granule_name, srid=srid, level=level, block_size=block_size,
                dynamic=False, start_time=start_date, end_time=end_date, subset_bbox=bbox, overwrite=True)


if __name__ == "__main__":

    from multiprocessing import Pool

    df = config.datafiles["LIS"]
    if isinstance(df["wildcard"], list):
        files = []
        for wc in df["wildcard"]:
            files += base_ingestor.get_ingest_files(df["folder"], wc)
    else:
        files = base_ingestor.get_ingest_files(df["folder"], df["wildcard"])

    fix_files = True
    if fix_files:
        # remove 8 trailing bytes to fix this error
        # ERROR: Couldn't find 'GRIB' or 'TDLP'
        # There were 8 trailing bytes in the file.
        fixed_files = []
        for file in files:
            ofilename = "fixed_%s" % os.path.basename(file)
            ofile = os.path.join(os.path.dirname(file), ofilename)

            size_bytes = os.path.getsize(file)
            with open(file, "rb") as ifh:
                with open(ofile, "wb") as ofh:
                    data = ifh.read(size_bytes-8)
                    ofh.write(data)
                    fixed_files.append(ofile)

        files = fixed_files
    else:
        files = base_ingestor.get_ingest_files(df["folder"], df["wildcard"])

    parallel = config.parallel
    if parallel:
        n_proc = config.nprocs
        pool_size = min(n_proc, len(files))
        p = Pool(pool_size)
        p.map(process_file, files)
        p.close()
        p.join()
    else:
        for file in files:
            process_file(file)
