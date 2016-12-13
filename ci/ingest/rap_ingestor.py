from datetime import datetime, timedelta
from ci.models.spatial_reference import RAP_Spatial_Reference
from ci.util.gdal_helper import find_band_num, get_band_metadata
from ci.models.gdal_raster import GDALRaster
from ci.ingest import config, logger, proj_helper, base_ingestor

__author__ = 'ujjwal'

variables = {

    "CAPE0": {
        "GRIB_ELEMENT": "CAPE",
        "GRIB_SHORT_NAME": "0-SFC"
    },

    "CAPE1": {
        "GRIB_ELEMENT": "CAPE",
        "GRIB_SHORT_NAME": "18000-0-SPDL"
    },

    "CAPE2": {
        "GRIB_ELEMENT": "CAPE",
        "GRIB_SHORT_NAME": "25500-0-SPDL"
    },

    "CAPE3": {
        "GRIB_ELEMENT": "CAPE",
        "GRIB_SHORT_NAME": "9000-0-SPDL"
    },

    "CIN0": {
        "GRIB_ELEMENT": "CIN",
        "GRIB_SHORT_NAME": "0-SFC"
    },

    "CIN1": {
        "GRIB_ELEMENT": "CIN",
        "GRIB_SHORT_NAME": "18000-0-SPDL"
    },

    "CIN2": {
        "GRIB_ELEMENT": "CIN",
        "GRIB_SHORT_NAME": "25500-0-SPDL"
    },

    "CIN3": {
        "GRIB_ELEMENT": "CIN",
        "GRIB_SHORT_NAME": "9000-0-SPDL"
    },

    "HTFL_HGT": {
        "GRIB_ELEMENT": "HGT",
        "GRIB_SHORT_NAME": "0-HTFL"
    },

    "HTFL_RH": {
        "GRIB_ELEMENT": "RH",
        "GRIB_SHORT_NAME": "0-HTFL"
    },

    "UGRD": {
        "GRIB_ELEMENT": "UGRD",
        "GRIB_SHORT_NAME": "10-HTGL"
    },

    "VGRD": {
        "GRIB_ELEMENT": "VGRD",
        "GRIB_SHORT_NAME": "10-HTGL"
    },

    "DEWPOINT": {
        "GRIB_ELEMENT": "DPT",
        "GRIB_SHORT_NAME": "2-HTGL"
    },

    "RAP_REFL": {
        "GRIB_ELEMENT": "REFC",
        "GRIB_SHORT_NAME": "0-EATM"
    }
}


def process_file(rap_file):
    provider_name = "RAP"
    srid = RAP_Spatial_Reference.epsg

    logger.info("Ingesting file %s" % rap_file)
    for variable in variables:
        logger.info("Processing variable %s" % variable)
        band_num = find_band_num(rap_file, filterr=variables[variable])

        if band_num is None:
            #raise Exception("Could not find band for %s" % variable)
            logger.error("Could not find band for %s" % variable)
        else:
            vars = ["GRIB_REF_TIME", "GRIB_VALID_TIME"]
            datevalues = get_band_metadata(rap_file, band_num, vars)
            startdate_utc_str = (datevalues["GRIB_REF_TIME"].split())[0]
            enddate_utc_str = (datevalues["GRIB_VALID_TIME"].split())[0]

            start_date = datetime.utcfromtimestamp(float(startdate_utc_str))
            #end_date = datetime.fromtimestamp(float(enddate_utc_str))
            end_date = start_date + timedelta(hours=1)

            block_size = (10, 10)
            ras = GDALRaster(rap_file, srid)
            ras.set_band_num(band_num)
            if variable == "RAP_REFL":
                ras.nodata_range = [-999, -9]

            level = int((variables[variable]["GRIB_SHORT_NAME"].split("-"))[0])
            granule_name = "%s_%s %s_%d" % (provider_name, variable, start_date.strftime("%Y%m%d %H:%M"), level)
            table_name = "%s_%s_%s_%d" % (provider_name, variable, start_date.strftime("%Y%m%d%H%M"), level)
            bbox = proj_helper.get_bbox(srid)
            base_ingestor.ingest(ras=ras, provider_name=provider_name, variable_name=variable, granule_name=granule_name,
                   table_name=granule_name, srid=srid, level=level, block_size=block_size, dynamic=False,
                   start_time=start_date, end_time=end_date, subset_bbox=bbox, overwrite=True, threshold=None)

if __name__ == "__main__":
    from multiprocessing import Pool

    df = config.datafiles["RAP"]
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

        p = Pool(pool_size)
        p.map(process_file, files)

        p.close()
        p.join()
    else:
        for file in files:
            process_file(file)
