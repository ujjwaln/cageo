__author__ = 'ujjwal'

"""
Generic ingestor for files that work well with gdal
"""

from osgeo import gdal
from osgeo import gdalconst


#older implementation, just matches "GRIB_ELEMENT"
def get_band_num(grib_file, grib_elem):
    ds = gdal.Open(grib_file, gdalconst.GA_ReadOnly)
    num_bands = ds.RasterCount
    i = 1
    while i <= num_bands:
        band = ds.GetRasterBand(i)
        meta = band.GetMetadata()
        if grib_elem == meta["GRIB_ELEMENT"]:
            return i
        i += 1


#matches dictionary
def find_band_num(grib_file, filterr={}):
    ds = gdal.Open(grib_file, gdalconst.GA_ReadOnly)
    if ds:
        num_bands = ds.RasterCount
        i = 1
        while i <= num_bands:
            band = ds.GetRasterBand(i)
            meta = band.GetMetadata()
            keys_found=0
            for key in filterr:
                if key in meta:
                    if meta[key] == filterr[key]:
                        keys_found += 1

            if keys_found == len(filterr):
                return i

            i += 1


def get_sds(hdf_file, wildcard=None):
    hdf_ds = gdal.Open(hdf_file)
    subdatasets = hdf_ds.GetSubDatasets()
    if wildcard is None:
        return subdatasets[0][0]

    for subdataset in subdatasets:
        if wildcard in subdataset[0] or wildcard in subdataset[1]:
            return subdataset[0]

    return None


def get_metadata(hdf_file, vars=[]):
    ds = gdal.Open(hdf_file)
    meta = ds.GetMetadata()
    results = {}
    for varname in vars:
        for item_key in meta:
            if str(item_key).lower() == varname.lower():
                results[varname] = meta[item_key]

        if not varname in results:
            results[varname] = None

    return results


def get_band_metadata(hdf_file, bandnum, vars=[]):
    ds = gdal.Open(hdf_file)
    band = ds.GetRasterBand(bandnum)
    meta = band.GetMetadata()
    results = {}
    for varname in vars:
        for item_key in meta:
            if str(item_key).lower() == varname.lower():
                results[varname] = meta[item_key]

        if not varname in results:
            results[varname] = None

    return results
