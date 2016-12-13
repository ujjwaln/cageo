from datetime import datetime, timedelta
from ci.models.gdal_raster import GDALRaster
from ci.models.spatial_reference import SRID_HRAP
from ci.util.nc_file_helper import nc_get_1d_vars_as_list
from ci.ingest import logger, proj_helper, config, base_ingestor


__author__ = 'ujjwal'


provider_name = "AHPS"
variable_name = "DAILYPRECIP"


def process_file(ahps_file):

    logger.info("Processing %s" % ahps_file)
    #ahps_file = get_ingest_file_path(r'AHPS_Precip_1day/nws_precip_conus_20140722.nc')
    vars = nc_get_1d_vars_as_list(ahps_file, ["timeofdata", "lat", "lon", "true_lat", "true_lon"])

    time_chars = vars["timeofdata"]
    lat = vars["lat"]
    lon = vars["lon"]
    true_lat = vars["true_lat"]
    true_lon = -1 * vars["true_lon"]

    #bottom-left, bottom-right, top-right and top-left
    bottom_left = lat[0], -1 * lon[0]
    bottom_right = lat[1], -1 * lon[1]
    top_right = lat[2], -1 * lon[2]
    top_left = lat[3], -1 * lon[3]

    bottom_left_xy = proj_helper.latlon2xy(bottom_left[0], bottom_left[1], SRID_HRAP)
    bottom_right_xy = proj_helper.latlon2xy(bottom_right[0], bottom_right[1], SRID_HRAP)
    top_left_xy = proj_helper.latlon2xy(top_left[0], top_left[1], SRID_HRAP)
    top_right_xy = proj_helper.latlon2xy(top_right[0], top_right[1], SRID_HRAP)

    time_str = "".join([ch for ch in time_chars])
    dtime = datetime.strptime(time_str, "%Y%m%d%HZ")

    logger.info("write to postgis - %s" % ahps_file)
    block_size = (50, 50)
    level = 0

    ras = GDALRaster(ahps_file, SRID_HRAP)
    ras.set_band_num(1)
    ras.nodata_value = -1
    ras.nodata_range = (-1, 1)

    scale_x1 = (top_right_xy[0] - top_left_xy[0]) / ras.size[0]
    scale_x2 = (bottom_right_xy[0] - bottom_left_xy[0]) / ras.size[0]
    scale_y1 = (bottom_right_xy[1] - top_right_xy[1]) / ras.size[1]
    scale_y2 = (bottom_left_xy[1] - top_left_xy[1]) / ras.size[1]

    scale_x = scale_x1
    scale_y = scale_y1
    skew_y = 0
    skew_x = 0
    ul_x = top_left_xy[0]
    ul_y = top_left_xy[1]

    #explicitly set project params since netcdf file does not have it
    ras.scale = (scale_x, scale_y)
    ras.ul = (ul_x, ul_y)
    ras.skew = (skew_x, skew_y)
    ras.geo_bounds = [ras.ul[0], ras.ul[0] + ras.size[0] * ras.scale[0],
                    ras.ul[1], ras.ul[1] + ras.size[1] * ras.scale[1]]

    granule_name = "%s_%s %s_%d" % (provider_name, variable_name, dtime.strftime("%Y%m%d %H:%M"), level)
    table_name = "%s_%s_%s_%d" % (provider_name, variable_name, dtime.strftime("%Y%m%d%H%M"), level)

    bbox = proj_helper.get_bbox(SRID_HRAP)
    #bbox = None

    start_time = dtime
    end_time = dtime + timedelta(days=1)

    base_ingestor.ingest(ras=ras, provider_name=provider_name, variable_name=variable_name, granule_name=granule_name,
       table_name=granule_name, srid=SRID_HRAP, level=level, block_size=block_size, dynamic=False,
       subset_bbox=bbox, start_time=start_time, end_time=end_time, overwrite=True)


if __name__ == "__main__":

    df = config.datafiles["AHPS_DAILYPRECIP"]

    if isinstance(df["wildcard"], list):
        files = []
        for wc in df["wildcard"]:
            files += base_ingestor.get_ingest_files(df["folder"], wc)
    else:
        files = base_ingestor.get_ingest_files(df["folder"], df["wildcard"])

    for ahps_file in files:
        process_file(ahps_file)
