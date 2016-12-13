import mmap
import contextlib
import struct, os, gzip
from datetime import datetime, timedelta
import osgeo.gdalconst as gdalc
from ci.models.array_raster import ArrayRaster
from ci.models.spatial_reference import ALBERS_Spatial_Reference
from ci.ingest import logger, proj_helper, config, base_ingestor

__author__ = 'ujjwal'


provider_name = "GOES"
variable_name = "CLOUDTYPE"

CHUNK_SIZE = 1024 * 100
all_x = None
all_y = None
array_raster = None
indexes = None


def parse_datetime_from_filename(fname):
    datadir = os.path.join(config.datadir, "GOES")
    str1 = fname.replace(datadir, "")
    parts = str1.split(os.sep)
    strdatetime = parts[2].replace("_Archive_Time_Data", "")
    parts1 = strdatetime.split("_")

    year = int(parts1[0])
    month = int(parts1[1][0:2])
    day = int(parts1[1][2:4])
    hour = int(parts1[2][0:2])
    min = int(parts1[2][2:4])

    return datetime(year=year, month=month, day=day, hour=hour, minute=min)


def bin_reader(fname, typechar='f', recycle=False, chunk_size=1024):
    word_size = struct.calcsize(typechar)
    with open(fname, "rb") as f:
        with contextlib.closing(mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)) as m:
            buf = m.read(word_size * chunk_size)
            while True:
                data = []
                num_bytes = len(buf)/word_size
                for offset in range(0, num_bytes):
                    val = struct.unpack_from(typechar, buf, offset * word_size)
                    data.append(val[0])

                yield data
                if num_bytes < chunk_size:
                    if recycle:
                        f.seek(0)
                    else:
                        break
                else:
                    buf = m.read(word_size * chunk_size)


def process_file(tf):

    fname = tf["file"]
    prev_time = tf["nt"]
    dtime = tf["dt"]

    logger.info("Processing file %s " % fname)
    ext_parts = os.path.splitext(fname)
    ext = ext_parts[1]
    remove_after_process = False

    if ext == ".gz":
        nc_file_name = ext_parts[0]
        nc_file_copy = os.path.join(os.path.dirname(fname), nc_file_name)
        with open(nc_file_copy, 'wb') as nc_file:
            gz_file = gzip.open(fname, 'rb')
            gz_bytes = gz_file.read()

            nc_file.write(gz_bytes)
            gz_file.close()

            data_file = nc_file_copy
            remove_after_process = True
    else:
        data_file = fname

    provider_name = "GOES"
    #variable_name = "CLOUDTYPE"
    cloud_mask = bin_reader(data_file, typechar='f', chunk_size=CHUNK_SIZE, recycle=False)
    cum_vals = []
    tcum_vals = []
    num_chunk = 0

    while True:
        try:
            val_chunk = cloud_mask.next()
            for k in range(0, len(val_chunk), 1):
                if (num_chunk * CHUNK_SIZE + k) in indexes:
                    #special masking for goes cm data, only include type 2 & 4
                    if val_chunk[k] == 2: #cumulus clouds
                        cum_vals.append(1)
                    else:
                        cum_vals.append(0)

                    if val_chunk[k] == 4: #towering cumulus clouds
                        tcum_vals.append(1)
                    else:
                        tcum_vals.append(0)

            num_chunk += 1
        except StopIteration:
            break

    level = 0
    block_size = 50, 50 #array_raster.size # 100, 100
    variable_name = "CUM_CLOUD"
    granule_name = "%s_%s_%s" % (provider_name, variable_name, dtime.strftime("%Y%d%m%H%M"))
    array_raster.set_data_with_xy(x=all_x, y=all_y, data=cum_vals)
    array_raster.dsname = granule_name
    base_ingestor.ingest(ras=array_raster, provider_name=provider_name, variable_name=variable_name,
        granule_name=granule_name, table_name=granule_name, srid=ALBERS_Spatial_Reference.epsg, level=level,
        block_size=block_size, dynamic=False, start_time=prev_time, end_time=dtime, subset_bbox=bbox, overwrite=True)

    variable_name = "TCUM_CLOUD"
    granule_name = "%s_%s_%s" % (provider_name, variable_name, dtime.strftime("%Y%d%m%H%M"))
    array_raster.set_data_with_xy(x=all_x, y=all_y, data=tcum_vals)
    array_raster.dsname = granule_name
    base_ingestor.ingest(ras=array_raster, provider_name=provider_name, variable_name=variable_name,
        granule_name=granule_name, table_name=granule_name, srid=ALBERS_Spatial_Reference.epsg, level=level,
        block_size=block_size, dynamic=False, start_time=prev_time, end_time=dtime, subset_bbox=bbox, overwrite=True)

    if remove_after_process:
        os.remove(data_file)


if __name__ == "__main__":

    df = config.datafiles["GOES_CLOUDTYPE"]
    if isinstance(df["wildcard"], list):
        files = []
        for wc in df["wildcard"]:
            files += base_ingestor.get_ingest_files(df["folder"], wc)
    else:
        files = base_ingestor.get_ingest_files(df["folder"], df["wildcard"])

    if not len(files):
        print "no files"
        exit()

    time_fname_map = []
    for f in files:
        dt = parse_datetime_from_filename(f)
        time_fname_map.append({
            "dt": dt,
            "file": f
        })

    lat_file = base_ingestor.get_ingest_file_path(df["folder"] + r'/SATCAST_Original_Latitude_6480_x_2200.dat')
    lon_file = base_ingestor.get_ingest_file_path(df["folder"] + r'/SATCAST_Original_Longitude_6480_x_2200.dat')

    lats = bin_reader(fname=lat_file, typechar='f', recycle=False, chunk_size=CHUNK_SIZE)
    lons = bin_reader(fname=lon_file, typechar='f', recycle=False, chunk_size=CHUNK_SIZE)

    _bbox = proj_helper.get_bbox(srid=4326)
    all_lats = []
    all_lons = []
    indexes = []
    num_chunk = 0

    while True:
        try:
            lon_chunk = lons.next()
            lat_chunk = lats.next()

            for c in range(0, len(lon_chunk), 1):
                ln = lon_chunk[c]
                lt = lat_chunk[c]

                if (lt > _bbox[3]) and (lt < _bbox[2]) and (ln > _bbox[0]) and (ln < _bbox[1]):
                    all_lons.append(ln)
                    all_lats.append(lt)
                    indexes.append(c + num_chunk * CHUNK_SIZE)

            num_chunk += 1
        except StopIteration:
            break

    indexes = set(indexes) #makes check for item presence faster

    all_x, all_y = proj_helper.latlon2xy1(all_lats, all_lons, ALBERS_Spatial_Reference.proj4)
    bbox = min(all_x), max(all_x), max(all_y), min(all_y)
    scale = (2000, -2000)
    ul = (bbox[0] - scale[0] * 0.5, bbox[2] + scale[0] * 0.5)
    nodata = 0
    size = int((bbox[1]-bbox[0]) / scale[0]), int((bbox[3]-bbox[2]) / scale[1])

    array_raster = ArrayRaster(ds_name="", data_array=None, size=size, ul=ul, scale=scale, skew=(0, 0),
                               srid=ALBERS_Spatial_Reference.epsg, gdal_datatype=gdalc.GDT_Float32,
                               nodata_value=nodata)

    sorted_time_fname_map = sorted(time_fname_map, key=lambda e: e["dt"])
    prev_time = sorted_time_fname_map[0]["dt"] - timedelta(minutes=15)

    for tf in sorted_time_fname_map:
        tf["nt"] = prev_time
        prev_time = tf["dt"]

    from multiprocessing import Pool

    parallel = config.parallel
    if parallel:
        n_proc = config.nprocs
        p = Pool(n_proc)
        p.map(process_file, sorted_time_fname_map)
        p.close()
        p.join()
    else:
        for tf in sorted_time_fname_map:
            process_file(tf)
