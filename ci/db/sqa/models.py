__author__ = 'ujjwal'


class DataFormat(object):
    def __init__(self, name):
        self.name = name


class Variable(object):
    def __init__(self, name, unit, description):
        self.name = name
        self.unit = unit
        self.description = description


class Provider(object):
    def __init__(self, name):
        self.name = name


class DataGranule(object):
    def __init__(self, provider, variable, starttime, endtime, level, extent, name, srid, table_name, file_name):
        self.provider = provider
        self.variable = variable
        self.starttime = starttime
        self.endtime = endtime
        self.level = level
        self.extent = extent
        self.name = name
        self.srid = srid
        self.table_name = table_name
        self.file_name = file_name


class RasterTile(object):
    def __init__(self, rast, datagranule):
        self.rast = rast
        self.datagranule = datagranule


class RoiGeom(object):
    def __init__(self, roi_name, mrms_granule_id, starttime, endtime, geom, center,
                 stormy_poly, center_lat, center_lon, iarea, type):
        self.roi_name = roi_name
        self.mrms_granule_id = mrms_granule_id
        self.starttime = starttime
        self.endtime = endtime
        self.geom = geom
        self.center = center
        self.storm_poly = stormy_poly
        self.center_lat = center_lat
        self.center_lon = center_lon
        self.iarea = iarea
        self.type = type


# class RoiStat(object):
#     def __init__(self, roi_id, roi_name, granule_id, count, ssum, mean, stddev, mmin, mmax, variable_id):
#         self.roi_id = roi_id
#         self.roi_name = roi_name
#         self.granule_id = granule_id
#         self.count = count
#         self.sum = ssum
#         self.mean = mean
#         self.stddev = stddev
#         self.min = mmin
#         self.max = mmax
#         self.variable_id = variable_id
#

class RoiStat(object):
    def __init__(self, roi_id, roi_name, count, ssum, mean, stddev, mmin, mmax, variable_id):
        self.roi_id = roi_id
        self.roi_name = roi_name
        self.count = count
        self.sum = ssum
        self.mean = mean
        self.stddev = stddev
        self.min = mmin
        self.max = mmax
        self.variable_id = variable_id


class Mask(object):
    def __init__(self, name, geom):
        self.name = name
        self.geom = geom


class RoiTrack(object):
    def __init__(self, name, geom, dtimes, track):
        self.name = name
        self.geom = geom
        self.dtimes = dtimes
        self.track = track