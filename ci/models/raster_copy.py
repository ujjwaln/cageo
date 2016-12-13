import binascii
import numpy
from osgeo import ogr
import ci.utils.gdal_raster_conversion_helper as conversion_helper

__author__ = 'ujjwal'


class Raster(object):
    """
        Represents a raster object that can be instantiated
        from memory and written to postgis
    """
    def __init__(self, size, ul, scale, skew, srid, gdal_datatype, nodata_value, nodata_range=None, bottom_up=False):
        #if pixel 0,0 is at ul, bottom_up = false. default
        self.bottom_up = bottom_up
        self.size = size
        self.ul = ul
        self.scale = scale
        self.skew = skew
        self.srid = srid
        self.origin = (0, 0)

        #i_start,i_end, j_start, j_end
        self.pixel_bounds = [0, self.size[0], 0, self.size[1]]

        #x_start, x_end, y_start, y_end
        self.geo_bounds = [self.ul[0], self.ul[0] + self.size[0] * self.scale[0],
                           self.ul[1], self.ul[1] + self.size[1] * self.scale[1]]

        self.gdal_datatype = gdal_datatype
        self.nodata_value = nodata_value

        #sometimes, as in MRMS data, it is required to set a range of values to nodata
        self.nodata_range = nodata_range

    def xy2rc(self, x, y):
        c = int((x - self.ul[0]) / self.scale[0])
        r = int((y - self.ul[1]) / self.scale[1])
        if self.bottom_up:
            r = self.size[1] - r
        return r, c

    def rc2xy(self, r, c):
        x = self.ul[0] + c * self.scale[0]
        y = self.ul[1] - r * self.scale[1]
        return x, y

    def subset(self, region_bounds):
        lons = region_bounds[0], region_bounds[1]
        lats = region_bounds[2], region_bounds[3]

        lon_min = min(lons)
        lon_max = max(lons)
        lat_min = min(lats)
        lat_max = max(lats)

        lon_min = max(min(self.geo_bounds[0], self.geo_bounds[1]), lon_min)
        lon_max = min(max(self.geo_bounds[0], self.geo_bounds[1]), lon_max)
        lat_min = max(min(self.geo_bounds[2], self.geo_bounds[3]), lat_min)
        lat_max = min(max(self.geo_bounds[2], self.geo_bounds[3]), lat_max)

        ll_row, ll_col = self.xy2rc(lon_min, lat_min)
        ur_row, ur_col = self.xy2rc(lon_max, lat_max)

        if self.bottom_up:
            self.size = ur_col - ll_col, ur_row - ll_row
            self.ul = lon_min, lat_max
            self.origin = ll_col, ll_row
        else:
            self.size = ur_col - ll_col, ll_row - ur_row
            self.ul = lon_min, lat_max
            self.origin = ll_col, ur_row

        # if self.bottom_up:
        #     self.pixel_bounds = (ll_col, ur_col, ll_row, ur_row)
        #     self.geo_bounds = (lon_min, lon_max, lat_min, lat_max)
        # else:
        #     self.pixel_bounds = (ll_col, ur_col, ur_row, ll_row)
        #     self.geo_bounds = (lon_min, lon_max, lat_max, lat_min)

        #
        # col_min = int((lon_min - self.ul[0]) / self.scale[0])
        # col_max = int((lon_max - self.ul[0]) / self.scale[0])
        #
        # if self.bottom_up:
        #     row_min = self.size[1] - int((lat_max - self.ul[1]) / self.scale[1])
        #     row_max = self.size[1] - int((lat_min - self.ul[1]) / self.scale[1])
        #     self.pixel_bounds = (col_min, col_max, row_min, row_max)
        #     self.geo_bounds = (lon_min, lon_max, lat_min, lat_max)
        # else:
        #     row_min = int((lat_max - self.ul[1]) / self.scale[1])
        #     row_max = int((lat_min - self.ul[1]) / self.scale[1])
        #     self.pixel_bounds = (col_min, col_max, row_min, row_max)
        #     self.geo_bounds = (lon_min, lon_max, lat_max, lat_min)

    def get_data(self, xoff, yoff, valid_block_size, block_size):
        raise NotImplementedError

    def subset1(self, region_bounds):
        pix_bounds = (
            int(max((region_bounds[0] - self.geo_bounds[0]) / self.scale[0], 0)),
            int(max(min((region_bounds[1] - self.geo_bounds[0]) / self.scale[0], self.size[0]), 0)),
            int(max((region_bounds[2] - self.geo_bounds[2]) / self.scale[1], 0)),
            int(max(min((region_bounds[3] - self.geo_bounds[2]) / self.scale[1], self.size[1]), 0))
        )
        self.pixel_bounds = pix_bounds

        geo_bounds = (
            max(region_bounds[0], self.geo_bounds[0]),
            min(region_bounds[1], self.geo_bounds[1]),
            min(region_bounds[2], self.geo_bounds[2]),
            max(region_bounds[3], self.geo_bounds[3])
        )
        self.geo_bounds = geo_bounds

    def tile_generator(self, block_size):
        if self.nodata_value is None:
            self.nodata_value = -999
            #raise Exception("Nodatavalue not set")

        if self.gdal_datatype is None:
            raise Exception("datatype not set")

        raster_band_header_wkb = conversion_helper.get_raster_band_header_wkb(self.nodata_value, self.gdal_datatype)
        tile_x_num = 0

        block_size = min(block_size[0], self.size[0]), min(block_size[1], self.size[1])
        while tile_x_num < int(self.size[0] / block_size[0]):
            tile_y_num = 0
            while tile_y_num < int(self.size[1] / block_size[1]):

                tile_ul = self.xy2rc(self.origin[0] + tile_x_num, self.origin[1] + tile_y_num)
                # tile_ul = (self.geo_bounds[0] + tile_x_num * block_size[0] * self.scale[0],
                #             self.geo_bounds[2] + tile_y_num * block_size[1] * self.scale[1])

                xoff = self.origin[0] + tile_x_num * block_size[0]
                yoff = self.origin[1] + tile_y_num * block_size[1]

                valid_block_size = (min(block_size[0], self.size[0] - xoff),
                                    min(block_size[1], self.size[1] - yoff))

                #set raster info wkb
                raster_info_wkb = conversion_helper.get_raster_info_wkb(tile_ul, self.scale, self.skew,
                                    self.srid, block_size[0], block_size[1], num_bands=1, endian=1, version=0)

                block_data = self.get_data(xoff, yoff, valid_block_size, block_size)
                if not self.nodata_range is None:
                    if self.nodata_range[1] > self.nodata_range[0]:
                        for val in numpy.nditer(block_data, op_flags=['readwrite']):
                            if self.nodata_range[0] < val < self.nodata_range[1]:
                                val[...] = self.nodata_value

                raster_band_data_wkb = binascii.hexlify(block_data)
                raster_band_wkb = raster_info_wkb + raster_band_header_wkb + raster_band_data_wkb
                yield raster_band_wkb

                tile_y_num += 1
            tile_x_num += 1

    def tile_generator1(self, block_size):
        if self.nodata_value is None:
            self.nodata_value = -999
            #raise Exception("Nodatavalue not set")

        if self.gdal_datatype is None:
            raise Exception("datatype not set")

        raster_band_header_wkb = conversion_helper.get_raster_band_header_wkb(self.nodata_value, self.gdal_datatype)
        tile_x_num = 0

        size = self.pixel_bounds[1] - self.pixel_bounds[0], self.pixel_bounds[3] - self.pixel_bounds[2]
        block_size = min(block_size[0], size[0]), min(block_size[1], size[1])

        while tile_x_num < int(size[0] / block_size[0]):
            tile_y_num = 0
            while tile_y_num < int(size[1] / block_size[1]):
                tile_ul = (self.geo_bounds[0] + tile_x_num * block_size[0] * self.scale[0],
                            self.geo_bounds[2] + tile_y_num * block_size[1] * self.scale[1])

                xoff = self.origin[0] + self.pixel_bounds[0] + tile_x_num * block_size[0]
                yoff = self.origin[1] + self.pixel_bounds[2] + tile_y_num * block_size[1]

                valid_block_size = (min(block_size[0], self.size[0] - xoff),
                                    min(block_size[1], self.size[1] - yoff))

                #set raster info wkb
                raster_info_wkb = conversion_helper.get_raster_info_wkb(tile_ul, self.scale, self.skew,
                                    self.srid, block_size[0], block_size[1], num_bands=1, endian=1, version=0)

                block_data = self.get_data(xoff, yoff, valid_block_size, block_size)
                if not self.nodata_range is None:
                    if self.nodata_range[1] > self.nodata_range[0]:
                        for val in numpy.nditer(block_data, op_flags=['readwrite']):
                            if self.nodata_range[0] < val < self.nodata_range[1]:
                                val[...] = self.nodata_value

                raster_band_data_wkb = binascii.hexlify(block_data)
                raster_band_wkb = raster_info_wkb + raster_band_header_wkb + raster_band_data_wkb
                yield raster_band_wkb

                tile_y_num += 1
            tile_x_num += 1

    def vector_generator(self, block_size):
        if self.nodata_value is None:
            #raise Exception("Nodatavalue not set")
            self.nodata_value = -999

        if self.gdal_datatype is None:
            raise Exception("Nodatavalue not set")

        tile_x_num = 0
        while tile_x_num <= int(self.size[0] / block_size[0]):
            tile_y_num = 0
            while tile_y_num <= int(self.size[1] / block_size[1]):
                shapes = []
                xoff = self.pixel_bounds[0] + tile_x_num * block_size[0]
                yoff = self.pixel_bounds[2] + tile_y_num * block_size[1]

                valid_block_size = (min(block_size[0], self.size[0] - xoff),
                                    min(block_size[1], self.size[1] - yoff))

                buffer = self.get_data(xoff, yoff, valid_block_size, block_size)
                #buffer will be of shape block_size[1], block_size[0]
                for row in range(0, valid_block_size[1]):
                    for col in range(0, valid_block_size[0]):
                        x = self.ul[0] + (xoff + col + 0.5) * self.scale[0]
                        y = self.ul[1] + (yoff + row + 0.5) * self.scale[1]
                        value = numpy.asscalar(buffer[row, col])
                        if value <> self.nodata_value:
                            point_geom = ogr.Geometry(ogr.wkbPoint)
                            point_geom.AddPoint(x, y)
                            shapes.append((point_geom, value))

                yield shapes

                tile_y_num += 1
            tile_x_num += 1

    def __str__(self):
        return """
            ul = {}, {}
            size = {}, {}
            scale = {}, {}
            skew = {}, {}
            pixel_bounds = {}, {}, {}, {}
            geo_bounds = {}, {}, {}, {}
        """.format(self.ul[0], self.ul[1],
            self.size[0], self.size[1],
            self.scale[0], self.scale[1],
            self.skew[0], self.skew[1],
            self.pixel_bounds[0], self.pixel_bounds[1], self.pixel_bounds[2], self.pixel_bounds[3],
            self.geo_bounds[0], self.geo_bounds[1], self.geo_bounds[2], self.geo_bounds[3])

    def wkt_extent(self):
        return "POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))" % (self.geo_bounds[0], self.geo_bounds[2],
            self.geo_bounds[1], self.geo_bounds[2], self.geo_bounds[1], self.geo_bounds[3],
            self.geo_bounds[0], self.geo_bounds[3], self.geo_bounds[0], self.geo_bounds[2])
