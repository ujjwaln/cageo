import binascii
import numpy
import math
from osgeo import ogr
import ci.util.gdal_raster_conversion_helper as conversion_helper


__author__ = 'ujjwal'


class Raster(object):
    """
        Represents a raster object that can be instantiated
        from memory and written to postgis
    """
    def __init__(self, dsname, size, ul, scale, skew, srid, gdal_datatype, nodata_value, nodata_range=None,
                 bottom_up=False):

        #if pixel 0,0 is at ul, bottom_up = false. default
        self.dsname = dsname
        self.bottom_up = bottom_up
        self.size = size
        self.ul = ul
        self.scale = scale
        self.skew = skew
        self.srid = srid
        self.origin = (0, 0)
        self.gdal_datatype = gdal_datatype
        self.nodata_value = nodata_value

        #sometimes, as in MRMS data, it is required to set a range of values to nodata
        self.nodata_range = nodata_range
        self.reclassifier = {}
        self.reclassifier_callback = None
        self.geo_bounds = [self.ul[0], self.ul[0] + self.size[0] * self.scale[0],
                           self.ul[1], self.ul[1] + self.size[1] * self.scale[1]]

    def xy2rc(self, x, y):
        c = int((x - self.ul[0]) / self.scale[0]) + self.origin[0]
        r = int((y - self.ul[1]) / self.scale[1]) + self.origin[1]
        if self.bottom_up:
            r = self.size[1] - r

        c = min(max(c, 0), self.size[0] + self.origin[0])
        r = min(max(r, 0), self.size[1] + self.origin[1])
        return r, c

    def rc2xy(self, c, r):
        x = self.ul[0] + c * self.scale[0]
        if self.bottom_up:
            y = self.ul[1] + (self.size[1] - r) * self.scale[1]
        else:
            y = self.ul[1] + r * self.scale[1]

        return x, y

    def plot(self):
        import matplotlib.pyplot as plt
        pixels = self.get_data(0, 0, self.size[0], self.size[1])

        fig = plt.figure()
        ax = fig.add_subplot(1, 1, 1)
        ax.set_title('colormap')

        plt.imshow(pixels)
        ax.set_aspect('equal')

        plt.colorbar(orientation='vertical')
        plt.show()

    def subset(self, region_bounds):
        lons = region_bounds[0], region_bounds[1]
        lats = region_bounds[2], region_bounds[3]

        lon_min = min(lons)
        lon_max = max(lons)
        lat_min = min(lats)
        lat_max = max(lats)

        ll_row, ll_col = self.xy2rc(lon_min, lat_min)
        ur_row, ur_col = self.xy2rc(lon_max, lat_max)

        if ll_row == ur_row or ll_col == ur_col:
            raise Exception("Non Intersection in Raster subset")

        self.size = abs(ur_col - ll_col), abs(ur_row - ll_row)
        self.ul = max(lon_min, self.ul[0]), min(lat_max, self.ul[1])

        if self.bottom_up:
            self.origin = ll_col, ll_row
        else:
            self.origin = ll_col, ur_row

        self.geo_bounds = [self.ul[0], self.ul[0] + self.size[0] * self.scale[0],
                           self.ul[1], self.ul[1] + self.size[1] * self.scale[1]]

    def get_data(self, xoff, yoff, valid_block_size, block_size):
        raise NotImplementedError

    def tile_generator(self, block_size):
        if self.nodata_value is None:
            self.nodata_value = -999

        if self.gdal_datatype is None:
            raise Exception("datatype not set")

        raster_band_header_wkb = conversion_helper.get_raster_band_header_wkb(self.nodata_value, self.gdal_datatype)
        block_size = min(block_size[0], self.size[0]), min(block_size[1], self.size[1])

        num_tiles_x = math.ceil(self.size[0] * 1.0 / block_size[0])
        num_tiles_y = math.ceil(self.size[1] * 1.0 / block_size[1])

        tile_y_num = 0
        while tile_y_num < num_tiles_y:
            tile_x_num = 0
            while tile_x_num < num_tiles_x:

                xoff = self.origin[0] + tile_x_num * block_size[0]
                yoff = self.origin[1] + tile_y_num * block_size[1]

                valid_block_size = (min(block_size[0], self.size[0] - (xoff - self.origin[0])),
                                    min(block_size[1], self.size[1] - (yoff - self.origin[1])))
                if self.bottom_up:
                    tile_ul = self.rc2xy(tile_x_num * block_size[0], tile_y_num * block_size[1] + valid_block_size[1])
                    tile_br = self.rc2xy(tile_x_num * block_size[0] + block_size[0],
                                         tile_y_num * block_size[1])
                else:
                    tile_ul = self.rc2xy(tile_x_num * block_size[0], tile_y_num * block_size[1])
                    tile_br = self.rc2xy(tile_x_num * block_size[0] + valid_block_size[0],
                                         tile_y_num * block_size[1] + valid_block_size[1])

                # set raster info wkb
                raster_info_wkb = conversion_helper.get_raster_info_wkb(tile_ul, self.scale, self.skew,
                            self.srid, valid_block_size[0], valid_block_size[1], num_bands=1, endian=1, version=0)

                # block_data = self.get_data(xoff, yoff, valid_block_size, valid_block_size)
                block_data = self.get_data(xoff, yoff, valid_block_size, valid_block_size)

                # plot_arr(block_data)
                max_val = numpy.asscalar(block_data.max())
                min_val = numpy.asscalar(block_data.min())

                if not self.nodata_range is None:
                    if self.nodata_range[1] > self.nodata_range[0]:
                        for val in numpy.nditer(block_data, op_flags=['readwrite']):
                            scalar_value = numpy.asscalar(val)
                            if self.nodata_range[0] < scalar_value < self.nodata_range[1]:
                                val[...] = self.nodata_value
                            if len(self.reclassifier) and (scalar_value in self.reclassifier):
                                val[...] = self.reclassifier[scalar_value]
                            if self.reclassifier_callback is not None:
                                val[...] = self.reclassifier_callback(val[...])

                raster_band_data_wkb = binascii.hexlify(block_data)
                raster_band_wkb = raster_info_wkb + raster_band_header_wkb + raster_band_data_wkb

                if self.bottom_up:
                    x_min = tile_ul[0]
                    x_max = tile_br[0]
                    y_min = tile_ul[1]
                    y_max = tile_br[1]
                else:
                    x_min = tile_ul[0]
                    x_max = tile_br[0]
                    y_min = tile_br[1]
                    y_max = tile_ul[1]

                extent = "SRID=%d;POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))" % (
                    self.srid, x_min, y_min, x_max, y_min, x_max, y_max, x_min, y_max, x_min, y_min
                )

                yield {
                    "wkb": raster_band_wkb,
                    "min": min_val,
                    "max": max_val,
                    "extent": extent
                }

                tile_x_num += 1
            tile_y_num += 1

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
                if self.bottom_up:
                    tile_ul = self.rc2xy(tile_x_num * block_size[0], tile_y_num * block_size[1] + block_size[1])
                    tile_br = self.rc2xy(tile_x_num * block_size[0] + block_size[0], tile_y_num * block_size[1])
                else:
                    tile_ul = self.rc2xy(tile_x_num * block_size[0], tile_y_num * block_size[1])
                    tile_br = self.rc2xy(tile_x_num * block_size[0] + block_size[0],
                                         tile_y_num * block_size[1] + block_size[1])

                #tile_ul = self.rc2xy(tile_x_num * block_size[0], tile_y_num * block_size[1])
                xoff = self.origin[0] + tile_x_num * block_size[0]
                yoff = self.origin[1] + tile_y_num * block_size[1]

                valid_block_size = (min(block_size[0], self.size[0] - (xoff - self.origin[0])),
                                    min(block_size[1], self.size[1] - (yoff - self.origin[1])))

                buffer = self.get_data(xoff, yoff, valid_block_size, block_size)
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

            geo_bounds = {}, {}, {}, {}
        """.format(self.ul[0], self.ul[1],
            self.size[0], self.size[1],
            self.scale[0], self.scale[1],
            self.skew[0], self.skew[1],

            self.geo_bounds[0], self.geo_bounds[1], self.geo_bounds[2], self.geo_bounds[3])

    def wkt_extent(self):
        return "SRID=%d;POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))" % (self.srid, self.geo_bounds[0], self.geo_bounds[2],
            self.geo_bounds[1], self.geo_bounds[2], self.geo_bounds[1], self.geo_bounds[3],
            self.geo_bounds[0], self.geo_bounds[3], self.geo_bounds[0], self.geo_bounds[2])
