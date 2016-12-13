import numpy
from ci.models.raster import Raster
from ci.util.gdal_raster_conversion_helper import gdal2numpy

__author__ = 'ujjwal'


class ArrayRaster (Raster):

    def __init__(self, ds_name, data_array, size, ul, scale, skew, srid, gdal_datatype, nodata_value):
        self.data = data_array
        super(ArrayRaster, self).__init__(ds_name, size, ul, scale, skew, srid, gdal_datatype, nodata_value)

    def get_data_old(self, xoff, yoff, valid_block_size, block_size):
        pixels = numpy.copy(self.data[xoff: xoff+valid_block_size[0], yoff: yoff+valid_block_size[1]], order='C')

        if block_size[0] > valid_block_size[0] or block_size[1] > valid_block_size[1]:
            buffer = numpy.zeros((block_size[0], block_size[1]), gdal2numpy(self.gdal_datatype))
            buffer.fill(self.nodata_value)
            buffer[0:valid_block_size[0], 0: valid_block_size[1]] = pixels[:, :]
        else:
            buffer = pixels

        return buffer

    def get_data(self, xoff, yoff, valid_block_size, block_size):
        pixels = numpy.copy(self.data[xoff: xoff+valid_block_size[0], yoff: yoff+valid_block_size[1]], order='C')
        if block_size[0] > valid_block_size[0] or block_size[1] > valid_block_size[1]:
            buffer = numpy.zeros((block_size[0], block_size[1]), gdal2numpy(self.gdal_datatype))
            buffer.fill(self.nodata_value)
            #buffer[0:valid_block_size[0], 0: valid_block_size[1]] = pixels[:, :].transpose()
            buffer[0:valid_block_size[0], 0: valid_block_size[1]] = pixels[:, :]
        else:
            #buffer = pixels.transpose()
            buffer = pixels

        return buffer

    def set_data_with_xy(self, x, y, data, stat="last"):
        xbins = [self.ul[0] + j * self.scale[0] for j in range(0, self.size[0]+1)]
        ybins = [self.ul[1] + j * self.scale[1] for j in range(0, self.size[1]+1)]

        n_x = numpy.digitize(x, xbins)
        n_y = numpy.digitize(y, ybins)

        ndt = gdal2numpy(self.gdal_datatype)
        self.data = numpy.zeros((len(xbins), len(ybins)), ndt)
        self.data.fill(self.nodata_value)

        assert len(x) == len(y)
        assert len(x) <= len(data)

        if stat == "mean":
            counts = numpy.zeros((len(xbins), len(ybins)), ndt)
            counts.fill(0)

        for i in range(0, len(x)):
            bin_num_x = n_x[i] - 1
            bin_num_y = n_y[i] - 1

            if (bin_num_x > 0) and (bin_num_x < self.size[0]) and (bin_num_y > 0) and (bin_num_y < self.size[1]):
                if stat == "last":
                    self.data[bin_num_x, bin_num_y] = data[i]
                if stat == "sum":
                    self.data[bin_num_x, bin_num_y] += data[i]
                if stat == "mean":
                    self.data[bin_num_x, bin_num_y] = \
                    (data[i] + self.data[bin_num_x, bin_num_y] * counts[bin_num_x, bin_num_y]) / (counts[bin_num_x, bin_num_y] + 1)
                    counts[bin_num_x, bin_num_y] += 1
                if stat == "count" and data[i] <> self.nodata_value:
                    if self.nodata_range is None:
                        if self.data[bin_num_x, bin_num_y] == self.nodata_value:
                            self.data[bin_num_x, bin_num_y] = 1
                        else:
                            self.data[bin_num_x, bin_num_y] += 1
                    else:
                        if data[i] >= self.nodata_range[0] and data[i] < self.nodata_range[1]:
                            if self.data[bin_num_x, bin_num_y] == self.nodata_value:
                                self.data[bin_num_x, bin_num_y] = 1
                            else:
                                self.data[bin_num_x, bin_num_y] += 1
