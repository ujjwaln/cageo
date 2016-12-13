__author__ = 'ujjwal'

import binascii
import struct
import numpy
import osgeo.gdalconst as gdalc


def numpy2gdal(ndt):
    pixtypes = {
        numpy.uint8: gdalc.GDT_Byte,
        numpy.int16: gdalc.GDT_Int16,
        numpy.uint16: gdalc.GDT_UInt16,
        numpy.int32: gdalc.GDT_Int32,
        numpy.uint32: gdalc.GDT_UInt32,
        numpy.float32: gdalc.GDT_Float32,
        numpy.float64: gdalc.GDT_Float64
    }

    return pixtypes.get(ndt, None)


def gdal2numpy(gdt):
    pixtypes = {
        gdalc.GDT_Byte    : numpy.uint8,
        gdalc.GDT_Int16   : numpy.int16,
        gdalc.GDT_UInt16  : numpy.uint16,
        gdalc.GDT_Int32   : numpy.int32,
        gdalc.GDT_UInt32  : numpy.uint32,
        gdalc.GDT_Float32 : numpy.float32,
        gdalc.GDT_Float64 : numpy.float64
    }

    return pixtypes.get(gdt, None)


def gdal2pt(gdt):
    """Translate GDAL data type to WKT Raster pixel type."""
    pixtypes = {
        gdalc.GDT_Byte    : { 'name': 'PT_8BUI',  'id':  4 },
        gdalc.GDT_Int16   : { 'name': 'PT_16BSI', 'id':  5 },
        gdalc.GDT_UInt16  : { 'name': 'PT_16BUI', 'id':  6 },
        gdalc.GDT_Int32   : { 'name': 'PT_32BSI', 'id':  7 },
        gdalc.GDT_UInt32  : { 'name': 'PT_32BUI', 'id':  8 },
        gdalc.GDT_Float32 : { 'name': 'PT_32BF',  'id': 10 },
        gdalc.GDT_Float64 : { 'name': 'PT_64BF',  'id': 11 }
    }

    # XXX: Uncomment these logs to debug types translation
    #logit('MSG: Input GDAL pixel type: %s (%d)\n' % (gdal.GetDataTypeName(gdt), gdt))
    #logit('MSG: Output WKTRaster pixel type: %(name)s (%(id)d)\n' % (pixtypes.get(gdt, 13)))

    return pixtypes.get(gdt, 13)


def pt2fmt(pt):
    """Returns binary data type specifier for given pixel type."""
    fmttypes = {
        4: 'B', # PT_8BUI
        5: 'h', # PT_16BSI
        6: 'H', # PT_16BUI
        7: 'i', # PT_32BSI
        8: 'I', # PT_32BUI
        10: 'f', # PT_32BF
        11: 'd'  # PT_64BF
        }
    return fmttypes.get(pt, 'x')


"""Translate GDAL data type to WKT Raster pixel type."""
NUMPY_TO_PGSQL_TYPE_MAPPING = {
    numpy.uint8:  {'name': 'PT_8BUI', 'id': 4},
    numpy.int16:  {'name': 'PT_16BSI', 'id': 5},
    numpy.uint16:  {'name': 'PT_16BUI', 'id': 6},
    numpy.int32:  {'name': 'PT_32BSI', 'id': 7},
    numpy.uint32:  {'name': 'PT_32BUI', 'id': 8},
    numpy.float32:  {'name': 'PT_32BF', 'id': 10},
    numpy.float64:  {'name': 'PT_64BF', 'id': 11}
}


def __convert_binary_to_hex_encoded_string(fmt, data):
    fmt_little = '<' + fmt #< denotes little endian alignment
    hexstr = binascii.hexlify(struct.pack(fmt_little, data)).upper()
    return hexstr


def __construct_raster_info_wkb(upper_left, scale, skew, srid, width, height, num_bands=1, endian=1, version=0):
    wkb = ''

    #endian-ness, 1 - little endian,1 - big endian
    wkb += __convert_binary_to_hex_encoded_string('B', endian)

    #versionvar
    wkb += __convert_binary_to_hex_encoded_string('H', version)

    #number of bands
    wkb += __convert_binary_to_hex_encoded_string('H', num_bands)

    #geo reference
    wkb += __convert_binary_to_hex_encoded_string('d', scale[0])
    wkb += __convert_binary_to_hex_encoded_string('d', scale[1])
    wkb += __convert_binary_to_hex_encoded_string('d', upper_left[0]) #x
    wkb += __convert_binary_to_hex_encoded_string('d', upper_left[1]) #y
    wkb += __convert_binary_to_hex_encoded_string('d', skew[0])
    wkb += __convert_binary_to_hex_encoded_string('d', skew[1])
    wkb += __convert_binary_to_hex_encoded_string('i', srid)

    #number of columns and rows
    wkb += __convert_binary_to_hex_encoded_string('H', width)
    wkb += __convert_binary_to_hex_encoded_string('H', height)

    return wkb


def get_raster_info_wkb(upper_left, scale, skew, srid, width, height, num_bands, endian, version):
    return __construct_raster_info_wkb(upper_left, scale, skew, srid, width, height, num_bands, endian, version)


def get_raster_band_header_wkb(nodata_value, gdal_data_type):

    pixel_type = gdal2pt(gdal_data_type)["id"]

    wkb = ''
    first4bits = 0
    if not nodata_value is None:
        first4bits += 64
    else:
        nodata_value = 0

    #encode pixel type
    wkb += __convert_binary_to_hex_encoded_string('B', pixel_type + first4bits)

    #encode nodata value
    wkb += __convert_binary_to_hex_encoded_string(pt2fmt(pixel_type), nodata_value)

    return wkb
