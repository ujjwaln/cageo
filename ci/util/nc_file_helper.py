__author__ = 'ujjwal'


import netCDF4
import numpy
import random
from datetime import datetime
from netCDF4 import date2num


def create_test_nc_file(filename, num_lats=10, num_lons=10, num_hghts=1, num_times=1, format='NETCDF4'):

    f = netCDF4.Dataset(filename, 'w', format)

    lat_data = numpy.arange(34.35, 34.45, 0.01)
    lon_data = numpy.arange(-86.34, -86.14, 0.01)

    num_lats = len(lat_data)
    num_lons = len(lon_data)

    # alabama_max_lat = 34.448821
    # alabama_min_lat = 34.349672
    # alabama_max_lon = -86.148434
    # alabama_min_lon = -86.340866

    #dimensions
    f.createDimension('lat', num_lats)
    f.createDimension('lon', num_lons)
    #f.createDimension('Ht', num_hghts)
    #f.createDimension('time', num_times)

    # variables.
    #times = f.createVariable('time', 'f8', ('time',))
    #ht = f.createVariable('Ht', 'i4', ('Ht',))
    lat = f.createVariable('Lat', 'f4', ('lat',))
    lon = f.createVariable('Lon', 'f4', ('lon',))

    lat[:] = lat_data[:]
    lon[:] = lon_data[:]
    #ht[:] = numpy.arange(0, num_hghts, 1)

    f.description = 'test_nc_file'
    f.history = 'created by create_test_nc_file'
    f.source = 'test script'
    lat.units = 'degrees north'
    lon.units = 'degrees east'
    #ht.units = 'hPa'

    #temp.units = 'K'
    # times.units = 'hours since 0001-01-01 00:00:00.0'
    # times.calendar = 'gregorian'

    dates = [datetime(2001, 3, 1)]
    #times[:] = date2num(dates, units=times.units, calendar=times.calendar)
    #mrefl = f.createVariable("MREFL", 'f4', ('Ht', 'lat', 'lon'))
    mrefl = f.createVariable("ELEV", 'f4', ('lat', 'lon'))

    #for num_hght in range(0, num_hghts, 1):
    grid = []
    for i in range(0, num_lats, 1):
        row = []
        for j in range(0, num_lons, 1):
            row.append(random.random() * 100.0)
        grid.append(row)

    #mrefl[num_hght] = numpy.array(grid)
    mrefl = numpy.array(grid)

    f.close()


def print_netcdf_info(nc_file, format="NETCDF4"):
    rootgroup = netCDF4.Dataset(nc_file, "r", format)

    print "dimensions"
    for dimension in rootgroup.dimensions:
        print dimension
        print rootgroup.dimensions[dimension]

    print "variables"
    for variable in rootgroup.variables:
        print variable
        print rootgroup.variables[variable]

    print "attributes"
    for name in rootgroup.ncattrs():
        print name, '=', getattr(rootgroup, name)


def nc_get_1d_vars_as_list(nc_file, varnames):
    vars = {}
    rootgroup = netCDF4.Dataset(nc_file, "r", format="NETCDF4")

    for varname in varnames:
        var = rootgroup.variables[varname]
        values = var[:]
        vars[varname]=values

    rootgroup.close()
    return vars


if __name__ == '__main__':
    create_test_nc_file("test.nc", num_lats=4, num_lons=4, num_hghts=2)
