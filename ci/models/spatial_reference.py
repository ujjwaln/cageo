import osr

__author__ = 'ujjwal'


SRID_WGS84 = 4326
SRID_MODIS = 96842
SRID_RAP = 900914
SRID_ALBERS = 900915
SRID_HRAP = 900916
SRID_GFS = 900917
SRID_LIS = 900918
SRID_USA_ALBERS = 102003


class SpatialReference(object):
    def __init__(self, esri_sr_text, force_epsg=-1):
        srs = osr.SpatialReference()
        srs.ImportFromESRI([esri_sr_text])

        self.wkt = srs.ExportToWkt()
        self.proj4 = srs.ExportToProj4()

        srs.AutoIdentifyEPSG()
        self.epsg = srs.GetAuthorityCode(None)
        if self.epsg is None:
            self.epsg = force_epsg
        else:
            self.epsg = int(self.epsg)

        auth_name = 'sr-org'
        auth_srid = 6842
        self.sql_insert_statement = """
            INSERT into spatial_ref_sys(srid, auth_name, auth_srid, proj4text, srtext)
            values (%d, '%s', %d, '%s', '%s');
        """ % (self.epsg, auth_name, auth_srid, self.proj4, self.wkt)

        self.srs=srs

__sr_text_modis_sinusoidal = \
        'PROJCS["Sinusoidal",'+\
            'GEOGCS["GCS_Undefined",'+\
                'DATUM["Undefined",SPHEROID["User_Defined_Spheroid",6371007.181,0.0]],'+\
                'PRIMEM["Greenwich",0.0],'+\
                'UNIT["Degree",0.0174532925199433]'+\
            '],'+\
            'PROJECTION["Sinusoidal"],'+\
            'PARAMETER["False_Easting",0.0],'+\
            'PARAMETER["False_Northing",0.0],'+\
            'PARAMETER["Central_Meridian",0.0],'+\
            'UNIT["Meter",1.0]'+\
        ']'

__sr_text_rap_grid =\
    'PROJCS["unnamed",'+\
        'GEOGCS["Coordinate System imported from GRIB file",'+\
            'DATUM["unknown", SPHEROID["Sphere",6371229,0]],'+\
            'PRIMEM["Greenwich",0],'+\
            'UNIT["degree",0.0174532925199433]'+\
        '],'+\
        'PROJECTION["Lambert_Conformal_Conic_2SP"],'+\
        'PARAMETER["standard_parallel_1",25],'+\
        'PARAMETER["standard_parallel_2",25],'+\
        'PARAMETER["latitude_of_origin",25],'+\
        'PARAMETER["central_meridian",265],'+\
        'PARAMETER["false_easting",0],'+\
        'PARAMETER["false_northing",0]'+\
    ']'

__sr_text_albers = \
    'PROJCS["NAD_1983_Albers",'+\
    'GEOGCS["NAD83",'+\
        'DATUM["North_American_Datum_1983",'+\
            'SPHEROID["GRS 1980",6378137,298.257222101,'+\
                'AUTHORITY["EPSG","7019"]],'+\
            'TOWGS84[0,0,0,0,0,0,0],'+\
            'AUTHORITY["EPSG","6269"]],'+\
        'PRIMEM["Greenwich",0,'+\
            'AUTHORITY["EPSG","8901"]],'+\
        'UNIT["degree",0.0174532925199433,'+\
            'AUTHORITY["EPSG","9108"]],'+\
        'AUTHORITY["EPSG","4269"]],'+\
    'PROJECTION["Albers_Conic_Equal_Area"],'+\
    'PARAMETER["standard_parallel_1",29.5],'+\
    'PARAMETER["standard_parallel_2",45.5],'+\
    'PARAMETER["latitude_of_center",23],'+\
    'PARAMETER["longitude_of_center",-96],'+\
    'PARAMETER["false_easting",0],'+\
    'PARAMETER["false_northing",0],'+\
    'UNIT["meters",1]]'

__sr_text_hrap_polar = \
    'PROJCS["User_Defined_Stereographic_North_Pole",'+\
    'GEOGCS["GCS_User_Defined",'+\
        'DATUM["D_User_Defined",'+\
            'SPHEROID["User_Defined_Spheroid",6371200.0,0.0]],'+\
        'PRIMEM["Greenwich",0.0],'+\
        'UNIT["Degree",0.0174532925199433]],'+\
    'PROJECTION["Stereographic_North_Pole"],'+\
    'PARAMETER["False_Easting",0.0],'+\
    'PARAMETER["False_Northing",0.0],'+\
    'PARAMETER["Central_Meridian",-105.0],'+\
    'PARAMETER["Standard_Parallel_1",60.0],'+\
    'UNIT["Meter",1.0]]'

__sr_text_gfs = \
    'GEOGCS["Coordinate System imported from GRIB file",'+\
    'DATUM["unknown",'+\
    '    SPHEROID["Sphere",6371229,0]],'+\
    'PRIMEM["Greenwich",0],'+\
    'UNIT["degree",0.0174532925199433]]'

__sr_text_lis = \
    'GEOGCS["Coordinate System imported from GRIB file",'+\
    'DATUM["unknown",'+\
    '    SPHEROID["Sphere",6371200,0]],'+\
    'PRIMEM["Greenwich",0],'+\
    'UNIT["degree",0.0174532925199433]]'

__sr_text_usa_contig_albers = \
    'PROJCS["USA_Contiguous_Albers_Equal_Area_Conic",'+\
    'GEOGCS["GCS_North_American_1983",'+\
    '    DATUM["North_American_Datum_1983",'+\
    '        SPHEROID["GRS_1980",6378137,298.257222101]],'+\
    '    PRIMEM["Greenwich",0],'+\
    '    UNIT["Degree",0.017453292519943295]],'+\
    'PROJECTION["Albers_Conic_Equal_Area"],'+\
    'PARAMETER["False_Easting",0],'+\
    'PARAMETER["False_Northing",0],'+\
    'PARAMETER["longitude_of_center",-96],'+\
    'PARAMETER["Standard_Parallel_1",29.5],'+\
    'PARAMETER["Standard_Parallel_2",45.5],'+\
    'PARAMETER["latitude_of_center",37.5],'+\
    'UNIT["Meter",1],'+\
    'AUTHORITY["EPSG","102003"]]'


MODIS_SpatialReference = SpatialReference(__sr_text_modis_sinusoidal, SRID_MODIS)
RAP_Spatial_Reference = SpatialReference(__sr_text_rap_grid, SRID_RAP)
ALBERS_Spatial_Reference = SpatialReference(__sr_text_albers, SRID_ALBERS)
HRAP_Spatial_Reference = SpatialReference(__sr_text_hrap_polar, SRID_HRAP)
GFS_Spatial_Reference = SpatialReference(__sr_text_gfs, SRID_GFS)
LIS_Spatial_Reference = SpatialReference(__sr_text_lis, SRID_LIS)
USA_CONTIG_ALBERS_Spatial_Reference = SpatialReference(__sr_text_usa_contig_albers, SRID_USA_ALBERS)


def create_hrap_shapefile_srs_from_esri_prj():
    prj=r"/home/ujjwal/nws_precip_1day_observed_shape_20150309/nws_precip_1day_observed_20150309.prj"
    prj_file = open(prj, 'r')
    prj_txt = prj_file.read()
    print prj_txt

    sr = osr.SpatialReference()
    sr.ImportFromESRI([prj_txt])
    print sr.ExportToProj4()
    print sr.AutoIdentifyEPSG()


if __name__=='__main__':
    print GFS_Spatial_Reference.proj4
    print RAP_Spatial_Reference.proj4
    print ALBERS_Spatial_Reference.proj4
