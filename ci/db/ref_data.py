from ci.db.sqa.models import DataFormat, Variable, Provider

__author__ = 'ujjwal'


#formats
raster_format = DataFormat(name="RASTER")
vector_format = DataFormat(name="VECTOR")

FORMATS = [raster_format, vector_format]

#insert variables
refl_variable = Variable(name="REFL", description="Radar Base Reflectivity", unit="dB")
refl_reclass_var = Variable(name='REFL_reclass', unit=None, description="Reclassified MRMS REFL")
ndvi_variable = Variable(name="NDVI", description="NDVI", unit=None)
lct_variable = Variable(name="LCT", description="Land Cover Type", unit=None)
elev_variable = Variable(name="ELEV", description="Elevation", unit=None)
slope_variable = Variable(name="SLOPE", description="Topographic slope in degrees", unit=None)
aspect_variable = Variable(name="ASPECT", description="Topographic aspect in degrees", unit=None)
temp_variable = Variable(name="TEMP", description="Temperature in K", unit=None)

cape_variable0 = Variable(name="CAPE0", description="Convective Available Potential Energy", unit=None)
cape_variable1 = Variable(name="CAPE1", description="Convective Available Potential Energy", unit=None)
cape_variable2 = Variable(name="CAPE2", description="Convective Available Potential Energy", unit=None)
cape_variable3 = Variable(name="CAPE3", description="Convective Available Potential Energy", unit=None)

cin_variable0 = Variable(name="CIN0", description="Convective Inhibition", unit=None)
cin_variable1 = Variable(name="CIN1", description="Convective Inhibition", unit=None)
cin_variable2 = Variable(name="CIN2", description="Convective Inhibition", unit=None)
cin_variable3 = Variable(name="CIN3", description="Convective Inhibition", unit=None)

lfc_variable = Variable(name="LFC", description="Level of Free Convection", unit=None)
inso_variable = Variable(name="INSO", description="GOES Solar Insolation", unit=None)
htfl_hgt_variable = Variable(name="HTFL_HGT", description="Height of Freezing Level - Geopotential height", unit=None)
htfl_rh_variable = Variable(name="HTFL_RH", description="Height of Freezing Level - RH", unit=None)
uwnd_variable = Variable(name="UGRD", description="U-component of wind", unit=None)
vwnd_variable = Variable(name="VGRD", description="V-component of wind", unit=None)
uder_variable = Variable(name="UDER", description="dU/dX", unit=None)
vder_variable = Variable(name="VDER", description="dV/dX", unit=None)
uv_conv_variable = Variable(name="WINDCONV", description="dU/dX + dV/dX", unit=None)
dwpnt_variable = Variable(name="DEWPOINT", description="Surface dewpoint temperature", unit=None)
prate_variable = Variable(name="PRATE", description="Precipitation rate", unit=None)
rap_refl_variable = Variable(name="RAP_REFL", description="RAP simulated Reflectivity", unit=None)

daily_precip_variable = Variable(name="DAILYPRECIP", description="Daily precipitation", unit=None)
cloudtype_variable = Variable(name="CLOUDTYPE", description="Cloud Type", unit=None)

cum_cloudtype_variable = Variable(name="CUM_CLOUD", description="Cumulus Cloud Type", unit=None)
tcum_cloudtype_variable = Variable(name="TCUM_CLOUD", description="Towering Cumulus Cloud Type", unit=None)

smois_variable0 = Variable(name="SMOIS0", description="0-10 cm  soil moisture", unit=None)
smois_variable1 = Variable(name="SMOIS1", description="10-40 cm soil moisture", unit=None)
stemp_variable = Variable(name="TSOIL0", description="0-10 cm  soil temperature", unit=None)
ctemp_variable = Variable(name="CONVTEMP", description="Convective temperature", unit=None)
waterbody_variable = Variable(name="WATERBODY", description="Land Water mask", unit=None)
ci_count_variable = Variable(name="CI_COUNT", description="CI Event count", unit=None)

ku_sigma_variable = Variable(name="KU_SIGMA", description="Ku sigma", unit=None)
ka_sigma_variable = Variable(name="KA_SIGMA", description="Ka sigma", unit=None)

VARIABLES = [refl_variable, refl_reclass_var, ndvi_variable, lct_variable, elev_variable, lfc_variable,
             inso_variable, htfl_hgt_variable, htfl_rh_variable, slope_variable, aspect_variable,
             uwnd_variable, vwnd_variable, daily_precip_variable, temp_variable,
             cape_variable0, cape_variable1, cape_variable2, cape_variable3, cin_variable0, cin_variable1,
             cin_variable2, cin_variable3, smois_variable0, smois_variable1, stemp_variable, dwpnt_variable,
             ctemp_variable, uder_variable, vder_variable, waterbody_variable, uv_conv_variable, prate_variable,
             cloudtype_variable, tcum_cloudtype_variable, cum_cloudtype_variable, rap_refl_variable, ci_count_variable,
             ku_sigma_variable, ka_sigma_variable]

#insert Providers
mrms_provider = Provider(name="MRMS")
modis_provider = Provider(name="MODIS")
gtopo_provider = Provider(name="GTOPO30")
rap_provider = Provider(name="RAP")
ahps_provider = Provider(name="AHPS")
gfs_provider = Provider(name="GFS")
goes_provider = Provider(name="GOES")
lis_provider = Provider(name="LIS")
gpm_provider = Provider(name="GPM")

PROVIDERS = [mrms_provider, modis_provider, gtopo_provider, rap_provider,
             ahps_provider, gfs_provider, goes_provider, lis_provider, gpm_provider]

import os
data_dir = os.path.join(os.path.dirname(__file__), "../data")
se_mask = ("shapefile", "se", os.path.join(data_dir, r"se_layers.shp"))
al_mask = ("bbox", "alabama", (-90, 35, -85, 30), 4326)
se_bbox = ("bbox", "se_bbox", (-94.6, 37, -75, 25), 4326)
al_sm_bbox = ("bbox", "al_sm_bbox", (-85.7124426614, 30.279847741, -84.272442661, 29.49984774), 4326)

MASKINFOS = [al_mask, se_mask, se_bbox, al_sm_bbox]
