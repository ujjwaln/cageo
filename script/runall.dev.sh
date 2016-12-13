#!/bin/bash

export SRC=$HOME/essic/python/ci
export PYTHONPATH=$SRC

python $SRC/ci/db/build.py
python $SRC/ci/ingestors/ahps_precip_ingestor.py
python $SRC/ci/ingestors/goes_cm_ingestor.py
python $SRC/ci/ingestors/gtopo_ingestor.py
python $SRC/ci/ingestors/lis_sm_ingestor.py
python $SRC/ci/ingestors/mrms_nc_ingestor.py
python $SRC/ci/ingestors/modis_ndvi_tif_ingestor.py
python $SRC/ci/ingestors/modis_lct_tif_ingestor.py
python $SRC/ci/ingestors/rap_ingestor.py
python $SRC/ci/ingestors/rap_derived_ingestor.py

python $SRC/ci/run/gen_roi_geoms.py
python $SRC/ci/run/gen_roi_stats.py
python $SRC/ci/run/gen_output_file.py