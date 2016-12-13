import pandas as pd
from ci.config import logger


__author__ = 'ujjwal'

"""
    Check for -999 values and replace with zero or remove entire row
"""

check_vars = ['CAPE0', 'CAPE1', 'CAPE2', 'CAPE3', 'CIN0', 'CIN1', 'CIN2', 'CIN3', 'SMOIS0', 'SMOIS1']
replace_vars = ['CUM_CLOUD', 'TCUM_CLOUD', 'DAILYPRECIP', 'ELEV', 'LCT', 'NDVI', 'SLOPE', 'ASPECT', 'WATERBODY', 'RAP_REFL']
stats = ['count', 'max', 'mean', 'stddev', 'min', 'sum']


def good_data(df):
    use_col = []
    col_names = []
    for var in check_vars:
        for stat in stats:
            var_name = "%s_%s" % (var, stat)
            col_names.append(var_name)

    for row_index, row in df.iterrows():
        good = True
        if -999 in row[col_names].values:
            good = False
        use_col.append(good)

    return use_col


def replace_data(df):
    col_names = []
    for var in replace_vars:
        for stat in stats:
            var_name = "%s_%s" % (var, stat)
            col_names.append(var_name)

    for row_index, row in df.iterrows():
        if -999 in row[col_names].values:
            for col in df.columns:
                if col in col_names and row.loc[col] == -999:
                    df.loc[row_index, col] = 0


def clean(file_name, ofilename, max_waterbody_count=-1):
    logger.info('processing %s' % file_name)
    odf = pd.read_csv(file_name, header=0)  # first line contains header

    logger.info('checking data, %d rows' % len(odf))
    use_rows = good_data(odf)

    odf1 = odf.loc[use_rows].copy()
    logger.info('replacing bad data, %d good rows' % len(odf1))
    replace_data(odf1)

    if max_waterbody_count > 0:
        logger.info('restricting rows by WATERBODY_count')
        odf2 = odf1[odf1["WATERBODY_count"] < max_waterbody_count]
        odf2.to_csv(ofilename, index=False)
    else:
        # print 'writing outpout file %s, %d rows' % (ofilename, len(odf2))
        odf1.to_csv(ofilename, index=False)

if __name__ == '__main__':

    file_name = "../data/output.july.csv"
    ofilename = "../data/clean.july.csv"
    clean(file_name, ofilename)

    file_name = "../data/output.aug.csv"
    ofilename = "../data/clean.aug.csv"
    clean(file_name, ofilename)

    # file_name = "../data/output.combo.csv"
    # ofilename = "../data/clean.combo.csv"
    # clean(file_name, ofilename)
