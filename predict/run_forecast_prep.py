from ml.prep.clean_output import clean as clean_output
from ml.prep.save_states import save as save_states
from ml.prep.save_states import load_df as load_state_df
from ml.prep.save_diffs import save as save_diffs
from ml.prep.save_diffs import load_df as load_diff_df

from ml.learn import hour, model_file_name, DAT_BEGIN_VAR, roi_items
from ml.learn.create_learning_input_file import create_learning_input

import os
import pandas as pd
import numpy as np
import cPickle

from ci.config import logger


ifile = "./data/output.pred.csv.08-08-2030"
ofilename = "./data/clean.csv"
diff_file = "./data/diffs.csv"
state_file = "./data/states.csv"


def save_learning_files():

    logger.info("cleaning output file %s" % ifile)
    clean_output(ifile, ofilename)

    df, cols = load_state_df(ofilename)
    save_states(df, cols, state_file)
    logger.info("Saved state file")

    df, cols = load_diff_df(ofilename)
    save_diffs(df, cols, diff_file)
    logger.info("Saved diff file")


def gen_forecast():

    curdir = os.path.dirname(__file__)
    data_dir = os.path.join(curdir, "data")

    #create random forest training input file
    state_file_name = os.path.join(data_dir, "states.csv")
    diff_file_name = os.path.join(data_dir, "diffs.csv")
    learn_file = os.path.join(data_dir, "learn.%dhr.csv" % hour)
    create_learning_input(state_file_name, diff_file_name, learn_file)

    #test the model
    testing_file = os.path.join(data_dir, "learn.%dhr.csv" % hour)
    roi_output_file = os.path.join(data_dir, "roi_output.pred.%dhr.csv" % hour)

    df = pd.read_csv(testing_file, header=0)
    cols = list(df.columns.values)

    with open(model_file_name, 'rb') as f:
        rf = cPickle.load(f)

        if DAT_BEGIN_VAR in cols:
            data_begin_idx = cols.index(DAT_BEGIN_VAR)
        else:
            data_begin_idx = cols.index(roi_items[-1])+1

        roi_type_idx = cols.index("type")
        mat = df.as_matrix()

        x = mat[:, data_begin_idx:].astype(np.float32)
        y = mat[:, roi_type_idx].astype(np.int32)
        lats = mat[:, 2].astype(np.float32)
        lons = mat[:, 3].astype(np.float32)
        logger.info("Testing model, shape (%d, %d)" % (x.shape[0], x.shape[1]))

        #preds = rf.predict(x)
        preds = rf.predict_proba(x)

        roi_ofile = open(roi_output_file, 'w')
        roi_ofile.write("roi_name, actual, prediction, lat, lon\n")
        roi_names = df["roi_id"]

        idx = 0
        for p in preds:
            roi_ofile.write("%s, %d, %f, %f, %f\n" % (roi_names[idx], y[idx], p[1], lats[idx], lons[idx]))
            #roi_ofile.write("%s, %d, %f, %f, %f\n" % (roi_names[idx], y[idx], p, lats[idx], lons[idx]))
            idx += 1

        roi_ofile.close()


if __name__ == "__main__":
    #save_learning_files()
    gen_forecast()
