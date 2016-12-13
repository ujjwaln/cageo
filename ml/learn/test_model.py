import cPickle
import pandas as pd
import numpy as np
from ml.learn import roi_items, DAT_BEGIN_VAR, model_file_name
from ci.util.logger import logger
from sklearn.metrics import classification_report


__author__ = 'ujjwal'


def test_model(testing_file, roi_output_file):

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
        preds = rf.predict(x)
        #preds = rf.predict_proba(x)

        class_names = ["Non CI",  "CI"]
        logger.info(classification_report(y, preds, labels=[0, 1], target_names=class_names))

        roi_ofile = open(roi_output_file, 'w')
        roi_ofile.write("roi_name, actual, prediction, lat, lon\n")
        roi_names = df["roi_id"]

        idx = 0
        for p in preds:
            # roi_ofile.write("%s, %d, %f, %f, %f\n" % (roi_names[idx], y[idx], p[1], lats[idx], lons[idx]))
            roi_ofile.write("%s, %d, %f, %f, %f\n" % (roi_names[idx], y[idx], p, lats[idx], lons[idx]))
            idx += 1

        roi_ofile.close()


def rap_refl_accuracy(output_file):

    df = pd.read_csv(output_file, header=0)
    #df = df.query('0 <= type <= 1')
    x = df["type"].as_matrix()

    refls = df["RAP_REFL_max_0"]
    y = []
    for r in refls:
        if r >= 35:
            y.append(1)
        else:
            y.append(0)

    correct_ci = 0
    total_ci = 0
    correct_non_ci = 0
    total_non_ci = 0
    results = zip(x, y)

    for r in results:
        if r[0] == 1:
            if r[0] == r[1]:
                correct_ci += 1
            total_ci += 1

        if r[0] == r[1]:
            total_non_ci += 1
            if r[0] == 0:
                correct_non_ci += 1

    logger.info("CI Accuracy %f" % (100.0 * correct_ci / total_ci))
    logger.info("Non CI Accuracy %f" % (100.0 * correct_non_ci / total_non_ci))

