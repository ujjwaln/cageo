import os
import cPickle
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.grid_search import GridSearchCV
import numpy as np

from ml.learn import roi_items, DAT_BEGIN_VAR, model_file_name, feature_importance_filename
from ci.util.logger import logger


__author__ = 'ujjwal'


def grid_search(training_file):
    df = pd.read_csv(training_file, header=0)
    cols = list(df.columns.values)

    if DAT_BEGIN_VAR in cols:
        data_begin_idx = cols.index(DAT_BEGIN_VAR)
    else:
        data_begin_idx = cols.index(roi_items[-1])+1

    roi_type_idx = cols.index("type")
    mat = df.values
    x = mat[:, data_begin_idx:].astype(np.float32)
    y = mat[:, roi_type_idx].astype(np.int32)

    params = {
        'n_estimators': [500, 1000, 1500],
        'criterion': ['entropy', 'gini'],
        'min_samples_leaf': [1, 3, 5]
    }

    rf = RandomForestClassifier(n_jobs=-1)
    gs = GridSearchCV(rf, params, n_jobs=-1)
    gs.fit(x, y)

    print gs.best_params_
    print gs.best_score_


def train_model(training_file, n_estimators, min_samples_leaf):

    df = pd.read_csv(training_file, header=0)
    cols = list(df.columns.values)

    if DAT_BEGIN_VAR in cols:
        data_begin_idx = cols.index(DAT_BEGIN_VAR)
    else:
        data_begin_idx = cols.index(roi_items[-1])+1

    roi_type_idx = cols.index("type")
    mat = df.values

    x = mat[:, data_begin_idx:].astype(np.float32)
    y = mat[:, roi_type_idx].astype(np.int32)

    logger.info("Training model, shape (%d, %d)" % (x.shape[0], x.shape[1]))

    rf = RandomForestClassifier(n_estimators=n_estimators, criterion="entropy", n_jobs=-1,
                                min_samples_leaf=min_samples_leaf, oob_score=True)
    rf = rf.fit(x, y)

    logger.info('oob scopre %f' % rf.oob_score_)

    # scores = cross_val_score(rf, x, y, cv=5, n_jobs=-1)
    # logger.info('crossval score %f, %f' % (scores.mean(), scores.std()*2))

    if os.path.exists('../data/%s' % model_file_name):
        os.remove('../data/%s' % model_file_name)

    imps = rf.feature_importances_
    named_imps = zip(cols[data_begin_idx:], imps)
    with open(feature_importance_filename, "w") as impf:
        for pair in sorted(named_imps, key=lambda i: i[1], reverse=True):
            #logger.info("%s, %.5f" % (pair[0], pair[1]))
            impf.write("%s, %.5f\n" % (pair[0], pair[1]))
            #print "%s, %.5f" % (pair[0], pair[1])

    with open(model_file_name, 'wb') as f:
        cPickle.dump(rf, f)
        logger.info("Saved model file %s" % model_file_name)


if __name__ == '__main__':
    tfile = "./../data/train.learn.0hr.csv"
    grid_search(training_file=tfile)
