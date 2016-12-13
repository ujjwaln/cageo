from ml.prep.clean_output import clean as clean_output
from ml.prep.save_states import save as save_states
from ml.prep.save_states import load_df as load_state_df
from ml.prep.save_diffs import save as save_diffs
from ml.prep.save_diffs import load_df as load_diff_df
from ci.config import logger
import pandas as pd
import numpy as np


ifiles = ["./data/output.july.csv", "./data/output.aug.csv"]

cfile = "./data/combo.csv"
ofilename = "./data/clean.csv"
test_file = "./data/tmp.test.csv"
train_file = "./data/tmp.train.csv"

test_diff_file = "./data/test.diffs.csv"
test_state_file = "./data/test.states.csv"
train_diff_file = "./data/train.diffs.csv"
train_state_file = "./data/train.states.csv"


split_factor = 0.2 # N * split_factor test features, N * (1-split_factor) train features

SHUFFLED = 1
TEMPORAL = 2


def split_test_train(create_combo=False, clean=False, mode=SHUFFLED):

    if create_combo:
        logger.info("creating july/aug combined output file %s" % cfile)
        flag = True
        with open(cfile, 'w') as of:
            for f in ifiles:
                logger.info("processing %s" % f)
                with open(f, 'r') as fin:
                    line = fin.readline()
                    while len(line) > 0:
                        if "roi_id" in line:
                            if flag:
                                of.write(line)
                            flag = False
                        else:
                            of.write(line)
                        line = fin.readline()

    if clean:
        logger.info("cleaning output file %s" % cfile)
        clean_output(cfile, ofilename)

    logger.info("Shuffling data for test and train")
    df = pd.read_csv(ofilename, header=0)

    if mode == SHUFFLED:
        grouped = df.groupby("roi_id")
        roi_ids = grouped.indices.keys()
        np.random.shuffle(roi_ids)
        split_idx = int(split_factor * len(roi_ids))
        test_roi_ids = roi_ids[0:split_idx]
        train_roi_ids = roi_ids[split_idx:-1]

        test_df = df.loc[(df["roi_id"].isin(test_roi_ids))]
        train_df = df.loc[(df["roi_id"].isin(train_roi_ids))]

    elif mode == TEMPORAL:
        starttimes = sorted(df["starttime"].values)
        split_time = starttimes[int(len(starttimes) * (1-split_factor))]

        train_df = df[df["starttime"] <= split_time]
        test_df = df[df["starttime"] > split_time]

    else:
        raise Exception("mode incorrect")

    test_df.to_csv(test_file, index=False)
    logger.info("Saved test file")

    train_df.to_csv(train_file, index=False)
    logger.info("Saved train file")


def save_learning_files():

    df, cols = load_state_df(train_file)
    save_states(df, cols, train_state_file)
    logger.info("Saved train state file")

    df, cols = load_diff_df(train_file)
    save_diffs(df, cols, train_diff_file)
    logger.info("Saved train diff file")

    df, cols = load_state_df(test_file)
    save_states(df, cols, test_state_file)
    logger.info("Saved test state file")

    df, cols = load_diff_df(test_file)
    save_diffs(df, cols, test_diff_file)
    logger.info("Saved test diff file")


if __name__ == "__main__":
    split_test_train(create_combo=False, clean=True, mode=SHUFFLED)
    save_learning_files()
