import pandas as pd
from ml.learn import roi_items, predictor_map
from ci.util.logger import logger


__author__ = 'ujjwal'


def create_learning_input(state_file_name, diff_file_name, combo_file_name):
    state_df = pd.read_csv(state_file_name, header=0) #first line contains header
    state_df_columns = list(state_df.columns.values)

    diff_df = pd.read_csv(diff_file_name, header=0) #first line contains header
    df_diff_columns = list(diff_df.columns.values)

    state_cols = list(roi_items)
    for var_name in predictor_map:
        for stat in predictor_map[var_name]['stats']:
            for state in predictor_map[var_name]['states']:
                col_name = "%s_%s_%d" % (var_name, stat, state)
                if col_name in state_df_columns:
                    state_cols.append(col_name)

    for col in state_df.columns.values:
        if not col in state_cols:
            state_df.pop(col)

    diff_cols = []
    for var_name in predictor_map:
        for stat in predictor_map[var_name]['stats']:
            if isinstance(predictor_map[var_name]['diffs'], list):
                for diff in predictor_map[var_name]['diffs']:
                    col_name = "%s_%s_diff_%s" % (var_name, stat, diff)
                    if col_name in df_diff_columns:
                        diff_cols.append(col_name)

    for col in diff_df.columns.values:
        if not col in diff_cols:
            diff_df.pop(col)

    combo_df = pd.concat([state_df, diff_df], axis=1)

    of = open(combo_file_name, 'w')
    of.write(",".join([col for col in combo_df.columns.values]))
    of.write("\n")
    for row_index, row in combo_df.iterrows():
        of.write(",".join([str(v) for v in row.values]))
        of.write("\n")
    of.close()

    logger.info("Created file %s" % combo_file_name)


if __name__ == "__main__":

    state_file_name = "../data/test.states.csv"
    diff_file_name = "../data/test.diffs.csv"
    test_learn_file = "../data/test.learn.4hr.csv"
    create_learning_input(state_file_name, diff_file_name, test_learn_file)

    state_file_name = "../data/train.states.csv"
    diff_file_name = "../data/train.diffs.csv"
    train_learn_file = "../data/train.learn.4hr.csv"
    create_learning_input(state_file_name, diff_file_name, train_learn_file)
