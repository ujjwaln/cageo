import pandas as pd
from ml.prep.defs import roi_items, stat_list, diff_list, variables


__author__ = 'ujjwal'


black_list = {
    "PRATE": "*"
}

DAT_BEGIN_VAR = "ASPECT_count"


def load_df(file_name):
    df = pd.read_csv(file_name, header=0)  # first line contains header
    df.sort(["roi_id"])

    # update white_list with stat names
    var_list = list(roi_items)
    for var in variables:
        for stat in stat_list:
            var_list.append("%s_%s" % (var, stat))

    for var in black_list:
        if black_list[var] == "*":
            stats = stat_list
        else:
            stats = black_list[var]

        for stat in stats:
            var_name = "%s_%s" % (var, stat)
            if var_name in var_list:
                var_list.remove(var_name)

    for col in df.columns.values:
        if not col in var_list:
            df.pop(col)

    column_names = list(df.columns.values)
    #column_names_with_diffs = list(column_names)
    column_names_with_diffs = list(roi_items)

    if DAT_BEGIN_VAR in column_names:
        data_begin_idx = column_names.index(DAT_BEGIN_VAR)
    else:
        data_begin_idx = column_names.index(roi_items[-1])+1

    for diff in diff_list:
        for c in column_names[data_begin_idx:]:
            column_names_with_diffs.append("%s_%s" % (c, diff))

    return df, column_names_with_diffs


def save(df, cols, file_name):
    roi_ids = set(list(df["roi_id"]))
    if DAT_BEGIN_VAR in cols:
        idx = cols.index(DAT_BEGIN_VAR)
    else:
        idx = cols.index(roi_items[-1])+1

    with open(file_name, 'w') as ofile:
        ofile.write(",".join([str(o) for o in cols]))
        ofile.write("\n")

        for roi_id in roi_ids:
            roi_df = df[df["roi_id"] == roi_id]
            if roi_df.shape[0] <> 5:
                #raise Exception("Error in roi %s" % roi_id)
                continue

            #print "Processing %s" % roi_id
            #ci_df = roi_df[(roi_df["type"] == 1) & (roi_df["iarea"] < 0.002)]
            ci_df = roi_df[(roi_df["type"] == 1)]
            if len(ci_df.values):
                ci_df_type_1 = roi_df[roi_df["type"] == -1]
                if (len(ci_df_type_1.values) == 0) or -999 in ci_df_type_1.values:
                    continue
                diff_0_1 = ci_df.values[0, idx:] - ci_df_type_1.values[0, idx:]

                ci_df_type_2 = roi_df[roi_df["type"] == -2]
                if (len(ci_df_type_2.values) == 0) or -999 in ci_df_type_2.values:
                    continue
                diff_1_2 = ci_df_type_1.values[0, idx:] - ci_df_type_2.values[0, idx:]

                ci_df_type_3 = roi_df[roi_df["type"] == -3]
                if (len(ci_df_type_3.values) == 0) or -999 in ci_df_type_3.values:
                    continue
                diff_2_3 = ci_df_type_2.values[0, idx:] - ci_df_type_3.values[0, idx:]

                ci_df_type_4 = roi_df[roi_df["type"] == -4]
                if (len(ci_df_type_4.values) == 0) or -999 in ci_df_type_4.values:
                    continue
                diff_3_4 = ci_df_type_3.values[0, idx:] - ci_df_type_4.values[0, idx:]

                #row = list(ci_df.values[0, :]) + list(diff_0_1) + list(diff_1_2) + list(diff_2_3) + list(diff_3_4)
                row = list(ci_df.values[0, :idx]) + list(diff_0_1) + list(diff_1_2) + list(diff_2_3) + list(diff_3_4)

                ofile.write(",".join(str(o) for o in row))
                ofile.write("\n")

            non_ci_df = roi_df[roi_df["type"] == 0]
            if len(non_ci_df.values):
                non_ci_df_type_1 = roi_df[roi_df["type"] == -5]
                if (len(non_ci_df_type_1.values) == 0) or -999 in non_ci_df_type_1.values:
                    continue
                diff_0_1 = non_ci_df.values[0, idx:] - non_ci_df_type_1.values[0, idx:]

                non_ci_df_type_2 = roi_df[roi_df["type"] == -6]
                if (len(non_ci_df_type_2.values) == 0) or -999 in non_ci_df_type_2.values:
                    continue
                diff_1_2 = non_ci_df_type_1.values[0, idx:] - non_ci_df_type_2.values[0, idx:]

                non_ci_df_type_3 = roi_df[roi_df["type"] == -7]
                if (len(non_ci_df_type_3.values) == 0) or -999 in non_ci_df_type_3.values:
                    continue
                diff_2_3 = non_ci_df_type_2.values[0, idx:] - non_ci_df_type_3.values[0, idx:]

                non_ci_df_type_4 = roi_df[roi_df["type"] == -8]
                if (len(non_ci_df_type_4.values) == 0) or -999 in non_ci_df_type_4.values:
                    continue
                diff_3_4 = non_ci_df_type_3.values[0, idx:] - non_ci_df_type_4.values[0, idx:]

                #row = list(non_ci_df.values[0, :]) + list(diff_0_1) + list(diff_1_2) + list(diff_2_3) + list(diff_3_4)
                row = list(non_ci_df.values[0, :idx]) + list(diff_0_1) + list(diff_1_2) + list(diff_2_3) + list(diff_3_4)

                ofile.write(",".join(str(o) for o in row))
                ofile.write("\n")


if __name__ == "__main__":

    df, cols = load_df("../data/clean.july.csv")
    save(df, cols, "../data/diffs.july.csv")

    df, cols = load_df("../data/clean.aug.csv")
    save(df, cols, "../data/diffs.aug.csv")

    # df, cols = load_df("../data/clean.combo.csv")
    # save(df, cols, "../data/diffs.combo.csv")
