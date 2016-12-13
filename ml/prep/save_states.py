import pandas as pd
from ml.prep.defs import roi_items, stat_list, variables

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
    return df, column_names


def save(df, cols, file_name):
    roi_ids = set(list(df["roi_id"]))
    if DAT_BEGIN_VAR in cols:
        idx = cols.index(DAT_BEGIN_VAR)
    else:
        idx = cols.index(roi_items[-1])+1

    all_cols = []
    all_cols += cols[:idx]
    for t in range(0, 5):
        for col in cols[idx:]:
            all_cols.append("%s_%d" % (col, t))

    with open(file_name, 'w') as ofile:
        ofile.write(",".join([str(o) for o in all_cols]))
        ofile.write("\n")

        for roi_id in roi_ids:
            roi_df = df[df["roi_id"] == roi_id]
            if roi_df.shape[0] <> 5:
                #raise Exception("Error in roi %s" % roi_id)
                continue

            # if roi_id == '55f39155-5476-4cbe-92af-29dc2a1cff42':
            #     print roi_id

            #print "Processing %s" % roi_id
            ci_df = roi_df[(roi_df["type"] == 1)]
            if len(ci_df.values):
                ci_df_type_1 = roi_df[roi_df["type"] == -1]
                if (len(ci_df_type_1.values) == 0) or -999 in ci_df_type_1.values:
                    continue
                ci_df_type_2 = roi_df[roi_df["type"] == -2]
                if (len(ci_df_type_2.values) == 0) or -999 in ci_df_type_2.values:
                    continue
                ci_df_type_3 = roi_df[roi_df["type"] == -3]
                if (len(ci_df_type_3.values) == 0) or -999 in ci_df_type_3.values:
                    continue
                ci_df_type_4 = roi_df[roi_df["type"] == -4]
                if (len(ci_df_type_4.values) == 0) or -999 in ci_df_type_4.values:
                    continue

                row = list(ci_df.values[0, :]) + \
                      list(ci_df_type_1.values[0, idx:]) + list(ci_df_type_2.values[0, idx:]) + \
                      list(ci_df_type_3.values[0, idx:]) + list(ci_df_type_4.values[0, idx:])

                ofile.write(",".join(str(o) for o in row))
                ofile.write("\n")

            non_ci_df = roi_df[roi_df["type"] == 0]
            if len(non_ci_df.values):
                nci_df_type_1 = roi_df[roi_df["type"] == -5]
                if (len(nci_df_type_1.values) == 0) or -999 in nci_df_type_1.values:
                    continue
                nci_df_type_2 = roi_df[roi_df["type"] == -6]
                if (len(nci_df_type_2.values) == 0) or -999 in nci_df_type_2.values:
                    continue
                nci_df_type_3 = roi_df[roi_df["type"] == -7]
                if (len(nci_df_type_3.values) == 0) or -999 in nci_df_type_3.values:
                    continue
                nci_df_type_4 = roi_df[roi_df["type"] == -8]
                if (len(nci_df_type_4.values) == 0) or -999 in nci_df_type_4.values:
                    continue

                row = list(non_ci_df.values[0, :]) + \
                      list(nci_df_type_1.values[0, idx:]) + list(nci_df_type_2.values[0, idx:]) + \
                      list(nci_df_type_3.values[0, idx:]) + list(nci_df_type_4.values[0, idx:])

                ofile.write(",".join(str(o) for o in row))
                ofile.write("\n")


if __name__ == "__main__":

    df, cols = load_df("../data/clean.july.csv")
    save(df, cols, "../data/states.july.csv")

    df, cols = load_df("../data/clean.aug.csv")
    save(df, cols, "../data/states.aug.csv")

    # df, cols = load_df("../data/clean.combo.csv")
    # save(df, cols, "../data/states.combo.csv")
