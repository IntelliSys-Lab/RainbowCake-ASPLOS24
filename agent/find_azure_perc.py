import numpy as np
import pandas as pd
import csv
import random
import os
import stat
from glob import glob
from params import USER_CONFIG


def sanitize_invocation_trace(
    azure_file_path,
    invocation_prefix,
    n,
    csv_suffix,
    perc_list
):
    invocation_df = pd.read_csv(azure_file_path + invocation_prefix + n + csv_suffix, index_col="HashFunction")
    invocation_df = invocation_df[~invocation_df.index.duplicated()]
    invocation_df_dict = invocation_df.to_dict('index')


    for func_hash in invocation_df_dict.keys():
        invocation_trace = invocation_df_dict[func_hash]
        v = 0
        for k in invocation_trace.keys():
            if k != "HashOwner" and k != "HashApp" and k != "HashFunction" and k != "Trigger":
                v = v + invocation_trace[k]
        perc_list.append(v / 1440 / 60)

    return perc_list

def get_perc(
    azure_file_path,
    p
):
    csv_suffix = ".csv"

    # Invocation traces
    n_invocation_files = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14"]
    invocation_prefix = "invocations_per_function_md.anon.d"

    perc_list = []
    
    for n in n_invocation_files:
        trace_dict = sanitize_invocation_trace(
            azure_file_path=azure_file_path, 
            invocation_prefix=invocation_prefix, 
            n=n, 
            csv_suffix=csv_suffix, 
            perc_list=perc_list
        )

    print("Finish sampling invocation traces!")

    return [np.percentile(perc_list, i) for i in p]


if __name__ == "__main__":
    azure_file_path = "azurefunctions-dataset2019/"
    p = [90, 95, 99]

    print("Load Azure Functions traces...")
    result = get_perc(
        azure_file_path=azure_file_path,
        p=p
    )

    for i, r in zip(p, result):
        print("Percentile {} is {}".format(i, r))
