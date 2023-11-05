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
    max_timestep,
    min_timestep,
    max_invoke_per_time,
    min_invoke_per_time,
    max_invoke_per_func,
    min_invoke_per_func,
    iat_cv,
    trace_dict
):
    def cv(x):
        return np.std(x) / np.mean(x)

    invocation_df = pd.read_csv(azure_file_path + invocation_prefix + n + csv_suffix, index_col="HashFunction")
    invocation_df = invocation_df[~invocation_df.index.duplicated()]
    invocation_df_dict = invocation_df.to_dict('index')

    for func_hash in invocation_df_dict.keys():
        invocation_trace = invocation_df_dict[func_hash]

        done = False
        cur_base = 0
        while done is False:
            timeline = []
            for timestep in range(max_timestep):
                invoke_num = int(invocation_trace["{}".format(cur_base + timestep + 1)])

                # Verify invocation per time
                if invoke_num <= max_invoke_per_time and invoke_num >= min_invoke_per_time:
                    timeline.append(invoke_num)
                else:
                    break

            # Verify invocation per function
            total_invoke_num = np.sum(timeline)
            total_length = len(timeline)
            if total_invoke_num <= max_invoke_per_func and total_invoke_num >= min_invoke_per_func and \
                    total_length <= max_timestep and total_length >= min_timestep and \
                        cv(timeline) >= iat_cv:
                trace_dict[func_hash] = {}
                trace_dict[func_hash]["invocation_trace"] = timeline
                trace_dict[func_hash]["HashApp"] = invocation_trace["HashApp"]
                trace_dict[func_hash]["Trigger"] = invocation_trace["Trigger"]
                done = True
            else:
                cur_base = cur_base + max_timestep
                if cur_base >= 1440:
                    done = True

    return trace_dict

def match_memory_trace(
    azure_file_path,
    memory_prefix,
    n,
    csv_suffix,
    trace_dict
):
    memory_df = pd.read_csv(azure_file_path + memory_prefix + n + csv_suffix, index_col="HashApp")
    memory_df = memory_df[~memory_df.index.duplicated()]
    memory_df_dict = memory_df.to_dict('index')
    
    for func_hash in trace_dict.keys():
        app_hash = trace_dict[func_hash]["HashApp"]
        if app_hash in memory_df_dict:
            trace_dict[func_hash]["memory_trace"] = memory_df_dict[app_hash]

    return trace_dict

def match_duration_trace(
    azure_file_path,
    duration_prefix,
    n,
    csv_suffix,
    trace_dict
):
    duration_df = pd.read_csv(azure_file_path + duration_prefix + n + csv_suffix, index_col="HashFunction")
    duration_df = duration_df[~duration_df.index.duplicated()]
    duration_df_dict = duration_df.to_dict('index')

    for func_hash in trace_dict.keys():
        if func_hash in duration_df_dict:
            trace_dict[func_hash]["duration_trace"] = duration_df_dict[func_hash]

    return trace_dict

#
# Import Azure Functions traces
#
def get_trace_dict(
    azure_file_path,
    max_timestep,
    min_timestep,
    max_invoke_per_time,
    min_invoke_per_time,
    max_invoke_per_func,
    min_invoke_per_func,
    iat_cv,
):
    csv_suffix = ".csv"

    # Invocation traces
    n_invocation_files = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14"]
    invocation_prefix = "invocations_per_function_md.anon.d"

    trace_dict = {}
    
    for n in n_invocation_files:
        trace_dict = sanitize_invocation_trace(
            azure_file_path=azure_file_path, 
            invocation_prefix=invocation_prefix, 
            n=n, 
            csv_suffix=csv_suffix, 
            max_timestep=max_timestep,
            min_timestep=min_timestep,
            max_invoke_per_time=max_invoke_per_time,
            min_invoke_per_time=min_invoke_per_time,
            max_invoke_per_func=max_invoke_per_func,
            min_invoke_per_func=min_invoke_per_func,
            iat_cv=iat_cv,
            trace_dict=trace_dict
        )

    print("Finish sampling invocation traces!")

    # # Memory traces
    # n_memory_files = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    # memory_prefix = "app_memory_percentiles.anon.d"
    
    # for n in n_memory_files:
    #     trace_dict = match_memory_trace(azure_file_path, memory_prefix, n, csv_suffix, trace_dict)

    # print("Finish matching memory traces!")

    # # Duration traces
    # n_duration_files = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14"]
    # duration_prefix = "function_durations_percentiles.anon.d"

    # for n in n_duration_files:
    #     trace_dict = match_duration_trace(azure_file_path, duration_prefix, n, csv_suffix, trace_dict)

    # print("Finish matching duration traces!")

    # # Remove incomplete traces
    # func_remove_list = []
    # for func_hash in trace_dict.keys():
    #     if "invocation_trace" not in trace_dict[func_hash] or \
    #         "memory_trace" not in trace_dict[func_hash] or \
    #         "duration_trace" not in trace_dict[func_hash]:
    #         func_remove_list.append(func_hash)

    # for func_hash in func_remove_list:
    #     trace_dict.pop(func_hash)

    return trace_dict

def sample_from_azure(
    azure_file_path,
    file_prefix,
    trace_dict,
    max_exp,
    max_timestep,
    min_timestep,
    user_config
):     
    # Adapt to experimental traces
    # print("trace_dict length: {}".format(len(trace_dict)))
    metrics_dict = {
        "func_hash": [],
        "total_iat": 0,
        "calls": 0,
        "total_duration": 0
    }

    # memory_trace_header = ["HashFunction", "SampleCount", "AverageAllocatedMb", "AverageAllocatedMb_pct1", \
    #     "AverageAllocatedMb_pct5", "AverageAllocatedMb_pct25", "AverageAllocatedMb_pct50", "AverageAllocatedMb_pct75", \
    #     "AverageAllocatedMb_pct95", "AverageAllocatedMb_pct99", "AverageAllocatedMb_pct100"]

    # duration_trace_header = ["HashFunction", "Average", "Count", "Minimum", "Maximum", "percentile_Average_0", \
    #     "percentile_Average_1", "percentile_Average_25", "percentile_Average_50", "percentile_Average_75", \
    #     "percentile_Average_99", "percentile_Average_100"]

    for i in range(max_exp):
        # func_hash_list = random.sample(list(trace_dict.keys()), k=len(user_config.keys()))
        func_hash_list = random.choices(list(trace_dict.keys()), k=len(user_config.keys()))
        random_timestep = random.randint(min_timestep, max_timestep)

        invocation_trace_csv = []
        # memory_trace_csv = []
        # duration_trace_csv = []
        total_invoke_num = 0

        invocation_trace_header = ["HashFunction", "Trigger"] + list(range(1, random_timestep+1))

        invocation_trace_csv.append(invocation_trace_header)
        # memory_trace_csv.append(memory_trace_header)
        # duration_trace_csv.append(duration_trace_header)

        for index, function_id in enumerate(user_config.keys()):
            func_hash = func_hash_list[index]
            invocation_trace = trace_dict[func_hash]["invocation_trace"][:random_timestep]
            total_invoke_num = total_invoke_num + np.sum(invocation_trace)
            
            # Record workload metrics
            if func_hash not in metrics_dict["func_hash"]:
                metrics_dict["func_hash"].append(func_hash)
            metrics_dict["calls"] = metrics_dict["calls"] + sum(invocation_trace)
            metrics_dict["total_iat"] = metrics_dict["total_iat"] + (len(invocation_trace) - 1)
            metrics_dict["total_duration"] = metrics_dict["total_duration"] + (len(invocation_trace))

            invocation_trace.insert(0, trace_dict[func_hash]["Trigger"])
            invocation_trace.insert(0, function_id)
            invocation_trace_csv.append(invocation_trace)

            # memory_trace = [function_id]
            # memory_trace_dict = trace_dict[func_hash]["memory_trace"]
            # for header in memory_trace_header:
            #     if header != "HashFunction":
            #         memory_trace.append(memory_trace_dict[header])

            # memory_trace_csv.append(memory_trace)

            # duration_trace = [function_id]
            # duration_trace_dict = trace_dict[func_hash]["duration_trace"]
            # for header in duration_trace_header:
            #     if header != "HashFunction":
            #         duration_trace.append(duration_trace_dict[header])
            # duration_trace_csv.append(duration_trace)

        with open(azure_file_path + "{}_invocation_traces_{}.csv".format(file_prefix, i), "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(invocation_trace_csv)

        # with open(azure_file_path + "{}_memory_traces_{}.csv".format(file_prefix, i), "w", newline="") as f:
        #     writer = csv.writer(f)
        #     writer.writerows(memory_trace_csv)

        # with open(azure_file_path + "{}_duration_traces_{}.csv".format(file_prefix, i), "w", newline="") as f:
        #     writer = csv.writer(f)
        #     writer.writerows(duration_trace_csv)

    with open(azure_file_path + "{}_z_metrics.csv".format(file_prefix), "w", newline="") as f:
        writer = csv.writer(f)
        metrics_dist_csv = []

        metrics_dist_csv.append(["funcs", len(metrics_dict["func_hash"])])
        metrics_dist_csv.append(["calls", metrics_dict["calls"]])
        metrics_dist_csv.append(["avg_iat", metrics_dict["total_iat"]/metrics_dict["calls"]])
        metrics_dist_csv.append(["request_per_sec", metrics_dict["calls"]/metrics_dict["total_duration"]])

        writer.writerows(metrics_dist_csv)

#
# Clean old sample files
#

def clean_old_samples(
    dir_name="azurefunctions-dataset2019/",
    file_pattern="sampled_*.csv"
):
    for file_name in glob(os.path.join(dir_name, file_pattern)):
        try:
            os.remove(file_name)
        except EnvironmentError:
            print("Require permission to {}".format(file_name))
            os.chmod(file_name, stat.S_IWRITE)
            os.remove(file_name)


if __name__ == "__main__":
    azure_file_path = "azurefunctions-dataset2019/"
    file_prefix = "cv_8.0"
    max_exp = 1
    max_timestep = 60
    min_timestep = 60
    max_invoke_per_time = 180
    min_invoke_per_time = 0
    max_invoke_per_func = 180
    min_invoke_per_func = 180
    iat_cv = 8.0

    # print("Clean old sample files...")
    # clean_old_samples(dir_name=azure_file_path, file_pattern="sampled_*.csv")

    print("Load Azure Functions traces...")
    trace_dict = get_trace_dict(
        azure_file_path=azure_file_path,
        max_timestep=max_timestep,
        min_timestep=min_timestep,
        max_invoke_per_time=max_invoke_per_time,
        min_invoke_per_time=min_invoke_per_time,
        max_invoke_per_func=max_invoke_per_func,
        min_invoke_per_func=min_invoke_per_func,
        iat_cv=iat_cv,
    )

    print("Sampling traces...")
    sample_from_azure(
        azure_file_path=azure_file_path,
        file_prefix=file_prefix,
        trace_dict=trace_dict,
        max_exp=max_exp,
        max_timestep=max_timestep,
        min_timestep=min_timestep,
        user_config=USER_CONFIG
    )
    
    print("Sampling finished!")
