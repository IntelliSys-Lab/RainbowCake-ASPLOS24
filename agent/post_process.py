import csv
import numpy as np
from params import USER_CONFIG

def process_request_trajectory(traj_filename):
    result_dict = {}
    csv_name = "{}_0_0_request_trajectory".format(traj_filename)

    with open('logs/{}.csv'.format(csv_name), newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            function_id = row["function_id"]
            init_time = int(row["init_time"])/1000
            e2e_time = (int(row["init_time"]) + int(row["wait_time"]) + int(row["duration"]))/1000
            if function_id not in result_dict.keys():
                result_dict[function_id] = {}
                result_dict[function_id]["init_time"] = []
                result_dict[function_id]["e2e_time"] = []

            result_dict[function_id]["init_time"].append(init_time)
            result_dict[function_id]["e2e_time"].append(e2e_time)

    csv_processed = [
        [
            "function_id",
            "init_avg",
            "init_std",
            "e2e_avg",
            "e2e_std",
        ]
    ]
    
    for function_id in USER_CONFIG.keys():
        csv_processed.append(
            [
                function_id,
                np.mean(result_dict[function_id]["init_time"]),
                np.std(result_dict[function_id]["init_time"]),
                np.mean(result_dict[function_id]["e2e_time"]),
                np.std(result_dict[function_id]["e2e_time"]),
            ]
        )

    with open('logs/{}_processed.csv'.format(csv_name), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(csv_processed)

if __name__ == "__main__":
    csv_name = "rc_8h"
    print("")
    print("Processing {}...".format(csv_name))
    print("")

    process_request_trajectory(csv_name)

    print("{} processing finished!".format(csv_name))
