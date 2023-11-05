import time
import json
import numpy as np
from utils import run_cmd
from params import WSK_CLI, COUCH_LINK
from logger import Logger


def invoke(function_id, param):
    try:
        cmd = '{} action invoke {} -b {} | grep -v "ok:"'.format(WSK_CLI, function_id, param)
        response = str(run_cmd(cmd))
        doc = json.loads(response)

        if len(doc["annotations"]) == 6:
            init_time = doc["annotations"][5]["value"]
        else:
            init_time = 0
        wait_time = doc["annotations"][1]["value"]
        duration = doc["duration"]
    except Exception:
        init_time = 0
        wait_time = 0
        duration = 0

    return init_time, wait_time, duration

def cold_start(interval, cold_loop_time, function_id, param):
    result = {}
    result["init_time"] = []
    result["wait_time"] = []
    result["duration"] = []
    result["completion_time"] = []

    for _ in range(cold_loop_time):
        time.sleep(interval)
        init_time, wait_time, duration = invoke(function_id, param)
        completion_time = init_time + wait_time + duration
        if completion_time > 0:
            result["init_time"].append(init_time)
            result["wait_time"].append(wait_time)
            result["duration"].append(duration)
            result["completion_time"].append(completion_time)

    time.sleep(interval)

    return result

def warm_start(interval, warm_loop_time, function_id, param):
    result = {}
    result["init_time"] = []
    result["wait_time"] = []
    result["duration"] = []
    result["completion_time"] = []

    for loop in range(warm_loop_time):
        init_time, wait_time, duration = invoke(function_id, param)
        completion_time = init_time + wait_time + duration
        if loop > 0 and completion_time > 0:
            result["init_time"].append(init_time)
            result["wait_time"].append(wait_time)
            result["duration"].append(duration)
            result["completion_time"].append(completion_time)

    time.sleep(interval)

    return result

def characterize_functions(
    interval,
    cold_loop_time,
    warm_loop_time,
    file_name, 
    function_invoke_params
):
    logger_wrapper = Logger()
    logger = logger_wrapper.get_logger(file_name, True)

    logger.debug("")
    logger.debug("**********")
    logger.debug("**********")
    logger.debug("**********")
    logger.debug("")
    logger.debug(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    for function_id in function_invoke_params.keys():
        param = function_invoke_params[function_id]
        cold_result = cold_start(interval, cold_loop_time, function_id, param)
        warm_result = warm_start(interval, warm_loop_time, function_id, param)

        # Log cold starts
        logger.debug("")
        logger.debug("{} cold start: ".format(function_id))
        logger.debug("init_time")
        logger.debug(','.join(str(init_time) for init_time in cold_result["init_time"]))
        logger.debug("wait_time")
        logger.debug(','.join(str(wait_time) for wait_time in cold_result["wait_time"]))
        logger.debug("duration")
        logger.debug(','.join(str(duration) for duration in cold_result["duration"]))
        logger.debug("completion_time")
        logger.debug(','.join(str(completion_time) for completion_time in cold_result["completion_time"]))

        # Log warm starts
        logger.debug("")
        logger.debug("{} warm start: ".format(function_id))
        logger.debug("init_time")
        logger.debug(','.join(str(init_time) for init_time in warm_result["init_time"]))
        logger.debug("wait_time")
        logger.debug(','.join(str(wait_time) for wait_time in warm_result["wait_time"]))
        logger.debug("duration")
        logger.debug(','.join(str(duration) for duration in warm_result["duration"]))
        logger.debug("completion_time")
        logger.debug(','.join(str(completion_time) for completion_time in warm_result["completion_time"]))


if __name__ == "__main__":
    interval = 15
    cold_loop_time = 10
    warm_loop_time = 11
    file_name = "workload_ideal_durations.txt"

    function_invoke_params = {}
    function_invoke_params["dh"] = "-p username hanfeiyu -p size 500000 -p parallel 128 "
    function_invoke_params["eg"] = ""
    function_invoke_params["ip"] = "-p width 1000 -p height 1000 -p size 128 -p parallel 8 "
    function_invoke_params["vp"] = "-p duration 10 -p size 1 -p parallel 1 "
    function_invoke_params["ir"] = "-p size 1 -p parallel 1 "
    function_invoke_params["knn"] = "-p dataset_size 1000 -p feature_dim 800 -p k 3 -p size 24 -p parallel 8 "
    function_invoke_params["alu"] = "-p size 100000000 -p parallel 64 "
    function_invoke_params["ms"] = "-p size 1000000 -p parallel 8 "
    function_invoke_params["gd"] = "-p x_row 20 -p x_col 20 -p w_row 40 -p size 8 -p parallel 8 "
    function_invoke_params["dv"] = "-p size 5000 -p parallel 8 "

    for function_id in function_invoke_params.keys():
        function_invoke_params[function_id] = function_invoke_params[function_id] + "-p couch_link {} -p db_name {}".format(COUCH_LINK, function_id)

    characterize_functions(
        interval=interval, 
        cold_loop_time=cold_loop_time,
        warm_loop_time=warm_loop_time,
        file_name=file_name, 
        function_invoke_params=function_invoke_params
    )

    print("")
    print("Workload characterization finished!")
    print("")
