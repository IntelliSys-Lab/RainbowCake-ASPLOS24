import time
import numpy as np
import copy as cp
import heapq
import couchdb
import redis
import csv

import params
from run_cmd import run_cmd


#
# Class utilities
#

class Function():
    """
    Function wrapper
    """
    
    def __init__(self, params):
        self.params = params
        self.function_id = self.params.function_id

    def get_function_id(self):
        return self.function_id
    
    def invoke_openwhisk(self):
        cmd = '{} action invoke {}'.format(params.WSK_CLI, self.function_id)
        result = str(run_cmd(cmd))
        request_id = result.split(" ")[-1]
        if request_id == "":
            request_id = result
        return request_id

    def invoke_openwhisk_multiprocessing(self, result_dict):
        cmd = '{} action invoke {}'.format(params.WSK_CLI, self.function_id)
        result = str(run_cmd(cmd))
        request_id = result.split(" ")[-1]
        if request_id == "":
            request_id = result
        result_dict[self.function_id].append(request_id)


class Request():
    """
    An invocation of a function
    """
    def __init__(
        self, 
        function_id,
        request_id,
        index,
        invoke_time,
    ):
        self.function_id = function_id
        self.request_id = request_id
        self.index = index
        self.invoke_time = invoke_time

        self.is_done = False
        self.done_time = 0
        self.init_time = 0
        self.wait_time = 0
        self.duration = 0
        self.is_timeout = False
        self.is_success = False
        self.is_cold_start = False

        # Multi-level details
        self.hit_level = None
        self.user_time = 0
        self.lang_time = 0
        self.bare_time = 0

    def get_function_id(self):
        return self.function_id

    def get_request_id(self):
        return self.request_id

    def get_index(self):
        return self.index

    def get_is_done(self):
        return self.is_done

    def get_invoke_time(self):
        return self.invoke_time

    def get_done_time(self):
        return self.done_time

    def get_init_time(self):
        return self.init_time

    def get_wait_time(self):
        return self.wait_time

    def get_duration(self):
        return self.duration

    def get_completion_time(self):
        return (self.init_time + self.wait_time + self.duration)

    def get_is_timeout(self):
        return self.is_timeout

    def get_is_success(self):
        return self.is_success
        
    def get_is_cold_start(self):
        return self.is_cold_start

    def get_hit_level(self):
        return self.hit_level

    def get_user_time(self):
        return self.user_time

    def get_lang_time(self):
        return self.lang_time

    def get_bare_time(self):
        return self.bare_time

    # Multiprocessing
    def try_update(self, result_dict, system_runtime, couch_link):
        couch_client = couchdb.Server(couch_link)
        couch_activations = couch_client["whisk_distributed_activations"]
        doc = couch_activations.get("guest/{}".format(self.request_id))
        result = result_dict[self.function_id][self.request_id]

        if doc is not None: 
            try:
                # print("Request {} doc ready!".format(self.request_id))
                result["is_done"] = True
                result["done_time"] = system_runtime
                result["wait_time"] = doc["annotations"][1]["value"]
                result["duration"] = doc["duration"]
                if doc["response"]["statusCode"] == 0:
                    result["is_success"] = True
                else:
                    result["is_success"] = False

                result["is_timeout"] = doc["annotations"][3]["value"]
                
                if len(doc["annotations"]) == 6:
                    result["is_cold_start"] = True
                    result["init_time"] = doc["annotations"][5]["value"]
                else:
                    result["is_cold_start"] = False
                    result["init_time"] = 0 

                result["hit_level"] = doc["logs"][0]
                result["user_time"] = int(doc["logs"][1])
                result["lang_time"] = int(doc["logs"][2])
                result["bare_time"] = int(doc["logs"][3])
            except Exception as e:
                # print("Exception {} occurred when processing request {}".format(e, self.request_id))
                result["is_done"] = False
        else:
            # print("Request {} doc not ready yet".format(self.request_id))
            result["is_done"] = False

    def set_updates(
        self, 
        is_done,
        done_time,
        is_timeout,
        is_success,
        init_time,
        wait_time,
        duration,
        is_cold_start,
        hit_level,
        user_time,
        lang_time,
        bare_time
    ):
        self.is_done = is_done
        self.done_time = done_time
        self.is_timeout = is_timeout
        self.is_success = is_success
        self.init_time = init_time
        self.wait_time = wait_time
        self.duration = duration
        self.is_cold_start = is_cold_start
        self.hit_level = hit_level
        self.user_time = user_time
        self.lang_time = lang_time
        self.bare_time = bare_time


class RequestRecord():
    """
    Recording of requests both in total and per Function
    """

    def __init__(self, function_profile):
        # General records
        self.total_request_record = []
        self.success_request_record = []
        self.undone_request_record = []
        self.timeout_request_record = []
        self.error_request_record = []

        # Records hashed by function id
        self.total_request_record_per_function = {}
        self.success_request_record_per_function = {}
        self.undone_request_record_per_function = {}
        self.timeout_request_record_per_function = {}
        self.error_request_record_per_function = {}

        for function_id in function_profile.keys():
            self.total_request_record_per_function[function_id] = []
            self.success_request_record_per_function[function_id] = []
            self.undone_request_record_per_function[function_id] = []
            self.timeout_request_record_per_function[function_id] = []
            self.error_request_record_per_function[function_id] = []

        # Records hashed by request id
        self.request_record_per_request = {}

    def put_requests(self, request):
        function_id = request.get_function_id()
        request_id = request.get_request_id()
        self.total_request_record.append(request)
        self.total_request_record_per_function[function_id].append(request)
        self.undone_request_record.append(request)
        self.undone_request_record_per_function[function_id].append(request)
        self.request_record_per_request[request_id] = request

    def update_requests(self, done_request_list):
        for request in done_request_list:
            function_id = request.get_function_id()

            if request.get_is_success() is True: # Success?
                self.success_request_record.append(request)
                self.success_request_record_per_function[function_id].append(request)
            else: # Not success
                if request.get_is_timeout() is True: # Timeout?
                    self.timeout_request_record.append(request)
                    self.timeout_request_record_per_function[function_id].append(request)
                else: # Error
                    self.error_request_record.append(request)
                    self.error_request_record_per_function[function_id].append(request)

            self.undone_request_record.remove(request)
            self.undone_request_record_per_function[function_id].remove(request)

    def label_all_undone_error(self, done_time):
        done_request_list = []
        for request in self.undone_request_record:
            request.set_updates(
                is_done=False,
                done_time=done_time,
                is_timeout=False,
                is_success=False,
                init_time=60,
                wait_time=60,
                duration=60,
                is_cold_start=True,
                hit_level=None,
                user_time=0,
                lang_time=0,
                bare_time=0
            )
            done_request_list.append(request)
        
        self.update_requests(done_request_list)

    def get_couch_key_list(self):
        key_list = []
        for request in self.undone_request_record:
            key_list.append("guest/{}".format(request.get_request_id()))

        return key_list

    def get_last_n_done_request_per_function(self, function_id, n):
        last_n_request = []

        for request in reversed(self.total_request_record_per_function[function_id]):
            if request.get_is_done() is True:
                last_n_request.append(request)
            
            if len(last_n_request) == n:
                break

        return last_n_request

    def get_total_size(self):
        return len(self.total_request_record)

    def get_undone_size(self):
        return len(self.undone_request_record)

    def get_success_size(self):
        return len(self.success_request_record)

    def get_timeout_size(self):
        return len(self.timeout_request_record)
        
    def get_error_size(self):
        return len(self.error_request_record)

    def get_avg_completion_time(self):
        request_num = 0
        total_completion_time = 0

        for request in self.success_request_record:
            request_num = request_num + 1
            total_completion_time = total_completion_time + request.get_completion_time()
        # for request in self.timeout_request_record:
        #     request_num = request_num + 1
        #     total_completion_time = total_completion_time + request.get_completion_time()
        # for request in self.error_request_record:
        #     request_num = request_num + 1
        #     total_completion_time = total_completion_time + request.get_completion_time()
        
        if request_num == 0:
            avg_completion_time = 0
        else:
            avg_completion_time = total_completion_time / request_num

        return avg_completion_time

    def get_total_init_time(self):
        total_init_time = 0

        for request in self.success_request_record:
            total_init_time = total_init_time + request.get_init_time()

        return total_init_time

    def get_total_level_time(self):
        total_level_time = 0

        for request in self.success_request_record:
            total_level_time = total_level_time + request.get_user_time() + request.get_lang_time() + request.get_bare_time()

        return total_level_time

    def get_avg_interval(self):
        total_interval = 0
        num = 0
        for i, request in enumerate(self.total_request_record):
            if i < len(self.total_request_record) - 1:
                next_request = self.total_request_record[i+1]
                interval = next_request.get_invoke_time() - request.get_invoke_time()

                if interval > 0:
                    total_interval = total_interval + interval
                    num = num + 1

        if num == 0:
            avg_interval = 0
        else:
            avg_interval = total_interval / num

        return avg_interval

    def get_default_hit_level_num(self):
        default_warm_num = 0
        default_cold_num = 0

        for request in self.success_request_record:
            if request.get_is_cold_start() is True:
                default_cold_num = default_cold_num + 1
            else:
                default_warm_num = default_warm_num + 1
        
        return default_warm_num, default_cold_num

    def get_hit_level_num(self):
        user_num = 0
        lang_num = 0
        bare_num = 0
        cold_num = 0

        for request in self.success_request_record:
            if request.get_hit_level() == "user":
                user_num = user_num + 1
            elif request.get_hit_level() == "lang":
                lang_num = lang_num + 1
            elif request.get_hit_level() == "bare":
                bare_num = bare_num + 1
            else:
                cold_num = cold_num + 1
        
        return user_num, lang_num, bare_num, cold_num

    def get_csv_request_trajectory(self):
        csv_trajectory = []
        header = [
            "index", 
            "invoke_time", 
            "request_id", 
            "function_id", 
            "success", 
            "timeout", 
            "hit_level",
            "init_time", 
            "wait_time", 
            "duration", 
            "user_time", 
            "lang_time", 
            "bare_time"
        ]
        csv_trajectory.append(header)
        for request in self.total_request_record:
            if request.get_is_done() is True and request.get_request_id() != "":
                csv_trajectory.append(
                    [
                        request.get_index(),
                        int(request.get_invoke_time()/60),
                        request.get_request_id(),
                        request.get_function_id(),
                        request.get_is_success(),
                        request.get_is_timeout(),
                        request.get_hit_level(), 
                        request.get_init_time(),
                        request.get_wait_time(),
                        request.get_duration(),
                        request.get_user_time(), 
                        request.get_lang_time(),
                        request.get_bare_time()
                    ]
                )

        return csv_trajectory

    def get_csv_request_timeline(self):
        csv_timeline = []
        header = [
            "invoke_time", 
            "total_init_time", 
            "total_wait_time", 
            "total_duration", 
            "num_user",
            "num_lang",
            "num_bare",
            "num_cold",
            "total_user_time", 
            "total_lang_time", 
            "total_bare_time", 
        ]
        csv_timeline.append(header)

        timeline_dict = {}
        for request in self.total_request_record:
            if request.get_is_done() is True and request.get_request_id() != "":
                invoke_time = int(request.get_invoke_time()/60)
                if invoke_time not in timeline_dict.keys():
                    timeline_dict[invoke_time] = {}
                    timeline_dict[invoke_time]["total_init_time"] = 0
                    timeline_dict[invoke_time]["total_wait_time"] = 0
                    timeline_dict[invoke_time]["total_duration"] = 0
                    timeline_dict[invoke_time]["num_user"] = 0
                    timeline_dict[invoke_time]["num_lang"] = 0
                    timeline_dict[invoke_time]["num_bare"] = 0
                    timeline_dict[invoke_time]["num_cold"] = 0
                    timeline_dict[invoke_time]["total_user_time"] = 0
                    timeline_dict[invoke_time]["total_lang_time"] = 0
                    timeline_dict[invoke_time]["total_bare_time"] = 0

                timeline_dict[invoke_time]["total_init_time"] = timeline_dict[invoke_time]["total_init_time"] + request.get_init_time()
                timeline_dict[invoke_time]["total_wait_time"] = timeline_dict[invoke_time]["total_wait_time"] + request.get_wait_time()
                timeline_dict[invoke_time]["total_duration"] = timeline_dict[invoke_time]["total_duration"] + request.get_duration()
                
                if request.get_hit_level() == "user":
                    timeline_dict[invoke_time]["num_user"] = timeline_dict[invoke_time]["num_user"] + 1
                elif request.get_hit_level() == "lang":
                    timeline_dict[invoke_time]["num_lang"] = timeline_dict[invoke_time]["num_lang"] + 1
                elif request.get_hit_level() == "bare":
                    timeline_dict[invoke_time]["num_bare"] = timeline_dict[invoke_time]["num_bare"] + 1
                else:
                    timeline_dict[invoke_time]["num_cold"] = timeline_dict[invoke_time]["num_cold"] + 1

                timeline_dict[invoke_time]["total_user_time"] = timeline_dict[invoke_time]["total_user_time"] + request.get_user_time()
                timeline_dict[invoke_time]["total_lang_time"] = timeline_dict[invoke_time]["total_lang_time"] + request.get_lang_time()
                timeline_dict[invoke_time]["total_bare_time"] = timeline_dict[invoke_time]["total_bare_time"] + request.get_bare_time()

        min_time = min(timeline_dict.keys())
        max_time = max(timeline_dict.keys())

        for i in range(max_time - min_time + 1):
            invoke_time = min_time + i
            if invoke_time in timeline_dict.keys():
                total_init_time = timeline_dict[invoke_time]["total_init_time"]
                total_wait_time = timeline_dict[invoke_time]["total_wait_time"]
                total_duration = timeline_dict[invoke_time]["total_duration"]
                num_user = timeline_dict[invoke_time]["num_user"]
                num_lang = timeline_dict[invoke_time]["num_lang"]
                num_bare = timeline_dict[invoke_time]["num_bare"]
                num_cold = timeline_dict[invoke_time]["num_cold"]
                total_user_time = timeline_dict[invoke_time]["total_user_time"]
                total_lang_time = timeline_dict[invoke_time]["total_lang_time"]
                total_bare_time = timeline_dict[invoke_time]["total_bare_time"]
            else:
                total_init_time = 0
                total_wait_time = 0
                total_duration = 0
                num_user = 0
                num_lang = 0
                num_bare = 0
                num_cold = 0
                total_user_time = 0
                total_lang_time = 0
                total_bare_time = 0

            csv_timeline.append(
                [
                    i + 1,
                    total_init_time,
                    total_wait_time,
                    total_duration,
                    num_user,
                    num_lang,
                    num_bare,
                    num_cold,
                    total_user_time,
                    total_lang_time,
                    total_bare_time,
                ]
            )
        
        return csv_timeline

    def get_csv_container_trajectory(self, pool):
        redis_client = redis.Redis(connection_pool=pool)
        traj_dict = redis_client.hgetall("wasted_memory_time")

        csv_trajectory = []
        header = [
            "index", 
            "container_id", 
            "user_used", 
            "user_dead", 
            "lang_used", 
            "lang_dead", 
            "bare_used",
            "bare_dead",
        ]
        csv_trajectory.append(header)
        index = 1
        for container_id in traj_dict.keys():
            traj = [index, container_id]
            levels = traj_dict[container_id].split(";")
            for level in levels:
                memory_time_used = 0
                memory_time_dead = 0
                if level != "":
                    segs = level.split(" ")
                    for seg in segs:
                        memory = int(float(seg.split(",")[0]))
                        start = int(seg.split(",")[1])
                        end = int(seg.split(",")[2])
                        status = str(seg.split(",")[3])
                        if status == "used":
                            memory_time_used = memory_time_used + memory*(end - start)/1000
                        else:
                            memory_time_dead = memory_time_dead + memory*(end - start)/1000
                        
                traj.append(memory_time_used)
                traj.append(memory_time_dead)

            csv_trajectory.append(traj)
            index = index + 1

        return csv_trajectory

    def get_csv_container_timeline(self, pool):
        redis_client = redis.Redis(connection_pool=pool)
        traj_dict = redis_client.hgetall("wasted_memory_time")

        timeline_dict = {}

        for container_id in traj_dict.keys():
            levels = traj_dict[container_id].split(";")
            for level in levels:
                if level != "":
                    segs = level.split(" ")
                    for seg in segs:
                        memory = int(float(seg.split(",")[0]))
                        start = int(seg.split(",")[1])
                        end = int(seg.split(",")[2])
                        status = str(seg.split(",")[3])
                        period = int((end - start)/1000/60) + 1 # avoid 0
                        for i in range(period):
                            timestamp = int(start/1000/60) + i
                            if timestamp not in timeline_dict.keys():
                                timeline_dict[timestamp] = {}
                                timeline_dict[timestamp]["num_idle"] = 0
                                timeline_dict[timestamp]["mem_used"] = 0
                                timeline_dict[timestamp]["mem_dead"] = 0
                            
                            timeline_dict[timestamp]["num_idle"] = timeline_dict[timestamp]["num_idle"] + 1
                            if status == "used":
                                timeline_dict[timestamp]["mem_used"] = timeline_dict[timestamp]["mem_used"] + memory
                            else:
                                timeline_dict[timestamp]["mem_dead"] = timeline_dict[timestamp]["mem_dead"] + memory
                            
        csv_timeline = []
        header = [
            "timestamp", 
            "num_idle",
            "mem_used",
            "mem_dead",
        ]
        csv_timeline.append(header)

        if len(timeline_dict.keys()) > 0:
            min_time = min(timeline_dict.keys())
            max_time = max(timeline_dict.keys())
            for i in range(max_time - min_time + 1):
                timestamp = min_time + i
                if timestamp in timeline_dict.keys():
                    num_idle = timeline_dict[timestamp]["num_idle"]
                    mem_used = timeline_dict[timestamp]["mem_used"]
                    mem_dead = timeline_dict[timestamp]["mem_dead"]
                else:
                    num_idle = 0
                    mem_used = 0
                    mem_dead = 0

                csv_timeline.append(
                    [
                        i + 1,
                        num_idle,
                        mem_used,
                        mem_dead,
                    ]
                )

        return csv_timeline

    def get_total_size_per_function(self, function_id):
        return len(self.total_request_record_per_function[function_id])

    def get_undone_size_per_function(self, function_id):
        return len(self.undone_request_record_per_function[function_id])

    def get_success_size_per_function(self, function_id):
        return len(self.success_request_record_per_function[function_id])

    def get_timeout_size_per_function(self, function_id):
        return len(self.timeout_request_record_per_function[function_id])

    def get_total_request_record(self):
        return self.total_request_record

    def get_success_request_record(self):
        return self.success_request_record

    def get_undone_request_record(self):
        return self.undone_request_record

    def get_timeout_request_record(self):
        return self.timeout_request_record

    def get_error_request_record(self):
        return self.error_request_record

    def get_total_request_record_per_function(self, function_id):
        return self.total_request_record_per_function[function_id]

    def get_success_request_record_per_function(self, function_id):
        return self.success_request_record_per_function[function_id]

    def get_undone_request_record_per_function(self, function_id):
        return self.undone_request_record_per_function[function_id]

    def get_timeout_request_record_per_function(self, function_id):
        return self.timeout_request_record_per_function[function_id]

    def get_error_request_record_per_function(self, function_id):
        return self.error_request_record_per_function[function_id]

    def get_request_per_request(self, request_id):
        return self.request_record_per_request[request_id]

    def reset(self):
        self.total_request_record = []
        self.success_request_record = []
        self.undone_request_record = []
        self.timeout_request_record = []
        self.error_request_record = []

        for function_id in self.total_request_record_per_function.keys():
            self.total_request_record_per_function[function_id] = []
            self.success_request_record_per_function[function_id] = []
            self.undone_request_record_per_function[function_id] = []
            self.timeout_request_record_per_function[function_id] = []
            self.error_request_record_per_function[function_id] = []

        self.request_record_per_request = {}


class Profile():
    """
    Record settings of functions
    """
    
    def __init__(self, function_profile):
        self.function_profile = function_profile
        self.default_function_profile = cp.deepcopy(function_profile)

    def put_function(self, function):
        function_id = function.get_function_id()
        self.function_profile[function_id] = function
    
    def get_size(self):
        return len(self.function_profile)

    def get_function_profile(self):
        return self.function_profile

    def reset(self):
        self.function_profile = cp.deepcopy(self.default_function_profile)
        
        
class EventPQ():
    """
    A priority queue of events, dictates which and when function will be invoked
    """
    
    def __init__(
        self, 
        pq,
        max_timestep
    ):
        self.pq = pq
        self.default_pq = cp.deepcopy(pq)
        self.max_timestep = max_timestep
        
    def get_event(self):
        if self.is_empty() is True:
            return None, None, None
        else:
            (timestep, counter, function_id) = heapq.heappop(self.pq)
            return timestep, counter, function_id
    
    def get_current_size(self):
        return len(self.pq)

    def get_total_size(self):
        return len(self.default_pq)

    def get_max_timestep(self):
        return self.max_timestep

    def is_empty(self):
        if len(self.pq) == 0:
            return True
        else:
            return False

    def reset(self):
        self.pq = cp.deepcopy(self.default_pq)
    

class SystemTime():
    """
    Time module wrapper
    """
    
    def __init__(self, interval_limit=1):
        self.system_up_time = time.time()
        self.system_runtime = 0
        self.system_step = 0

        self.interval_limit = interval_limit

    def get_system_up_time(self):
        return self.system_up_time

    def get_system_runtime(self):
        return self.system_runtime

    def get_system_step(self):
        return self.system_step

    def update_runtime(self, increment):
        old_runtime = self.system_runtime
        target_runtime = self.system_runtime + increment
        while self.system_runtime <= target_runtime:  
            self.system_runtime = time.time() - self.system_up_time
        return self.system_runtime - old_runtime

    def step(self, increment):
        self.system_step = self.system_step + increment
        return self.update_runtime(increment)

    def reset(self):
        self.system_up_time = time.time()
        self.system_runtime = 0
        self.system_step = 0

#
# Log utilities
#

def export_csv_request_trajectory(
    rm_name,
    exp_id,
    episode,
    csv_trajectory
):
    file_path = params.LOG_PATH
    file_name = "{}_{}_{}_request_trajectory.csv".format(rm_name, exp_id, episode)

    with open(file_path + file_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(csv_trajectory)

def export_csv_container_trajectory(
    rm_name,
    exp_id,
    episode,
    csv_trajectory
):
    file_path = params.LOG_PATH
    file_name = "{}_{}_{}_container_trajectory.csv".format(rm_name, exp_id, episode)

    with open(file_path + file_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(csv_trajectory)

def export_csv_container_timeline(
    rm_name,
    exp_id,
    episode,
    csv_timeline
):
    file_path = params.LOG_PATH
    file_name = "{}_{}_{}_container_timeline.csv".format(rm_name, exp_id, episode)

    with open(file_path + file_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(csv_timeline)

def export_csv_request_timeline(
    rm_name,
    exp_id,
    episode,
    csv_timeline
):
    file_path = params.LOG_PATH
    file_name = "{}_{}_{}_request_timeline.csv".format(rm_name, exp_id, episode)

    with open(file_path + file_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(csv_timeline)
