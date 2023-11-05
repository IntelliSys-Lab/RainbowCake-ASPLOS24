import time
import redis
import couchdb
import numpy as np
import multiprocessing

from utils import SystemTime, Request, RequestRecord
from run_cmd import run_cmd
from params import WSK_CLI
from workload_generator import WorkloadGenerator


class Environment():
    """ 
    Environment for running experiments
    """

    def __init__(
        self,
        workload_params,
        env_params
    ):
        self.workload_params = workload_params
        self.env_params = env_params

        # Set up workloads
        self.workload_generator = WorkloadGenerator(
            azure_file_path=self.workload_params.azure_file_path,
            user_defined_dict=self.workload_params.user_defined_dict,
            exp_prefix=self.workload_params.exp_prefix,
            exp_id=self.workload_params.exp_id
        )
        self.profile = self.workload_generator.generate_profile()
        self.event_pq = self.workload_generator.generate_event_pq()

        # Get total number of invokers
        self.n_invoker = self.env_params.n_invoker
        
        # Set up Redis client
        self.pool = redis.ConnectionPool(
            host=self.env_params.redis_host, 
            port=self.env_params.redis_port, 
            password=self.env_params.redis_password, 
            decode_responses=True
        )
        self.redis_client = redis.Redis(connection_pool=self.pool)

        # Set up CouchDB client
        self.couch_client = couchdb.Server(self.env_params.couch_link)

        # Set up time module
        self.system_time = SystemTime(self.env_params.interval_limit)

        # Set up request record
        self.request_record = RequestRecord(self.profile.get_function_profile())

        # Misc
        self.cool_down = self.env_params.cool_down
        self.eps = np.finfo(np.float32).eps.item()

    #
    # Interactions with OpenWhisk
    #

    def invoke_openwhisk(
        self, 
        index,
        function_id
    ):
        function = self.profile.get_function_profile()[function_id]
        request_id = function.invoke_openwhisk()

        # Create corresponding request
        request = Request(
            function_id=function_id, 
            request_id=request_id, 
            index=index,
            invoke_time=self.system_time.get_system_runtime(),
        )
        self.request_record.put_requests(request)

    def end_experiment(self):
        end = self.profile.get_function_profile()["EndExperiment"]
        end.invoke_openwhisk()

    def scaling_test(self, size):
        # print("Invoke time: {}".format(time.strftime("%H:%M:%S", time.localtime())))
        manager = multiprocessing.Manager()
        result_dict = manager.dict()
        jobs = []

        per_size = int(size/(len(self.profile.get_function_profile())-1))
        for function_id in self.profile.get_function_profile().keys():
            if function_id != "EndExperiment":
                result_dict[function_id] = manager.list()
                function = self.profile.get_function_profile()[function_id]
                for _ in range(per_size):
                    p = multiprocessing.Process(
                        target=function.invoke_openwhisk_multiprocessing,
                        args=(result_dict,)
                    )
                    jobs.append(p)
                    p.start()
        
        for p in jobs:
            p.join()

        # Create corresponding requests
        counter = 0
        for function_id in result_dict.keys():
            for request_id in result_dict[function_id]:
                request = Request(
                    index=counter,
                    function_id=function_id, 
                    request_id=request_id, 
                    invoke_time=self.system_time.get_system_runtime(),
                )
                self.request_record.put_requests(request)
                counter = counter + 1

        # print("Query time: {}".format(time.strftime("%H:%M:%S", time.localtime())))
        start_time = time.time()

        # Update the requests until the end
        loop = 0
        while True:
            if self.request_record.get_undone_size() == 0:
                break

            # Try update all inflight requests
            self.try_update_request_record()
            
            # print("Loop {}, undone request size {}:".format(loop, self.request_record.get_undone_size()))
            loop = loop + 1

        end_time = time.time()
        # print("End time: {}".format(time.strftime("%H:%M:%S", time.localtime())))

        return end_time - start_time

    def try_update_request_record(self):
        function_profile = self.profile.get_function_profile()
        manager = multiprocessing.Manager()
        result_dict = manager.dict()
        jobs = []

        for function_id in function_profile.keys():
            result_dict[function_id] = manager.dict()
            for request in self.request_record.get_undone_request_record_per_function(function_id):
                request_id = request.get_request_id()
                result_dict[function_id][request_id] = manager.dict()
                result_dict[function_id][request_id]["is_done"] = False
                
                p = multiprocessing.Process(
                    target=request.try_update,
                    args=(
                        result_dict, 
                        self.system_time.get_system_runtime(), 
                        self.env_params.couch_link,
                    )
                )
                jobs.append(p)
                p.start()
        
        for p in jobs:
            p.join()

        # Update requests according to the result dict
        done_request_list = []

        for function_id in result_dict.keys():
            for request_id in result_dict[function_id].keys():
                request = self.request_record.request_record_per_request[request_id]
                result = result_dict[function_id][request_id]

                # Check if done
                is_done = result["is_done"] 
                if is_done is True:
                    done_time = result["done_time"]
                    init_time = result["init_time"]
                    wait_time = result["wait_time"]
                    duration = result["duration"]
                    is_timeout = result["is_timeout"]
                    is_success = result["is_success"]
                    is_cold_start = result["is_cold_start"]
                    hit_level = result["hit_level"]
                    user_time = result["user_time"]
                    lang_time = result["lang_time"]
                    bare_time = result["bare_time"]

                    # Set updates for done requests
                    request.set_updates(
                        is_done=is_done,
                        done_time=done_time,
                        is_timeout=is_timeout,
                        is_success=is_success,
                        init_time=init_time,
                        wait_time=wait_time,
                        duration=duration,
                        is_cold_start=is_cold_start,
                        hit_level=hit_level,
                        user_time=user_time,
                        lang_time=lang_time,
                        bare_time=bare_time
                    )
                    done_request_list.append(request)

        # Update request records
        self.request_record.update_requests(done_request_list)

    def get_n_undone_request(self):
        try:
            n_undone_request = int(self.redis_client.get("n_undone_request"))
        except Exception as e:
            # print("Exception [{}] occurred when querying n_undone_request!".format(e))
            n_undone_request = 0

        return n_undone_request

    def cool_down_openwhisk(self, sh_name):
        cmd = "./{}.sh".format(sh_name)
        run_cmd(cmd)
        time.sleep(10)

    def get_mask(self):
        pass

    def get_observation(self, next_function_id):
        pass

    def get_reward(self):
        pass

    def get_done(self):
        if self.event_pq.is_empty() is True and \
        self.system_time.get_system_step() > self.event_pq.get_max_timestep() - 1 and \
        self.get_n_undone_request() == 0:
            return True
        else:
            return False

    def get_info(self, done):
        info = {
            "system_step": self.system_time.get_system_step(),
            "system_runtime": self.system_time.get_system_runtime(),
            "n_undone_request": self.get_n_undone_request(),
            "request_record": self.request_record,
        }

        if done is True:
            info["timeout_num"] = self.request_record.get_timeout_size()
            info["error_num"] = self.request_record.get_error_size()

        return info

    def step(
        self, 
        current_timestep,
        current_index,
        current_function_id,
        action,
    ):
        # Get next event
        if self.event_pq.is_empty() is False:
            # Events proceed
            if self.system_time.get_system_step() < current_timestep:
                self.system_time.step(current_timestep - self.system_time.get_system_step())

            # Invoke functions according to the given function
            self.invoke_openwhisk(current_index, current_function_id)

            # Get next event
            next_timestep, next_index, next_function_id = self.event_pq.get_event()

            done = False
        else:
            # print("Entering the last timestamp...")
            # Invoke the last event
            self.invoke_openwhisk(current_index, current_function_id)

            # Stop prewarming events
            self.end_experiment()

            # Proceed to the end
            if self.system_time.get_system_step() < self.event_pq.get_max_timestep():
                self.system_time.step(self.event_pq.get_max_timestep() -  self.system_time.get_system_step())
            
            # Wait until no inflight requests
            while True:
                if self.get_done() is True:
                    break

            # Make sure all containers are terminated
            self.end_experiment()

            # Wait until OpenWhisk terminates all idle containers
            # time.sleep(self.env_params.keep_alive_time)

            retry = 0
            while retry < self.env_params.update_retry_time:
                if self.request_record.get_undone_size() == 0:
                    break

                # Try update all inflight requests
                self.try_update_request_record()
                retry = retry + 1
                # print("Try update all requests with attempt {}".format(retry))

            self.request_record.label_all_undone_error(self.system_time.get_system_runtime())

            next_timestep = None
            next_index = None
            next_function_id = None
            # print("Leaving the last timestamp...")

            done = True
        
        observation = None
        mask = None
        reward = 0

        # Return information
        info = self.get_info(done)

        return observation, mask, reward, done, info, next_timestep, next_index, next_function_id

    def reset(self):
        self.system_time.reset()
        self.profile.reset()
        self.event_pq.reset()
        self.request_record.reset()
        self.redis_client.flushall()
        
        next_timestep, next_index, next_function_id = self.event_pq.get_event()
        observation = None
        mask = None
        
        return observation, mask, next_timestep, next_index, next_function_id
