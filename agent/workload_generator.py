import pandas as pd
import heapq
import itertools
from utils import Function, Profile, EventPQ
from params import FunctionParameters, EventPQParameters, TRACE_FILE_SUFFIX


class WorkloadGenerator():
    """
    Generate workfloads
    """
    def __init__(
        self,
        user_defined_dict,
        exp_prefix,
        exp_id,
        azure_file_path="../experiments/traces/azurefunctions-dataset2019/"
    ):
        self.azure_file_path = azure_file_path
        self.user_defined_dict = user_defined_dict
        self.exp_prefix = exp_prefix
        self.exp_id = exp_id

    def generate_profile_params(self):
        profile_params = {}
        for function_id in self.user_defined_dict.keys():
            param = FunctionParameters(
                function_id=function_id,
                lang=self.user_defined_dict[function_id]["lang"],
                memory=self.user_defined_dict[function_id]["memory"]
            )
            
            profile_params[function_id] = param

        return profile_params

    def generate_event_pq_params(self):
        invocation_traces = pd.read_csv(self.azure_file_path + "{}_invocation_traces_".format(self.exp_prefix) + str(self.exp_id) + TRACE_FILE_SUFFIX)
        event_pq_params = EventPQParameters(azure_invocation_traces=invocation_traces)
        return event_pq_params

    def generate_profile(self):
        function_profile = {}
        profile_params = self.generate_profile_params()
        
        # Parameters of functions
        for function_id in profile_params.keys():
            param = profile_params[function_id]
            function = Function(param)

            # Add to profile
            function_profile[function_id] = function

        # Create and register no-op function
        function_profile["EndExperiment"] = Function(
            FunctionParameters(
                function_id="EndExperiment",
                lang="python",
                memory=1024
            )
        )
        
        profile = Profile(function_profile=function_profile)

        return profile
    
    def generate_event_pq(self):
        event_pq_params = self.generate_event_pq_params()
        invocation_traces = event_pq_params.azure_invocation_traces
        max_timestep = len(invocation_traces.columns) - 2

        pq = []
        counter = itertools.count()
        for timestep in range(max_timestep):
            for _, row in invocation_traces.iterrows():
                function_id = row["HashFunction"]
                invoke_num = row["{}".format(timestep+1)]
                for i in range(invoke_num):
                    index = next(counter)
                    # t = timestep * 60 + int(60/invoke_num)*i
                    t = timestep * 60
                    heapq.heappush(pq, (t, index, function_id))
                    # heapq.heappush(pq, (timestep, index, function_id))

            # if timestep > 0:
            #     break

        event_pq = EventPQ(pq=pq, max_timestep=max_timestep)
        print("event_pq:")
        print(pq)
        return event_pq
    