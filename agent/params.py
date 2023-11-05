import os
from pathlib import Path
from run_cmd import run_cmd

#
# Global variables
#

# Environment parameters
HOME = str(Path.home())
WSK_CLI = "wsk -i"
N_INVOKER = int(run_cmd('cat ../ansible/environments/distributed/hosts | grep "invoker" | grep -v "\[invokers\]" | wc -l'))
REDIS_HOST = run_cmd('cat ../ansible/environments/distributed/hosts | grep -A 1 "\[edge\]" | grep "ansible_host" | awk {}'.format("{'print $1'}"))
REDIS_PORT = 6379
REDIS_PASSWORD = "openwhisk"
COUCH_PROTOCOL = "http"
COUCH_USER = "whisk_admin"
COUCH_PASSWORD = "some_passw0rd"
COUCH_HOST = run_cmd('cat ../ansible/environments/distributed/hosts | grep -A 1 "\[db\]" | grep "ansible_host" | awk {}'.format("{'print $1'}"))
COUCH_PORT = "5984"
COUCH_LINK = "{}://{}:{}@{}:{}/".format(COUCH_PROTOCOL, COUCH_USER, COUCH_PASSWORD, COUCH_HOST, COUCH_PORT)
COOL_DOWN = "refresh_all"
ENV_INTERVAL_LIMIT = 1
UPDATE_RETRY_TIME = 10
KEEP_ALIVE_TIME = 600 # second
LOG_PATH = os.path.dirname(os.getcwd())+'/demo/logs/'

# Training parameters
# AZURE_FILE_PATH = "azurefunctions-dataset2019/"
AZURE_FILE_PATH = "../demo/azurefunctions-dataset2019/"
EXP_PREFIX = [
    "demo"
    # "cv_8.0",
    # "cv_4.0",
    # "cv_2.0",
    # "cv_1.0",
    # "cv_0.8",
    # "cv_0.6",
    # "cv_0.4",
    # "cv_0.2", 
    # "8h", 
    # "mem_40",
    # "mem_80",
    # "mem_120",
    # "mem_160",
    # "mem_200",
]
TRACE_FILE_SUFFIX = ".csv"
EXP_TRAIN = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
EXP_EVAL = [0]
USER_CONFIG = {
    "ac": {"lang": "nodejs", "memory": 1024},
    "dh": {"lang": "nodejs", "memory": 1024},
    "ul": {"lang": "nodejs", "memory": 1024},
    "is": {"lang": "nodejs", "memory": 1024},
    "tn": {"lang": "nodejs", "memory": 1024},
    "oi": {"lang": "nodejs", "memory": 1024},
    "dv": {"lang": "python", "memory": 1024},
    "gb": {"lang": "python", "memory": 1024},
    "gm": {"lang": "python", "memory": 1024},
    "gp": {"lang": "python", "memory": 1024},
    "ir": {"lang": "python", "memory": 1024},
    "sa": {"lang": "python", "memory": 1024},
    "fc": {"lang": "python", "memory": 1024},
    "md": {"lang": "python", "memory": 1024},
    "vp": {"lang": "python", "memory": 1024},
    "dt": {"lang": "java", "memory": 1024},
    "dl": {"lang": "java", "memory": 1024},
    "dq": {"lang": "java", "memory": 1024},
    "ds": {"lang": "java", "memory": 1024},
    "dg": {"lang": "java", "memory": 1024},
}
MAX_EPISODE_TRAIN = 50
MAX_EPISODE_EVAL = 1
STATE_DIM = 11
ACTION_DIM = 1
HIDDEN_DIMS = [32, 16]
LEARNING_RATE = 1e-3
DISCOUNT_FACTOR = 1
PPO_CLIP = 0.2
PPO_EPOCH = 4
VALUE_LOSS_COEF = 0.5
ENTROPY_COEF = 0.01
MODEL_SAVE_PATH = "ckpt/"
MODEL_NAME = "best_model.ckpt"
        
#
# Parameter classes
#

class EnvParameters():
    """
    Parameters used for generating Environment
    """
    def __init__(
        self,
        n_invoker,
        redis_host,
        redis_port,
        redis_password,
        couch_link,
        cool_down,
        interval_limit,
        update_retry_time,
        keep_alive_time
    ):
        self.n_invoker = n_invoker
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_password = redis_password
        self.couch_link = couch_link
        self.cool_down = cool_down
        self.interval_limit = interval_limit
        self.update_retry_time = update_retry_time
        self.keep_alive_time = keep_alive_time

class WorkloadParameters():
    """
    Parameters used for workload configuration
    """
    def __init__(
        self,
        azure_file_path,
        user_defined_dict,
        exp_prefix,
        exp_id
    ):
        self.azure_file_path = azure_file_path
        self.user_defined_dict = user_defined_dict
        self.exp_prefix = exp_prefix
        self.exp_id = exp_id

class FunctionParameters():
    """
    Parameters used for generating Function
    """
    def __init__(
        self,
        function_id,
        lang,
        memory
    ):
        self.function_id = function_id
        self.lang = lang
        self.memory = memory

class EventPQParameters():
    """
    Parameters used for generating EventPQ
    """
    def __init__(
        self,
        azure_invocation_traces
    ):
        self.azure_invocation_traces = azure_invocation_traces
