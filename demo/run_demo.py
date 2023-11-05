import os
import sys
sys.path.append("../agent")

from logger import Logger
import params
from run_experiment import run_experiment


def mkdir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def run_demo(log_path):
    logger_wrapper = Logger(log_path=log_path)
    name = "RainbowCake"
    for exp_prefix in params.EXP_PREFIX:
        print("")
        print("Running demo trace...")
        print("")
        run_experiment(
            logger_wrapper=logger_wrapper,
            name=name,
            exp_prefix=exp_prefix,
        )

    print("")
    print("***************************")
    print("Demo experiments finished!")
    print("***************************")
    print("")


if __name__ == "__main__":
    mkdir(params.LOG_PATH)
    run_demo(params.LOG_PATH)
