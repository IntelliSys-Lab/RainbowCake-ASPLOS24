from env import Environment
from logger import Logger
from params import WorkloadParameters, EnvParameters, USER_CONFIG
import params


#
# Default
#

def scaling_test(
    logger_wrapper,
    size
):
    rm = "scaling_test"
    
    # Set up logger
    logger = logger_wrapper.get_logger(rm, True)

    # Start training
    episode = 0
    for exp_id in params.EXP_EVAL:
        # Set paramters for workloads
        workload_params = WorkloadParameters(
            azure_file_path=params.AZURE_FILE_PATH,
            user_defined_dict=params.USER_CONFIG,
            exp_id=exp_id
        )

        # Set paramters for Environment
        env_params = EnvParameters(
            n_invoker=params.N_INVOKER,
            redis_host=params.REDIS_HOST,
            redis_port=params.REDIS_PORT,
            redis_password=params.REDIS_PASSWORD,
            couch_link=params.COUCH_LINK,
            cool_down=params.COOL_DOWN,
            interval_limit=params.ENV_INTERVAL_LIMIT,
            update_retry_time=params.UPDATE_RETRY_TIME,
            keep_alive_time=params.KEEP_ALIVE_TIME
        )
        
        
        # Set up environment
        env = Environment(
            workload_params=workload_params,
            env_params=env_params
        )

        for episode_per_exp in range(params.MAX_EPISODE_EVAL):
            # Reset env
            # env.cool_down_openwhisk()
            logger = logger_wrapper.get_logger(rm, False)
            env.reset()

            # Scaling test
            completion_time = env.scaling_test(size)

            logger.info("")
            logger.info("**********")
            logger.info("**********")
            logger.info("**********")
            logger.info("")
            logger.info("Running {}".format(rm))
            logger.info("Exp {}, Episode {} finished".format(exp_id, episode))
            logger.info("Size {}, Completion time {}".format(size, completion_time))
                    

if __name__ == "__main__":
    logger_wrapper = Logger()
    size = 1000

    scaling_test(
        logger_wrapper=logger_wrapper,
        size=size
    )