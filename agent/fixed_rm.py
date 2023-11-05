from env import Environment
from logger import Logger
from utils import export_csv_request_trajectory, export_csv_container_trajectory, export_csv_container_timeline
from params import WorkloadParameters, EnvParameters
import params


def fixed_rm(
    logger_wrapper,
):
    rm = "fc_{}".format(params.EXP_PREFIX)

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
            # Record total number of events
            total_events = env.event_pq.get_total_size()

            # Reset env
            # env.cool_down_openwhisk()
            logger = logger_wrapper.get_logger(rm, False)
            observation, mask, current_timestep, current_index, current_function_id = env.reset()

            actual_time = 0
            system_time = 0
            system_runtime = 0
            reward_sum = 0

            action = None

            episode_done = False
            while episode_done is False:
                actual_time = actual_time + 1
                
                next_observation, next_mask, reward, done, info, next_timestep, next_index, next_function_id = env.step(
                    current_timestep=current_timestep,
                    current_index=current_index,
                    current_function_id=current_function_id,
                    action=action
                )

                if system_time < info["system_step"]:
                    system_time = info["system_step"]
                if system_runtime < info["system_runtime"]:
                    system_runtime = info["system_runtime"]

                reward_sum = reward_sum + reward
                
                if done:
                    if system_time < info["system_step"]:
                        system_time = info["system_step"]
                    if system_runtime < info["system_runtime"]:
                        system_runtime = info["system_runtime"]

                    timeout_num = info["timeout_num"]
                    error_num = info["error_num"]

                    logger.info("")
                    logger.info("**********")
                    logger.info("**********")
                    logger.info("**********")
                    logger.info("")
                    logger.info("Running {}".format(rm))
                    logger.info("Exp {}, Episode {} finished".format(exp_id, episode))
                    logger.info("{} actual timesteps".format(actual_time))
                    logger.info("{} system timesteps".format(system_time))
                    logger.info("{} system runtime".format(system_runtime))
                    logger.info("Total events: {}".format(total_events))
                    logger.info("Total reward: {}".format(reward_sum))
                    logger.info("Timeout num: {}".format(timeout_num))
                    logger.info("Error num: {}".format(error_num))

                    request_record = info["request_record"]
                    
                    export_csv_request_trajectory(
                        rm_name=rm,
                        exp_id=exp_id,
                        episode=episode,
                        csv_trajectory=request_record.get_csv_request_trajectory()
                    )
                    export_csv_container_trajectory(
                        rm_name=rm,
                        exp_id=exp_id,
                        episode=episode,
                        csv_trajectory=request_record.get_csv_container_trajectory(env.pool)
                    )
                    export_csv_container_timeline(
                        rm_name=rm,
                        exp_id=exp_id,
                        episode=episode,
                        csv_timeline=request_record.get_csv_container_timeline(env.pool)
                    )

                    episode_done = True
                
                observation = next_observation
                mask = next_mask
                current_timestep = next_timestep
                current_index = next_index
                current_function_id = next_function_id

            episode = episode + 1


if __name__ == "__main__":
    logger_wrapper = Logger()
    fixed_rm(logger_wrapper=logger_wrapper)
