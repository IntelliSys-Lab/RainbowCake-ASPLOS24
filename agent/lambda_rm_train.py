import numpy as np
import torch

from env import Environment
from logger import Logger
from utils import log_trends
from ppo2_agent import PPO2Agent
from params import WorkloadParameters, EnvParameters
import params


#
# Policy gradient training
#

def lambda_rm_train(logger_wrapper):
    rm = "lambda_rm_train"

    # Set up logger
    logger = logger_wrapper.get_logger(rm, True)

    # Set up policy gradient agent
    agent = PPO2Agent(
        observation_dim=params.STATE_DIM,
        action_dim=params.ACTION_DIM,
        hidden_dims=params.HIDDEN_DIMS,
        learning_rate=params.LEARNING_RATE,
        discount_factor=params.DISCOUNT_FACTOR,
        ppo_clip=params.PPO_CLIP,
        ppo_epoch=params.PPO_EPOCH,
        value_loss_coef=params.VALUE_LOSS_COEF,
        entropy_coef=params.ENTROPY_COEF
    )
    
    # Record max sum rewards
    min_avg_duration_slo = 1e8
    min_avg_duration_slo_episode = 0

    # Start training
    episode = 0
    for exp_id in params.EXP_TRAIN:
        # Set paramters for workloads
        workload_params = WorkloadParameters(
            azure_file_path=params.AZURE_FILE_PATH,
            user_defined_dict=params.USER_DEFINED_DICT,
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
            cpu_cap_per_function=params.CPU_CAP_PER_FUNCTION,
            memory_cap_per_function=params.MEMORY_CAP_PER_FUNCTION,
            memory_unit=params.MEMORY_UNIT
        )
        
        # Set up environment
        env = Environment(
            workload_params=workload_params,
            env_params=env_params
        )
        
        # Set up records
        reward_trend = []
        avg_duration_slo_trend = []
        avg_harvest_cpu_percent_trend = []
        avg_harvest_memory_percent_trend = []
        slo_violation_percent_trend = []
        acceleration_pecent_trend = []
        timeout_num_trend = []
        error_num_trend = []
        loss_trend = []

        for episode_per_exp in range(params.MAX_EPISODE_TRAIN):
            # Record total number of events
            total_events = env.event_pq.get_total_size()

            # Reset env and agent
            env.cool_down_openwhisk()
            logger = logger_wrapper.get_logger(rm, False)
            observation, mask, is_safeguard, current_timestep, current_function_id = env.reset()
            agent.reset()

            actual_time = 0
            system_time = 0
            system_runtime = 0
            reward_sum = 0

            observation_history = []
            mask_history = []
            action_history = []
            reward_history = []
            value_history = []
            log_prob_history = []

            episode_done = False
            while episode_done is False:
                actual_time = actual_time + 1
                action, _, value_pred, log_prob = agent.choose_action(observation, mask)
                next_observation, next_mask, next_is_safeguard, reward, done, info, next_timestep, next_function_id = env.step(
                    current_timestep=current_timestep,
                    current_function_id=current_function_id,
                    is_safeguard=is_safeguard,
                    action=env.decode_action(action)
                )

                # Detach tensors
                observation = observation.detach()
                mask = mask.detach()
                action = action.detach()
                value_pred = value_pred.detach()
                log_prob = log_prob.detach()
                
                # Record trajectories
                observation_history.append(observation)
                mask_history.append(mask)
                action_history.append(action)
                reward_history.append(reward)
                value_history.append(value_pred)
                log_prob_history.append(log_prob)

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
                        
                    avg_duration_slo = info["avg_duration_slo"]
                    avg_harvest_cpu_percent = info["avg_harvest_cpu_percent"]
                    avg_harvest_memory_percent = info["avg_harvest_memory_percent"]
                    slo_violation_percent = info["slo_violation_percent"]
                    acceleration_pecent = info["acceleration_pecent"]
                    timeout_num = info["timeout_num"]
                    error_num = info["error_num"]

                    # Concatenate trajectories
                    observation_history = torch.cat(observation_history, dim=0)
                    mask_history = torch.cat(mask_history, dim=0)
                    action_history = torch.cat(action_history, dim=0)
                    value_history = torch.cat(value_history).squeeze()
                    log_prob_history = torch.cat(log_prob_history, dim=0)

                    loss = agent.update(
                        observation_history=observation_history,
                        mask_history=mask_history,
                        action_history=action_history,
                        reward_history=reward_history,
                        value_history=value_history,
                        log_prob_history=log_prob_history
                    )

                    # Save the min duration slo model
                    if min_avg_duration_slo > avg_duration_slo:
                        min_avg_duration_slo = avg_duration_slo
                        min_avg_duration_slo_episode = episode
                        agent.save(params.MODEL_SAVE_PATH + "min_avg_duration_slo.ckpt")

                    logger.info("")
                    logger.info("**********")
                    logger.info("**********")
                    logger.info("**********")
                    logger.info("")
                    logger.info("Running {}".format(rm))
                    logger.info("Exp {}, Episode {} finished".format(exp_id, episode))
                    logger.info("{} actual timesteps".format(actual_time))
                    logger.info("{} system timesteps".format(system_runtime))
                    logger.info("Total events: {}".format(total_events))
                    logger.info("Total reward: {}".format(reward_sum))
                    logger.info("Avg relative duration: {}".format(avg_duration_slo))
                    logger.info("Avg harvest CPU percent: {}".format(avg_harvest_cpu_percent))
                    logger.info("Avg harvest memory percent: {}".format(avg_harvest_memory_percent))
                    logger.info("SLO violation percent: {}".format(slo_violation_percent))
                    logger.info("Acceleration pecent: {}".format(acceleration_pecent))
                    logger.info("Timeout num: {}".format(timeout_num))
                    logger.info("Error num: {}".format(error_num))
                    logger.info("Loss: {}".format(loss))
                    logger.info("")
                    logger.info("Current Min Avg Duration SLO: {}, observed at episode {}".format(min_avg_duration_slo, min_avg_duration_slo_episode))
                    
                    reward_trend.append(reward_sum)
                    avg_duration_slo_trend.append(avg_duration_slo)
                    avg_harvest_cpu_percent_trend.append(avg_harvest_cpu_percent)
                    avg_harvest_memory_percent_trend.append(avg_harvest_memory_percent)
                    slo_violation_percent_trend.append(slo_violation_percent)
                    acceleration_pecent_trend.append(acceleration_pecent)
                    timeout_num_trend.append(timeout_num)
                    error_num_trend.append(error_num)
                    loss_trend.append(loss)
                    
                    episode_done = True
                
                observation = next_observation
                mask = next_mask
                is_safeguard = next_is_safeguard
                current_timestep = next_timestep
                current_function_id = next_function_id

            episode = episode + 1

        # Log trends
        log_trends(
            overwrite=False,
            rm_name=rm,
            exp_id=exp_id,
            logger_wrapper=logger_wrapper,
            reward_trend=reward_trend,
            avg_duration_slo_trend=avg_duration_slo_trend,
            avg_harvest_cpu_percent_trend=avg_harvest_cpu_percent_trend,
            avg_harvest_memory_percent_trend=avg_harvest_memory_percent_trend,
            slo_violation_percent_trend=slo_violation_percent_trend,
            acceleration_pecent_trend=acceleration_pecent_trend,
            timeout_num_trend=timeout_num_trend,
            error_num_trend=error_num_trend,
            loss_trend=loss_trend
        )

    # Save the model with max training episode
    agent.save(params.MODEL_SAVE_PATH + "max_episode.ckpt")


if __name__ == "__main__":
    # Set up logger wrapper
    logger_wrapper = Logger()

    print("")
    print("**********")
    print("**********")
    print("**********")
    print("")

    # Start training
    print("Start training...")
    lambda_rm_train(logger_wrapper=logger_wrapper)

    print("")
    print("Training finished!")
    print("")
    print("**********")
    print("**********")
    print("**********")
    