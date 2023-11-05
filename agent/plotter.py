import matplotlib.pyplot as plt
import os


class Plotter():
    """
    Plot trend for each episode
    """
    def __init__(
        self,
        fig_path=os.path.dirname(os.getcwd())+"/agent/figures/"
    ):
        self.fig_path = fig_path
    
    def plot_save(
        self,
        prefix_name,
        reward_trend, 
        avg_completion_time_trend,
        timeout_num_trend,
        error_num_trend,
        cold_start_num_trend,
        loss_trend=None,
    ):
        fig = plt.figure('Total Reward Trend', figsize=(6,4)).add_subplot(111)
        fig.plot(reward_trend)
        fig.set_xlabel("Episode")
        fig.set_ylabel("Total reward")
        plt.savefig(self.fig_path + prefix_name + "_Total_Reward_Trend.png")
        plt.clf()
        
        fig = plt.figure('Avg Completion Time Trend', figsize=(6,4)).add_subplot(111)
        fig.plot(avg_completion_time_trend)
        fig.set_xlabel("Episode")
        fig.set_ylabel("Avg Completion Time")
        plt.savefig(self.fig_path + prefix_name + "_Avg_Completion_Time_Trend.png")
        plt.clf()

        fig = plt.figure('Timeout Num Trend', figsize = (6,4)).add_subplot(111)
        fig.plot(timeout_num_trend)
        fig.set_xlabel("Episode")
        fig.set_ylabel("Timeout num")
        plt.savefig(self.fig_path + prefix_name + "_Timeout_Num_Trend.png")
        plt.clf()

        fig = plt.figure('Error Num Trend', figsize = (6,4)).add_subplot(111)
        fig.plot(error_num_trend)
        fig.set_xlabel("Episode")
        fig.set_ylabel("Error num")
        plt.savefig(self.fig_path + prefix_name + "_Error_Num_Trend.png")
        plt.clf()

        fig = plt.figure('Cold Start Num Trend', figsize = (6,4)).add_subplot(111)
        fig.plot(error_num_trend)
        fig.set_xlabel("Episode")
        fig.set_ylabel("Cold start num")
        plt.savefig(self.fig_path + prefix_name + "_Cold_Start_Num_Trend.png")
        plt.clf()
        
        if loss_trend is not None:
            fig = plt.figure('Loss Trend', figsize = (6,4)).add_subplot(111)
            fig.plot(loss_trend)
            fig.set_xlabel("Episode")
            fig.set_ylabel("Loss")
            plt.savefig(self.fig_path + prefix_name + "_Loss_Trend.png")
        