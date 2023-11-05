import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.distributions import Categorical
import numpy as np
from mlp import MLP


class ActorCritic(nn.Module):
    def __init__(
        self, 
        actor,
        critic
    ):
        super().__init__()

        self.actor = actor
        self.critic = critic

    def forward(self, observation):
        action_pred = self.actor(observation)
        value_pred = self.critic(observation)
        
        return action_pred, value_pred


class PPO2Agent():
    def __init__(
        self,
        observation_dim,
        action_dim,
        hidden_dims,
        learning_rate,
        discount_factor,
        ppo_clip,
        ppo_epoch,
        value_loss_coef,
        entropy_coef
    ):
        self.observation_dim = observation_dim
        self.action_dim = action_dim
        self.hidden_dims = hidden_dims
        self.learning_rate = learning_rate
        self.gamma = discount_factor

        self.ppo_clip = ppo_clip
        self.ppo_epoch = ppo_epoch
        self.value_loss_coef = value_loss_coef
        self.entropy_coef = entropy_coef

        self.model = self.build_model()
        self.optimizer = torch.optim.AdamW(self.model.parameters(), lr= self.learning_rate)

    def build_model(self):
        actor = MLP(
            observation_dim=self.observation_dim,
            hidden_dims=self.hidden_dims,
            action_dim=self.action_dim,
        )

        critic = MLP(
            observation_dim=self.observation_dim,
            hidden_dims=self.hidden_dims,
            action_dim=1,
        )

        ac_model = ActorCritic(actor, critic)

        return ac_model

    def choose_action(self, observation, mask):
        self.model.eval()
        
        action_pred, value_pred = self.model(observation)
        action_pred = action_pred.reshape(1, -1)
        value_pred = torch.mean(value_pred.reshape(1, -1), dim=-1, keepdim=True)
        action_pred = action_pred + mask
        action_prob = F.softmax(action_pred, dim=-1)
        dist = Categorical(action_prob)
        action = dist.sample()
        log_prob = dist.log_prob(action)
        dist_entropy = dist.entropy().mean()
        
        return action, action_pred, value_pred, log_prob

    def update(
        self,
        observation_history,
        mask_history,
        action_history,
        reward_history,
        value_history,
        log_prob_history
    ):
        self.model.train()

        # Discount rewards
        reward_history = self.discount_rewards(reward_history)

        # Calculate advantage
        advantage = reward_history - value_history
        
        # Detach trajectory 
        observation_history = observation_history.detach()
        mask_history = mask_history.detach()
        action_history = action_history.detach()
        log_prob_history = log_prob_history.detach()
        advantage = advantage.detach()
        reward_history = reward_history.detach()

        # print("History after detached: ")
        # print("observation_history: {}".format(observation_history.shape))
        # print("mask_history: {}".format(mask_history.shape))
        # print("action_history shape: {}".format(action_history.shape))
        # print("reward_history shape: {}".format(reward_history.shape))
        # print("value_history shape: {}".format(value_history.shape))
        # print("log_prob_history shape: {}".format(log_prob_history.shape))

        # Back propagate batch avg loss
        loss_epoch = 0
        max_timestep = observation_history.size(0)
        for _ in range(self.ppo_epoch):
            # Get new log probs of actions for all input states
            action_pred_all, value_pred_all = self.model(observation_history)
            action_pred_all = action_pred_all.reshape(max_timestep, 1, -1)
            value_pred_all = torch.mean(value_pred_all.reshape(max_timestep, 1, -1), dim=-1)
            # print("action_pred_all shape: {}".format(action_pred_all.shape))
            # print("value_pred_all shape: {}".format(value_pred_all.shape))
            
            action_pred_all = action_pred_all + mask_history
            action_prob_all = F.softmax(action_pred_all, dim=-1)
            dist = Categorical(action_prob_all)
            dist_entropy = dist.entropy().mean()

            # New log probs and values using old actions
            new_log_prob_history = dist.log_prob(action_history)
            new_value_history = value_pred_all.squeeze()

            policy_ratio = (new_log_prob_history - log_prob_history).exp()
            policy_loss_1 = policy_ratio * advantage
            policy_loss_2 = torch.clamp(
                policy_ratio, 
                min = 1.0 - self.ppo_clip, 
                max = 1.0 + self.ppo_clip
            ) * advantage

            policy_loss = - torch.min(policy_loss_1, policy_loss_2).mean()
            value_loss = F.smooth_l1_loss(reward_history, new_value_history).mean()
            loss = policy_loss + value_loss * self.value_loss_coef - dist_entropy * self.entropy_coef

            self.optimizer.zero_grad()
            loss.backward()
            self.optimizer.step()
            loss_epoch = loss_epoch + loss.item()
            
        loss_epoch = loss_epoch / self.ppo_epoch

        return loss_epoch
    
    def discount_rewards(self, reward_history):
        discounted_rewards = []
        tmp = 0
        for reward in reward_history[::-1]:
            tmp = tmp * self.gamma + reward
            discounted_rewards.append(tmp)
        
        return torch.Tensor(discounted_rewards[::-1])
    
    def reset(self):
        pass

    def save(self, save_path):
        torch.save(self.model.state_dict(), save_path)

    def load(self, checkpoint_path):
        self.model.load_state_dict(torch.load(checkpoint_path))
        print("Total parameters: {}".format(self.count_parameters()))

    def count_parameters(self):
        return sum(p.numel() for p in self.model.parameters() if p.requires_grad)
        