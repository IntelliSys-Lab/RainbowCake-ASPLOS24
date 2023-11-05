import torch
import torch.nn as nn


class MLP(nn.Module):
    def __init__(
        self, 
        observation_dim, 
        hidden_dims,
        action_dim,
    ):
        super().__init__()

        self.observation_dim = observation_dim
        self.hidden_dims = hidden_dims
        if isinstance(hidden_dims, int):
            hidden_dims = [hidden_dims]
        self.action_dim = action_dim
        
        self.layers = []
        
        layer_input = nn.Sequential(
            nn.Linear(observation_dim, hidden_dims[0]),
            nn.Tanh()
        )
        self.layers.append(layer_input)
        
        for i in range(len(hidden_dims)):
            if i == len(hidden_dims)-1:
                layer_output = nn.Linear(hidden_dims[i], action_dim)
                self.layers.append(layer_output)
            else:
                layer_hidden = nn.Sequential(
                    nn.Linear(hidden_dims[i], hidden_dims[i+1]),
                    nn.Tanh()
                )
                self.layers.append(layer_hidden)
        
        self.layer_module = nn.ModuleList(self.layers)

    def forward(self, x):
        for layer in self.layer_module:
            x = layer(x)

        return x
        