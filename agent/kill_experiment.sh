#! /bin/bash

ps aux | grep -v grep | grep -E "launcher.py|faas_rank_train.py" | awk {'print $2'} | xargs kill
