#! /bin/bash

# Monitor invokers
monitor_ps=`ps aux | grep "monitor_invoker" | grep -v "grep"`

if [ -z "$monitor_ps" ]
then
    nohup python3 monitor_invoker.py >/dev/null 2>&1 &
fi
