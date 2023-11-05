#!/bin/bash

# Default to '*' key pattern, meaning all redis keys in the namespace
REDIS_KEY_PATTERN="${REDIS_KEY_PATTERN:-*}"

# Connect to redis-cli
REDIS_HOST=`cat ../ansible/environments/distributed/hosts | grep -A 1 "\[edge\]" | grep "ansible_host" | awk {'print $1'}`
REDIS_PORT="6379"
REDIS_PASSWORD="openwhisk"

REDIS_CLI="redis-cli -h $REDIS_HOST -p $REDIS_PORT -a $REDIS_PASSWORD --no-auth-warning"

for key in $($REDIS_CLI --scan --pattern "$REDIS_KEY_PATTERN")
do
    type=$($REDIS_CLI type $key)
    if [ $type = "list" ]
    then
        printf "$key => \n$($REDIS_CLI lrange $key 0 -1 | sed 's/^/  /')\n"
    elif [ $type = "hash" ]
    then
        printf "$key => \n$($REDIS_CLI hgetall $key | sed 's/^/  /')\n"
    else
        printf "$key => $($REDIS_CLI get $key)\n"
    fi
done
