#! /bin/bash

docker_hub_logout="docker logout"

# Logout for VMs
sudo $docker_hub_logout

# Logout for invokerss
invoker_id=`sudo docker ps --filter "name=invoker" | grep -v "CONTAINER" | awk '{print $1}'`

for id in $invoker_id
do
    sudo docker exec $id $docker_hub_logout
done
