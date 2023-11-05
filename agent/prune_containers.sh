#! /bin/bash

container_list=`sudo docker ps --filter "name=guest" | awk {'print $1'} | sed '1d'`

for container in $container_list
do
    sudo docker stop $container
done

sudo docker container prune --force

