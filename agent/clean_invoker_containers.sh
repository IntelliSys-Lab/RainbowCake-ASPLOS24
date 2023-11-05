#! /bin/bash

cd ../ansible 
sudo ansible-playbook -i environments/distributed clean_invoker_containers.yml
cd ../agent
