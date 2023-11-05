#! /bin/bash

cd ../ansible 
sudo ansible-playbook -i environments/distributed update.yml
sudo ansible-playbook -i environments/distributed openwhisk.yml
sudo ansible-playbook -i environments/distributed login_docker_hub.yml
cd ../agent
