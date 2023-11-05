#! /bin/bash

set -ex

# Install prerequisite
cd ../tools/ubuntu-setup
./all.sh
cd ../../demo

# Define all vars
cd ../ansible
EDGE_AUTH=`cat files/auth.guest`
EDGE_HOST=`cat environments/distributed/hosts | grep -A 1 "edge" | grep ansible_host | awk {'print $1'}`
EDGE_PORT=443
cd ../demo

# Build gradle
cd ..
sudo ./gradlew distDocker
cd demo

# Ansible install
cd ../ansible
sudo ansible all -i environments/distributed -m ping
sudo ansible-playbook -i environments/distributed setup.yml
sudo ansible-playbook -i environments/distributed couchdb.yml
sudo ansible-playbook -i environments/distributed initdb.yml
sudo ansible-playbook -i environments/distributed wipe.yml
sudo ansible-playbook -i environments/distributed openwhisk.yml
sudo ansible-playbook -i environments/distributed postdeploy.yml
sudo ansible-playbook -i environments/distributed apigateway.yml
sudo ansible-playbook -i environments/distributed routemgmt.yml
cd ../demo

# Configure wsk cli
sudo ln -sfn $(pwd)/wsk /usr/local/bin/wsk
wsk property set --auth ${EDGE_AUTH} --apihost https://${EDGE_HOST}:${EDGE_PORT}

# Setup applications
cd ../applications
./deploy_functions.sh
cd ../demo
