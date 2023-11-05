#! /bin/bash

ansible-playbook -i environments/distributed openwhisk.yml -e mode=clean
sudo ansible-playbook -i environments/distributed couchdb.yml -e mode=clean
sudo ansible-playbook -i environments/distributed postdeploy.yml -e mode=clean
sudo ansible-playbook -i environments/distributed apigateway.yml -e mode=clean
sudo ansible-playbook -i environments/distributed routemgmt.yml -e mode=clean
