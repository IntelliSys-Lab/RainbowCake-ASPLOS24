<!--
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

# RainbowCake-ASPLOS24

[![DOI](https://zenodo.org/badge/696946314.svg)](https://zenodo.org/doi/10.5281/zenodo.10056676)

<img src="https://github.com/IntelliSys-Lab/RainbowCake-ASPLOS24/blob/master/logo.png" width="100">

----

This repo contains a demo implementation of our ASPLOS 2024 paper, [RainbowCake: Mitigating Cold-starts in Serverless with Layer-wise Container Caching and Sharing](https://github.com/IntelliSys-Lab/RainbowCake-ASPLOS24). 
> Serverless computing has grown rapidly as a new cloud computing paradigm that promises ease-of-management, cost-efficiency, and auto-scaling by shipping functions via self-contained virtualized containers. Unfortunately, serverless computing suffers from severe cold-start problems---starting containers incurs non-trivial latency. Full container caching is widely applied to mitigate cold-starts, yet has recently been outperformed by two lines of research: partial container caching and container sharing. However, either partial container caching or container sharing techniques exhibit their drawbacks. Partial container caching effectively deals with burstiness while leaving cold-start mitigation halfway; container sharing reduces cold-starts by enabling containers to serve multiple functions while suffering from excessive memory waste due to over-packed containers. This paper proposes RainbowCake, a layer-wise container pre-warming and keep-alive technique that effectively mitigates cold-starts with sharing awareness at minimal waste of memory. With structured container layers and sharing-aware modeling, RainbowCake is robust and tolerant to invocation bursts. We seize the opportunity of container sharing behind the startup process of standard container techniques. RainbowCake breaks the container startup process of a container into three stages and manages different container layers individually. We develop a sharing-aware algorithm that makes event-driven layer-wise caching decisions in real-time. Experiments on OpenWhisk clusters with real-world workloads show that RainbowCake reduces 68\% function startup latency and 77\% memory waste compared to state-of-the-art solutions.

----

RainbowCake is built atop [Apache OpenWhisk](https://github.com/apache/openwhisk). We describe how to build and deploy RainbowCake from scratch for this demo. We also provide a public AWS EC2 AMI for fast reproducing the demo experiment.

## Build From Scratch

### Hardware Prerequisite
- Operating systems and versions: Ubuntu 20.04
- Resource requirement
  - CPU: >= 8 cores
  - Memory: >= 15 GB
  - Disk: >= 30 GB
  - Network: no requirement since it's a single-node deployment

Equivalent AWS EC2 instance type: c4.2xlarge with 30 GB EBS storage under root volume

### Deployment and Run Demo
This demo hosts all RainbowCake's components on a single node.   

**Instruction**

1. Download the github repo.
```
git clone https://github.com/IntelliSys-Lab/RainbowCake-ASPLOS24
```
2. Go to [`RainbowCake-ASPLOS24/demo`](https://github.com/IntelliSys-Lab/RainbowCake-ASPLOS24/tree/master/demo).
```
cd RainbowCake-ASPLOS24/demo
```
3. Set up the environment. This could take quite a while due to building Docker images from scratch. The recommended shell to run `setup.sh` is Bash.
```
./setup.sh
```
4. Run RainbowCake's demo. The demo experiment may take several minutes to complete.
```
python3 run_demo.py
```

## Reproduce via AWS EC2 AMI

### Prerequisite
- [AWS EC2](https://aws.amazon.com/ec2/): Instance type should be at least the size of **c4.2xlarge** with at least **30 GB EBS storage under root volume**
- [AWS EC2 AMI](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AMIs.html): **ami-0dde141c1a2583f46**

### Run Demo
Since this AMI has preinstalled all dependencies and built all Docker images, you can directly launch the demo once your EC2 instance is up.

**Instruction**

1. Launch a c4.2xlarge instance with 30 GB EBS storage under root volume using AMI **ami-0b5c53c5c909b8b6a**. Our AMI can be found by searching the AMI ID: **EC2 Management Console** -> **Images/AMIs** -> **Public Images** -> **Search**.
2. Log into your EC2 instance and go to [`RainbowCake-ASPLOS24/demo`](https://github.com/IntelliSys-Lab/RainbowCake-ASPLOS24/tree/master/demo).
```
cd RainbowCake-ASPLOS24/demo
```
3. Run RainbowCake's demo. The demo experiment may take one or two minutes to complete.
```
python3 run_demo.py
```

## Experimental Results and OpenWhisk Logs

After executing `run_demo.py`, you may use the [wsk-cli](https://github.com/IntelliSys-Lab/RainbowCake-ASPLOS24/blob/master/demo/wsk) to check the results of function executions:
```
wsk -i activation list
```

Detailed experimental results are collected as CSV files under `RainbowCake-ASPLOS24/demo/logs`, including function end-to-end and startup latency, invocation startup types, timelines, and container memory waste. Note that `RainbowCake-ASPLOS24/demo/logs` is not present in the initial repo. It will only be generated after running an experiment. OpenWhisk system logs can be found under `/var/tmp/wsklogs`.

## Workloads

We provide the codebase of [20 serverless applications](https://github.com/IntelliSys-Lab/RainbowCake-ASPLOS24/tree/master/applications) used in our evaluation. However, due to hardware and time limitations, we only provide a simple [demo invocation trace](https://github.com/IntelliSys-Lab/RainbowCake-ASPLOS24/tree/master/demo/azurefunctions-dataset2019) for the demo experiment.

## Distributed RainbowCake
The steps of deploying a distributed RainbowCake are basically the same as deploying a distributed OpenWhisk cluster. For deploying a distributed RainbowCake, please refer to the README of [Apache OpenWhisk](https://github.com/apache/openwhisk) and [Ansible](https://github.com/apache/openwhisk/tree/master/ansible). 
