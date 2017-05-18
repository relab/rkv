#!/bin/bash

for n in {01..04}; do
    ansible-playbook -v -i hosts replaceleader.yml -e "backend=gorums throughput=50 output=rl_${n} maxentries=1024 clients=200 time=60s";
done;
