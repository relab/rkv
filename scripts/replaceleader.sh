#!/bin/bash

for n in {01..04}; do
    for backend in {gorums,etcd,hashicorp}; do
        ansible-playbook -v -i hosts replaceleader.yml -e "backend=${backend} throughput=20 output=rl_${n} maxentries=1024 clients=200 time=60s";
    done;
done;
