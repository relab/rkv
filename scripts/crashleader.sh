#!/bin/bash

for n in {01..04}; do
    for backend in {gorums,etcd,hashicorp}; do
        ansible-playbook -v -f 10 -i hosts crashleader.yml -e "backend=${backend} throughput=30 output=cl_${n} maxentries=1024 clients=200 time=60s";
    done;
done;
