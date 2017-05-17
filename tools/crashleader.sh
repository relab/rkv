#!/bin/bash

for n in {01..04}; do
    for T in {gorums,etcd,hashicorp}; do
        ansible-playbook -v -i hosts throughputvstime.yml -e "backend=etcd throughput=50 output=cl_${n} maxentries=1024 clients=200";
    done;
done;
