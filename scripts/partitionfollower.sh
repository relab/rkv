#!/bin/bash

for n in {01..10}; do
    ansible-playbook -v -f 10 -i hosts partitionfollower.yml -e "backend=hashicorp throughput=50 output=pf_${n} maxentries=1024 clients=200 time=80s order=true";
done;

exit;

for n in {01..10}; do
    ansible-playbook -v -f 10 -i hosts partitionfollower.yml -e "backend=gorums throughput=50 output=pf_${n} maxentries=1024 clients=200 time=80s order=true";
done;

for n in {01..10}; do
    ansible-playbook -v -f 10 -i hosts partitionfollower.yml -e "backend=gorums throughput=50 output=pf_${n} maxentries=1024 clients=200 time=80s order=false";
done;

for n in {01..10}; do
    ansible-playbook -v -f 10 -i hosts partitionfollower.yml -e "backend=hashicorp throughput=50 output=pf_${n} maxentries=1024 clients=200 time=80s order=true";
done;

for n in {01..10}; do
    ansible-playbook -v -f 10 -i hosts partitionfollower.yml -e "backend=etcd throughput=50 output=pf_${n} maxentries=1024 clients=200 time=80s order=true";
done;
