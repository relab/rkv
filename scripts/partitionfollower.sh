#!/bin/bash

for n in {01..05}; do
    for T in 50; do
        for backend in gorums; do
            for order in {true,false}; do
                ansible-playbook -v -f 10 -i hosts partitionfollower.yml -e "backend=${backend} throughput=${T} output=pf_${n} maxentries=1024 clients=200 time=80s order=${order}";
            done;
        done;
        for backend in {hashicorp,etcd}; do
            ansible-playbook -v -f 10 -i hosts partitionfollower.yml -e "backend=${backend} throughput=${T} output=pf_${n} maxentries=1024 clients=200 time=80s order=true";
        done;
    done;
done;
