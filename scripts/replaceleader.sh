#!/bin/bash

for n in {01..03}; do
    for T in {05,30}; do
        for backend in gorums; do
            ansible-playbook -v -f 10 -i hosts replaceleader.yml -e "backend=${backend} throughput=${T} output=rl_${n} maxentries=1024 clients=200 time=60s order=true";
        done;
    done;
done;
