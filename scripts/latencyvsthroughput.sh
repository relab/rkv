#!/bin/bash

for n in {01..04}; do
    for T in {005,025,050,060,070,080,090,100,110,120,130} ; do
        ansible-playbook -v -i hosts latencyvsthroughput.yml -e "backend=gorums throughput=$T output=${n} maxentries=1024 clients=200 time=60s";
    done;
done;

for n in {01..04}; do
    for T in {005,010,020,030,040,050,060,070,080}; do
        ansible-playbook -v -i hosts latencyvsthroughput.yml -e "backend=gorums throughput=$T output=${n} maxentries=64 clients=200 time=60s";
    done;
done;

for n in {01..04}; do
    for T in {005,025,050,060,070,080,090,100,110,120} ; do
        ansible-playbook -v -i hosts latencyvsthroughput.yml -e "backend=hashicorp throughput=$T output=${n} maxentries=1024 clients=200 time=60s";
    done;
done;

for n in {01..04}; do
    for T in {005,010,020,030,040,050,060,070,080}; do
        ansible-playbook -v -i hosts latencyvsthroughput.yml -e "backend=hashicorp throughput=$T output=${n} maxentries=64 clients=200 time=60s";
    done;
done;

for n in {01..04}; do
    for T in {005,025,050,060,070,080,090,100,110,120} ; do
        ansible-playbook -v -i hosts latencyvsthroughput.yml -e "backend=etcd throughput=$T output=${n} maxentries=1024 clients=200 time=60s";
    done;
done;

for n in {01..04}; do
    for T in {005,010,020,030,040,050,060,070,080}; do
        ansible-playbook -v -i hosts latencyvsthroughput.yml -e "backend=etcd throughput=$T output=${n} maxentries=64 clients=200 time=60s";
    done;
done;
