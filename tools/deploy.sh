#!/bin/bash

ansible-playbook deploy.yml -v --private-key=~/.ssh/id_rsa
