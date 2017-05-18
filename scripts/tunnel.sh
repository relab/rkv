#!/bin/bash

# Used to forward prometheus requests to remote servers. lport must be 59100,
# 5901, 5902, 5903, 5904 or 5905, as specified in prometheus.yml. Where 59100 is
# used for the client, and 590x is used for the server with raft id x. rport
# should be either 59100 or 9201 depending on being a client or server.

if [[ $# -ne 3 ]]; then
    echo -e "\n\ttunnel.sh establishes a tunnel from localhost:lport to remote:rport.";
    echo -e "\n\tUsage ./tunnel.sh remote lport rport\n";
    exit;
fi

ssh $1 -L $2:localhost:$3 -q
