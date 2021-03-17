#!/bin/bash
set -eo pipefail

[ $(uname) = Linux ] || {
    echo "Sorry, this script only works on Linux";
    exit 1;
}

[ -z $2 ] && {
    echo "Emulate network latency on the localhost.

Usage:

    $(basename $0) DELAY JITTER [PORT1 PORT2 ...]

It is possible to specify PORT as 'empd' to apply delay to the
distribution ports of all running BEAM VMs (excluding the CT
master).

Both DELAY and JITTER should be more than 0

Example:

    $(basename $0) 500ms 1 8001 8002 empd"
    exit 1;
}

DELAY=$1
JITTER=$2
shift 2

# Clean up:
iptables -t mangle -F OUTPUT || true
tc qdisc del dev lo root || true

# Shape packets marked as 12
MARK=12
ID=$MARK
tc qdisc add dev lo root handle 1: htb
tc class add dev lo parent 1: classid 1:$ID htb rate 1000Mbps
tc qdisc add dev lo parent 1:$ID handle $MARK netem delay $DELAY $JITTER distribution normal
tc filter add dev lo parent 1: prio 1 protocol ip handle $MARK fw flowid 1:$ID

# Create firewall rules to mark the packets:
mark_port() {
    PORT=$1
    echo "Adding latency on tcp port $PORT"
    iptables -A OUTPUT -p tcp --dport $PORT -t mangle -j MARK --set-mark $MARK
}

while [ ! -z $1 ]; do
    PORT=$1
    shift
    if [ $PORT = epmd ]; then
        for i in $(epmd -names | grep -v 'ct@' | awk '/at port/{print $5}'); do
            mark_port $i
        done
    else
        mark_port $PORT
    fi
done
