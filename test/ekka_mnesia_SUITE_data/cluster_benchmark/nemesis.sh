#!/bin/bash
# This script runs as root, receives commands from the common test
# suite over FIFO, and forwards them to slowdown.sh
set -uo pipefail

FIFO=/tmp/nemesis
rm $FIFO
mkfifo $FIFO
chmod 666 $FIFO

while true; do
    if read line; then
        echo "Received command ${line}"
        $(dirname $0)/slowdown.sh $line 1 epmd
    fi
done < $FIFO
