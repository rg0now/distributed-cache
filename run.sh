#!/bin/bash

USAGE='./sim.sh <exec> <keys>'
[ -z "$1" -o -z "$2" ] && echo $USAGE && exit 1
EXEC=$1
KEYS=$2

for i in {1..10}; do
    SERVERS="localhost:11211"
    if ((i > 1)); then
        for (( j=2; j<=${i}; j++ )); do
            PORT=$((11210+j))
            SERVERS="${SERVERS},localhost:${PORT}"
        done
    fi
    
    ./memslap/memslap -s "$SERVERS" -F -t get --pg-host=localhost --pg-port=5432 --pg-db=test \
                      --pg-user=postgres --pg-pass=test -e $EXEC -k $KEYS -c 256
done

exit 0
