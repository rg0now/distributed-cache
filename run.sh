#!/bin/bash

USAGE='./sim.sh <exec> <keys> <output>'
[ -z "$1" -o -z "$2"  -o -z "$3" ] && echo $USAGE && exit 1
EXEC="$1"
KEYS="$2"
OUTPUT="$3"

THREAD_MULTIPLIER=4

# clear outputfile
echo "cores,mode,hit_rate,elapsed_time" > "$OUTPUT"

for i in {1..17}; do
    SERVERS="localhost:11211"
    if ((i > 1)); then
        for (( j=2; j<=${i}; j++ )); do
            PORT=$((11210+j))
            SERVERS="${SERVERS},localhost:${PORT}"
        done
    fi

    THREADS=$(($i*3))
    ITERATIONS=$(($EXEC/$THREADS))

    for mode in "modulo-hash" "random"; do
    
        COMMAND="./memslap/memslap -s $SERVERS -F -t get --pg-host=localhost --pg-port=5432 --pg-db=test --pg-user=postgres --pg-pass=test -e $ITERATIONS -k $KEYS -c $THREADS -m $mode -o $OUTPUT"
        echo "$COMMAND"
        $COMMAND

    done

done

exit 0
