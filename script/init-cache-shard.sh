#!/bin/bash

USAGE="init-cache-shard.sh <num-memcached-servers> <memory-MB>"
[ -z "$1" -o -z "$2" ] && echo $USAGE && exit 0
MEMCACHED_SERVER_NUM="$1"
MEMORY="$2"

cd "$(dirname "$0")"

echo "Start memcached"
killall memcached
sleep 1
for p in $(seq 11211 $((11211+MEMCACHED_SERVER_NUM-1))); do
    memcached -l localhost -m $MEMORY -t 1 -p $p -U $p &
done

exit 1
