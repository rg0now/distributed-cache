#!/bin/bash

TABLE_DUMP=test_table.dump
export PGPASSWORD=test

USAGE="init-cache-shard.sh <num-memcached-servers>"
[ -z "$1" ] && echo $USAGE && exit 0
MEMCACHED_SERVER_NUM="$1"

cd "$(dirname "$0")"

echo "Start memcached"
killall memcached
sleep 1
for p in $(seq 11211 $((11211+MEMCACHED_SERVER_NUM))); do
    memcached -l localhost -p $p -U $p &
done

exit 1
