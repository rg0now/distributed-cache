#!/bin/bash

TABLE_DUMP=test_table.dump
export PGPASSWORD=test

USAGE="init-db.sh <db-version> <num-keys>"
[ -z "$1" -o -z "$2" ] && echo $USAGE && exit 0
VERSION="$1"
KEY_NUM="$2"

cd "$(dirname "$0")"

echo "Start PostgreSQL server"
sudo pg_ctlcluster $VERSION main restart -f

echo "Change password"
psql -h localhost -U postgres -c "ALTER USER postgres WITH PASSWORD 'test';"

echo "Create database"
psql -h localhost -U postgres -c 'CREATE DATABASE "test"'

if [ -r "$TABLE_DUMP" ]; then
    echo "Restoring database from dump $TABLE_DUMP"
    pg_restore -h localhost -U postgres -d test -c -Fc "$TABLE_DUMP"
else
    echo "Creating new database, this may take a while"
    ./db_filler $KEY_NUM
    echo "Saving database for reuse"
    pg_dump -h localhost -U postgres -Fc test > "$TABLE_DUMP"
fi

echo "Start pgbouncer"
sudo killall pgbouncer
sleep 1
/usr/sbin/pgbouncer -d pgbouncer.ini -v

exit 1

