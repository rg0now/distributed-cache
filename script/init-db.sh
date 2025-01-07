#!/bin/bash

TABLE_DUMP=test_table.dump
export PGPASSWORD=test

USAGE="init-db.sh <num-keys>"
[ -z "$1" ] && echo $USAGE && exit 0
KEY_NUM="$1"

cd "$(dirname "$0")"

echo "Start PostgreSQL server"
sudo pg_ctlcluster 17 main restart -f

echo "Change password"
sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD 'test';"

if [ -r "$TABLE_DUMP" ]; then
    echo "Restoring database from dump $TABLE_DUMP"
    pg_restore -h localhost -U postgres -d test -c -Fc "$TABLE_DUMP"
else
    echo "Creating new database, this may take a while"
    ./db_filler $KEY_NUM
    echo "Saving database for reuse"
    pg_dump -h localhost -U postgres -Fc test > "$TABLE_DUMP"
fi

exit 1

