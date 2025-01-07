# Superlinear scaling test

## Compile

``` console
apt-get install git memcached libmemcached-tools libmemcached-dev postgresql libpq-dev
git clone ...
make -C memslap/ all
make -C script/ all
```

## Init

Start postgresql server version 17 on localhost, fill database with 10,000 keys, and save the
database, then start 2 memcached instances using key-sharding with 2 memcached instances:
  
``` console
rm script/test_table.dump 
script/init-db.sh 17 10000
script/init-cache-shard.sh 2
```

Same, but restore last saved database instead of creating a new one:

``` console
script/init-db.sh 10000
script/init-cache-shard.sh 2
```

## Run

Use 2 servers, get test, postgres on localhost, 10 keys, 20 executions
  
``` console
./memslap -s localhost:11211,localhost:11212 -t get -F --pg-host=localhost --pg-port=5432 --pg-db=test --pg-user=test --pg-pass=test -e 20 -k 10
```
