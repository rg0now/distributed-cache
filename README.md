# Distributed caching: Scaling benchmark

Distributed cache scaling benchmark using a PostgreSQL database with a memcached caching layer.

## Compile

``` console
apt-get install git memcached libmemcached-tools libmemcached-dev postgresql libpq-dev cpulimit
git clone ...
make -C memslap/ all
make -C script/ all
```

## Init

Start PostgreSQL server version 17 on localhost, fill database with 1,000,000 16 byte keys and 48
byte values, save the database, and then start 20 memcached instances on localhost, each running
with 4 MB of cache:
  
``` console
rm script/test_table.dump 
script/init-db.sh 17 1000000
script/init-cache-shard.sh 20 4
```

Note that in order to keep the load relatively law the memcached servers are each assigned 250
mcore CPU using `cpulimit`.

Filling the database initially may take a while, so the init script saves the database into a
binary dump in `script/test_table.dump` and using this to restore the database on subsequent
runs. If you wish to regenerate the key-value pairs, simply delete the database dump and re-run
`script/init-db.sh`:

``` console
rm script/test_table.dump 
script/init-db.sh 17 1000
```

## Run

The below will run a benchmark with 2 memcached servers (flushed on startup) using a modulo-hash
based key-sharding technique to distribute keys to memcached server instances, PostgreSQL reachable
on localhost:5432, warms up the memcached instances using 500,000 keys from, fires up 2 threads
with each thread executing 1 million cache-aside/lazy-loading reads iterations, each time querying
a random key: first we try to retrieve the value from memcached and if this fails, we load the
value from the PostgreSQL database and write it back into the cache:

``` console
./memslap -s localhost:11211,localhost:11212 -F -t get --pg-host=localhost --pg-port=5432 \
    --pg-db=test --pg-user=postgres --pg-pass=test -e 1000000 -k 500000 -c 2 -m modulo-hash \
    -o run-4000000-500000.csv
```

The benchmark statistics will be appended to a CSV file called `run-4000000-500000.csv`.

The below will run an entire evaluation scaling memcached and the PostgreSQL threads jointly,
executing 4,000,000 iterations in total using 500,000 keys, and write the stats into the file
`run-4000000-500000.csv`:

``` console
./run.sh 4000000 500000 run-4000000-500000.csv
```

The script will run two benchmarks per each scale: one using a module-hash key-sharding and another
one using random key distribution. This makes it possible to compare the scaling law with and
without locality-boosting.

The trick is to scale memcached from 1 to 20 servers and jointly scaling the number of PostgreSQL
threads. Usually we run 4 PostgreSQL client threads per each memcached server instance, this is set
in the `THREAD_MULTIPLIER` variable in `run.sh`. This allows to scale the entire storage layer
jointly. 

# License

Copyright 2025 by its authors. Some rights reserved. 

MIT License - see LICENSE for full text.
