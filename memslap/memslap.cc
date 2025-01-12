/*
    +--------------------------------------------------------------------+
    | libmemcached-awesome - C/C++ Client Library for memcached          |
    +--------------------------------------------------------------------+
    | Redistribution and use in source and binary forms, with or without |
    | modification, are permitted under the terms of the BSD license.    |
    | You should have received a copy of the license in a bundled file   |
    | named LICENSE; in case you did not receive a copy you can review   |
    | the terms online at: https://opensource.org/licenses/BSD-3-Clause  |
    +--------------------------------------------------------------------+
    | Copyright (c) 2006-2014 Brian Aker   https://datadifferential.com/ |
    | Copyright (c) 2020-2021 Michael Wallner        https://awesome.co/ |
    +--------------------------------------------------------------------+
*/

#include "libmemcached/mem_config.h"

#define PROGRAM_NAME        "memslap"
#define PROGRAM_DESCRIPTION "Generate load against a cluster of memcached servers."
#define PROGRAM_VERSION     "1.1"

#define DEFAULT_INITIAL_LOAD   10000ul
#define DEFAULT_EXECUTE_NUMBER 10000ul
#define DEFAULT_CONCURRENCY    1ul

#include "options.hpp"
#include "checks.hpp"
#include "time.hpp"
#include "random.hpp"

#include <atomic>
#include <thread>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <cstring>

static std::atomic_bool wakeup;

static unsigned long test_count = DEFAULT_EXECUTE_NUMBER;

static memcached_return_t counter(const memcached_st *, memcached_result_st *, void *ctx) {
  auto c = static_cast<size_t *>(ctx);
  ++(*c);
  return MEMCACHED_SUCCESS;
}

struct keyval_st {
  struct data {
    std::vector<std::string> chr;
    std::vector<size_t> len;
    explicit data(size_t num)
    : chr(num)
    , len(num) {}
  };

  data key;
  size_t num;
  random64 rnd;
  static constexpr size_t KEY_SIZE = 16;
  static constexpr size_t VALUE_SIZE = 32;

  explicit keyval_st(size_t num_)
  : key{num_}
  , num{num_}
  , rnd{} {
    for (auto i = 0u; i < num; ++i) {
      std::ostringstream oss;
      oss << "KEY_" << std::setfill('0') << std::setw(11) << i;
      key.chr[i] = oss.str();
      key.len[i] = key.chr[i].length();
    }
  }

  // ~keyval_st() {
  //   for (auto i = 0u; i < num; ++i) {
  //     delete key.chr[i];
  //     delete val.chr[i];
  //   }
  // }
};

typedef struct {
  unsigned long hit_num, miss_num, retrieved;
  time_format_us cache_lookup_duration, db_lookup_duration; // avg time for cache lookup vs cache+db lookup
  time_format_us thread_elapsed; // total thread execution time
} stats;

class thread_context {
public:
  thread_context(const client_options &opt_, const memcached_st &memc_, const keyval_st &kv_)
  : opt{opt_}
  , kv{kv_}
  , count{}
  , root(memc_)
  , memc{}
  , _stats{0,0,0,time_format_us(0),time_format_us(0)}
  , thread([this] { execute(); })
  {}

  ~thread_context() {
    if (conn) {
      PQfinish(conn);
      conn = nullptr;
    }
 }

  size_t complete() {
    thread.join();
    return count;
  }

  bool init() {
    // clone memcached connection
    memcached_clone(&memc, &root);

    // open postgres connection
    if (!opt.postgres.host || !opt.postgres.dbname) {
      // PostgreSQL connection is optional
      return true;
    }

    std::string conninfo =
      std::string("host=") + opt.postgres.host +
      " port=" + opt.postgres.port +
      " dbname=" + opt.postgres.dbname;

    if (opt.postgres.user) {
      conninfo += std::string(" user=") + opt.postgres.user;
    }
    if (opt.postgres.password) {
      conninfo += std::string(" password=") + opt.postgres.password;
    }

    conn = PQconnectdb(conninfo.c_str());

    if (PQstatus(conn) != CONNECTION_OK) {
      if (!opt.isset("quiet")) {
        std::cerr << "PostgreSQL connection failed: "
                  << PQerrorMessage(conn) << "\n";
      }
      PQfinish(conn);
      conn = nullptr;
      return false;
    }

    // Prepare our parameterized query for cache-aside lookups
    PGresult *res = PQprepare(conn,
                              "cache_lookup",
                              "SELECT value FROM test WHERE key = $1",
                              1,  // 1 parameter
                              NULL); // Let server infer parameter type

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
      if (!opt.isset("quiet")) {
        std::cerr << "Failed to prepare query: "
                  << PQerrorMessage(conn) << "\n";
      }
      PQclear(res);
      PQfinish(conn);
      conn = nullptr;
      return false;
    }

    PQclear(res);
    return true;
  }

  // warmup cache: this may rewrite keys if memcached does not have enough memory
  // WARNING: do not call on all threads
  int init_cache(unsigned long num, unsigned long index) {
    // For each execution, randomly select from our pool of keys
    for (auto i = 0u; i < kv.num; ++i) {
      if (i%num != index){
        continue; // only deal with part of the keys
      }
      // Query PostgreSQL
      std::string query = "SELECT value FROM test WHERE key = $1";
      const char *param_values[1] = {kv.key.chr[i].data()};
      const int param_lengths[1] = {static_cast<int>(kv.key.chr[i].size())};
      const int param_formats[1] = {0}; // text format

      PGresult *res = PQexecParams(conn, query.data(), 1, nullptr, param_values,
                                   param_lengths, param_formats, 0);

      if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        std::cerr << "ERROR: key " << kv.key.chr[i] << " not found in database" << std::endl;
        PQclear(res);
        return -1;
      }

      if (PQntuples(res) > 0) {
        std::string pg_value = PQgetvalue(res, 0, 0);
        memcached_return_t rc = memcached_set(&memc, kv.key.chr[i].data(), kv.key.chr[i].size(),
                                              pg_value.data(), pg_value.size(), 0, 0);
        if (rc != MEMCACHED_SUCCESS && opt.isset("verbose")) {
          std::cerr << "WARNING: storing key " << kv.key.chr[i] << " in cache failed with error: "
                    <<  memcached_strerror(&memc, rc) << std::endl;
        }
      }
      PQclear(res);
    }

    return 0;
  }

  void execute_get() {
    random64 rnd{};

    auto thread_start = time_clock::now();

    // For each execution, randomly select from our pool of keys
    for (auto i = 0u; i < test_count; ++i) {
      memcached_return_t rc;
      auto r = rnd(0, kv.num); // Select random key from our pool

      auto start = time_clock::now();
      free(memcached_get(&memc, kv.key.chr[r].data(), kv.key.chr[r].size(), nullptr, nullptr,
                         &rc));
      ++_stats.retrieved;

      if (rc == MEMCACHED_SUCCESS) {
        if (opt.isset("verbose")) {
          std::cout << "FOUND KEY "  << kv.key.chr[r] << " IN CACHE" << std::endl;
        }
        ++_stats.hit_num;
        auto elapsed = time_clock::now() - start;
        _stats.cache_lookup_duration = _stats.cache_lookup_duration + elapsed;
        continue;
      }

      if (opt.isset("verbose")) {
        std::cout << "NOT FOUND KEY "  << kv.key.chr[r] << " IN CACHE" << std::endl;
      }

      ++_stats.miss_num;
      auto restart = time_clock::now();

      // Cache miss - query PostgreSQL
      std::string query = "SELECT value FROM test WHERE key = $1";
      const char *param_values[1] = {kv.key.chr[r].data()};
      const int param_lengths[1] = {static_cast<int>(kv.key.chr[r].size())};
      const int param_formats[1] = {0}; // text format

      PGresult *res = PQexecParams(conn, query.data(), 1, nullptr, param_values,
                                   param_lengths, param_formats, 0);

      if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        std::cerr << "WARNING: key " << kv.key.chr[r] << " not found in database" << std::endl;
        auto elapsed = time_clock::now() - restart;
        _stats.db_lookup_duration += elapsed;
        PQclear(res);
        continue;
      }

      if (opt.isset("verbose")) {
        std::cout << "STORING KEY IN CACHE: " << kv.key.chr[r] << std::endl;
      }

      if (PQntuples(res) > 0) {
        std::string pg_value = PQgetvalue(res, 0, 0);
        rc = memcached_set(&memc, kv.key.chr[r].data(), kv.key.chr[r].size(),
                           pg_value.data(), pg_value.size(), 0, 0);

        if (rc != MEMCACHED_SUCCESS) {
          // if (rc != MEMCACHED_SUCCESS && opt.isset("verbose")) {
          std::cerr << "WARNING: storing key " << kv.key.chr[i] << " in cache failed with error: "
                    <<  memcached_strerror(&memc, rc) << std::endl;
        }
      }
      auto reelapsed = time_clock::now() - restart;
      _stats.db_lookup_duration += reelapsed;
      PQclear(res);
    }

    _stats.thread_elapsed = time_clock::now() - thread_start;
  }

  stats get_stats(){return _stats;}

private:
  const client_options &opt;
  const keyval_st &kv;
  size_t count;
  const memcached_st &root;
  memcached_st memc;
  std::thread thread;
  PGconn *conn;
  stats _stats;

  void execute() {
    while (!wakeup.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    execute_get();
  }
};

using opt_apply = std::function<bool(const client_options &,
                                     const client_options::extended_option &ext, memcached_st *)>;

static opt_apply wrap_stoul(unsigned long &ul) {
  return [&ul](const client_options &, const client_options::extended_option &ext, memcached_st *) {
    if (ext.arg && *ext.arg) {
      auto c = std::stoul(ext.arg);
      if (c) {
        ul = c;
      }
    }
    return true;
  };
}

static std::ostream &align(std::ostream &io) {
  return io << std::right << std::setw(8);
}

int main(int argc, char *argv[]) {
  client_options opt{PROGRAM_NAME, PROGRAM_VERSION, PROGRAM_DESCRIPTION};
  auto concurrency = DEFAULT_CONCURRENCY;
  auto load_count = DEFAULT_INITIAL_LOAD;

  for (const auto &def : opt.defaults) {
    opt.add(def);
  }

  opt.add("noreply", 'R', no_argument, "Enable the NOREPLY behavior for storage commands.").apply =
      [](const client_options &opt_, const client_options::extended_option &ext,
         memcached_st *memc) {
        if (MEMCACHED_SUCCESS != memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_NOREPLY, ext.set))
        {
          if (!opt_.isset("quiet")) {
            std::cerr << memcached_last_error_message(memc);
          }
          return false;
        }
        return true;
      };
  opt.add("udp", 'X', no_argument, "Use UDP.").apply =
    [](const client_options &opt_, const client_options::extended_option &ext,
       memcached_st *memc) {
      if (MEMCACHED_SUCCESS != memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_USE_UDP, ext.set))
        {
          if (!opt_.isset("quiet")) {
            std::cerr << memcached_last_error_message(memc) << "\n";
          }
          return false;
        }
      return true;
    };
  opt.add("flush", 'F', no_argument, "Flush all servers prior test.");
  opt.add("test", 't', required_argument, "Test to perform (options: get,mget,set; default: get).");
  opt.add("concurrency", 'c', required_argument,
          "Concurrency (number of threads to start; default: 1).")
      .apply = wrap_stoul(concurrency);
  opt.add("execute-number", 'e', required_argument,
          "Number of times to execute the tests (default: 10000).")
      .apply = wrap_stoul(test_count);
  opt.add("initial-load", 'l', required_argument,
          "Number of keys to load before executing tests (default: 10000)."
          "\n\t\tDEPRECATED: --execute-number takes precedence.")
      .apply = wrap_stoul(load_count);

  char set[] = "set";
  opt.set("test", true, set);

  if (!opt.parse(argc, argv)) {
    exit(EXIT_FAILURE);
  }

  memcached_st memc;
  if (!check_memcached(opt, memc)) {
    exit(EXIT_FAILURE);
  }

  if (!opt.apply(&memc)) {
    memcached_free(&memc);
    exit(EXIT_FAILURE);
  }

  auto total_start = time_clock::now();
  std::cout << std::fixed << std::setprecision(3);

  //------- FLUSH

  if (opt.isset("flush")) {
    if (opt.isset("verbose")) {
      std::cout << "- Flushing servers ...\n";
    }
    auto flush_start = time_clock::now();
    auto rc = memcached_flush(&memc, 0);
    auto flush_elapsed = time_clock::now() - flush_start;
    if (!memcached_success(rc)) {
      if (!opt.isset("quiet")) {
        std::cerr << "Failed to FLUSH: " << memcached_last_error_message(&memc) << "\n";
      }
      memcached_free(&memc);
      exit(EXIT_FAILURE);
    }
    if (!opt.isset("quiet")) {
      std::cout << "Time to flush        " << align << memcached_server_count(&memc)
                << " servers:               " << align << time_format(flush_elapsed).count()
                << " seconds.\n";
    }
  }

  //------- GENERATE KEYS

  if (opt.isset("verbose")) {
    std::cout << "- Generating 16 byte keys with 32 byte data for " << opt.num_keys
              << " keys ...\n";
  }
  auto keyval_start = time_clock::now();
  keyval_st kv{opt.num_keys};
  auto keyval_elapsed = time_clock::now() - keyval_start;

  if (!opt.isset("quiet")) {
    std::cout << "Time to generate     " << align << opt.num_keys
              << " test keys:             " << align << time_format(keyval_elapsed).count()
              << " seconds.\n";
  }

  //------- INIT

  if (opt.isset("verbose")) {
    std::cout << "- Starting " << concurrency << " threads ...\n";
  }
  auto thread_start = time_clock::now();
  std::vector<thread_context *> threads{};
  threads.reserve(concurrency);
  for (auto i = 0ul; i < concurrency; ++i) {
    auto t = new thread_context(opt, memc, kv);
    if (!t->init()) {
      exit(EXIT_FAILURE);
    }
    threads.push_back(t);
  }
  auto thread_elapsed = time_clock::now() - thread_start;
  if (!opt.isset("quiet")) {
    std::cout << "Time to start        " << align << concurrency
              << " threads:                  " << time_format(thread_elapsed).count()
              << " seconds.\n";
  }

  //------- WARMUP

  if (opt.isset("verbose")) {
    std::cout << "- Warming up cache for " << opt.num_keys
              << " keys ...\n";
  }
  keyval_start = time_clock::now();
  for (auto i = 0ul; i < concurrency; ++i) {
    if (threads[i]->init_cache(concurrency, i) < 0) {
      if (!opt.isset("quiet")) {
        std::cerr << "Failed to warmup cache at thread " << i << " out of " << concurrency << "threads\n";
      }
      memcached_free(&memc);
      exit(EXIT_FAILURE);
  }
  }
  keyval_elapsed = time_clock::now() - keyval_start;

  if (!opt.isset("quiet")) {
    std::cout << "Time to warmup cache " << align << opt.num_keys
              << " test keys:             " << align << time_format(keyval_elapsed).count()
              << " seconds.\n";
  }

  //------- TEST

  if (opt.isset("verbose")) {
    std::cout << "- Starting test: " << test_count << " x " << opt.argof("test") << " x "
              << concurrency << " ...\n";
  }
  auto count = 0ul;
  auto test_start = time_clock::now();
  wakeup.store(true, std::memory_order_release);

  if (!opt.isset("quiet")) {
    std::cout << "--------------------------------------------------------------------\n";
  }
  unsigned long hit_num=0, miss_num=0, i=1;
  double retrieved=0.0;
  double cache_lookup_time = 0.0, db_lookup_time = 0.0;
  for (auto &thread : threads) {
    count += thread->complete();
    auto stats = thread->get_stats();
    // std::cout << stats.hit_num << ", " << stats.miss_num << ", "
    //           << stats.retrieved << ", " << stats.cache_lookup_duration.count() << ", "
    //           << stats.db_lookup_duration.count() << std::endl;
    hit_num += stats.hit_num;
    miss_num += stats.miss_num;
    retrieved += (double)stats.retrieved;
    cache_lookup_time += time_format_us(stats.cache_lookup_duration).count() / stats.hit_num;
    db_lookup_time += time_format_us(stats.db_lookup_duration).count() / stats.miss_num;

    if (!opt.isset("quiet")) {
      std::cout << "Thread " << i++ << "stats: #hits=" << stats.hit_num << " (rate=" << float(stats.hit_num*100)/float(stats.retrieved)
                << "%), #miss="  << stats.miss_num << " (rate=" << float(stats.miss_num*100)/float(stats.retrieved)
                << "%), #avg_cache_lookup_time="  << time_format_us(stats.cache_lookup_duration).count() / stats.hit_num
                << "us, #avg_db_lookup_time="  << time_format_us(stats.db_lookup_duration).count() / stats.miss_num
                << "us, #thread_elapsed_time="  << time_format(stats.thread_elapsed).count()
                << "s" << std::endl;
    }

    delete thread;
  }
  cache_lookup_time /= concurrency;
  db_lookup_time /= concurrency;
  auto test_elapsed = time_clock::now() - test_start;

  if (!opt.isset("quiet")) {
    std::cout << "--------------------------------------------------------------------\n"
              << "Time to make " <<  std::setw(6) << std::scientific << retrieved << " get queries by "
              << std::fixed << concurrency << " threads:    "
              << align << time_format(test_elapsed).count() << " seconds.\n";

    std::cout << "Stats: #hits=" << hit_num << " (rate=" << float(hit_num*100)/float(retrieved)
              << "%), #miss="  << miss_num << " (rate=" << float(miss_num*100)/float(retrieved)
              << "%), #avg_cache_lookup_time="  << cache_lookup_time
              << "us, #avg_db_lookup_time="  << db_lookup_time
              << "us" << std::endl;

    std::cout << "--------------------------------------------------------------------\n"
              << "Time total:                                    " << align << std::setw(12)
              << time_format(time_clock::now() - total_start).count() << " seconds.\n";
  }

  memcached_free(&memc);
  exit(EXIT_SUCCESS);
}
