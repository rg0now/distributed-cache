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

      // std::cout << "I: " << i << std::endl;
      // std::cout << "K: " << key.chr[i] << ", len: " << key.len[i] << std::endl;
    }
  }

  // ~keyval_st() {
  //   for (auto i = 0u; i < num; ++i) {
  //     delete key.chr[i];
  //     delete val.chr[i];
  //   }
  // }
};

// warmup cache: this may rewrite keys if memcached does not have enough memory
static int init_cache(const client_options &opt, memcached_st &memc, const keyval_st &kv) {
  // For each execution, randomly select from our pool of keys
  for (auto i = 0u; i < kv.num; ++i) {
    // Query PostgreSQL
    std::string query = "SELECT value FROM test WHERE key = $1";
    const char *param_values[1] = {kv.key.chr[i].data()};
    const int param_lengths[1] = {static_cast<int>(kv.key.chr[i].size())};
    const int param_formats[1] = {0}; // text format

    PGresult *res = PQexecParams(opt.postgres.conn, query.data(), 1, nullptr, param_values,
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
      if (rc != MEMCACHED_SUCCESS) {
        std::cerr << "ERROR: key " << kv.key.chr[i] << " could not be stored in cache" << std::endl;
        PQclear(res);
        return -1;
      }
    }
    PQclear(res);
  }

  return 0;
}

static size_t execute_get(const client_options &opt, memcached_st &memc, const keyval_st &kv) {
  size_t retrieved = 0;
  random64 rnd{};

  // For each execution, randomly select from our pool of keys
  for (auto i = 0u; i < test_count; ++i) {
    memcached_return_t rc;
    auto r = rnd(0, kv.num); // Select random key from our pool

    // std::cout << "R: " << r << std::endl;
    // std::cout << "K: " << kv.key.chr[r] << ", len: " << kv.key.len[r] << std::endl;

    free(memcached_get(&memc, kv.key.chr[r].data(), kv.key.chr[r].size(), nullptr, nullptr,
                       &rc));

    if (check_return(opt, memc, kv.key.chr[r].data(), rc)) {
      // std::cout << "FOUND: " << kv.key.chr[r] << std::endl;
      ++retrieved;
    } else {
      // std::cout << "NOT FOUND: " << kv.key.chr[r] << std::endl;
    }

    if (rc == MEMCACHED_SUCCESS) {
      continue;
    }

    // Cache miss - query PostgreSQL
    std::string query = "SELECT value FROM test WHERE key = $1";
    const char *param_values[1] = {kv.key.chr[r].data()};
    const int param_lengths[1] = {static_cast<int>(kv.key.chr[r].size())};
    const int param_formats[1] = {0}; // text format

    PGresult *res = PQexecParams(opt.postgres.conn, query.data(), 1, nullptr, param_values,
                                 param_lengths, param_formats, 0);

    if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
      std::cerr << "WARNING: key " << kv.key.chr[r] << " not found in database" << std::endl;
      PQclear(res);
      continue;
    }

    if (PQntuples(res) > 0) {
      std::string pg_value = PQgetvalue(res, 0, 0);

      // std::cout << "V: " << pg_value.c_str() << std::endl;

      // Write to memcached
      rc = memcached_set(&memc, kv.key.chr[r].data(), kv.key.chr[r].size(),
                         pg_value.data(), pg_value.size(), 0, 0);

      if (rc != MEMCACHED_SUCCESS) {
        std::cerr << "WARNING: key " << kv.key.chr[r] << " could not be stored in cache" << std::endl;
        continue;
      }
    }

    PQclear(res);
    ++retrieved;
  }

  return retrieved;
}

class thread_context {
public:
  thread_context(const client_options &opt_, const memcached_st &memc_, const keyval_st &kv_)
  : opt{opt_}
  , kv{kv_}
  , count{}
  , root(memc_)
  , memc{}
  , thread([this] { execute(); }) {}

  ~thread_context() {
    memcached_free(&memc);
  }

  size_t complete() {
    thread.join();
    return count;
  }

private:
  const client_options &opt;
  const keyval_st &kv;
  size_t count;
  const memcached_st &root;
  memcached_st memc;
  std::thread thread;

  void execute() {
    memcached_clone(&memc, &root);

    while (!wakeup.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }

    count = execute_get(opt, memc, kv);
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

  // Initialize PostgreSQL connection if host and db are specified
  if (!opt.connect_postgres()) {
    memcached_free(&memc);
    exit(EXIT_FAILURE);
  }

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
      std::cout << "Time to flush      " << align << memcached_server_count(&memc)
                << " servers:               " << align << time_format(flush_elapsed).count()
                << " seconds.\n";
    }
  }

  if (opt.isset("verbose")) {
    std::cout << "- Generating 16 byte keys with 32 byte data for " << opt.num_keys
              << " keys ...\n";
  }
  auto keyval_start = time_clock::now();
  keyval_st kv{opt.num_keys};
  auto keyval_elapsed = time_clock::now() - keyval_start;

  if (!opt.isset("quiet")) {
    std::cout << "Time to generate   " << align << opt.num_keys
              << " test keys:             " << align << time_format(keyval_elapsed).count()
              << " seconds.\n";
  }

  if (opt.isset("verbose")) {
    std::cout << "- Initializing cache for " << opt.num_keys
              << " keys ...\n";
  }
  keyval_start = time_clock::now();
  if (init_cache(opt, memc, kv) < 0) {
      if (!opt.isset("quiet")) {
        std::cerr << "Failed to init cache\n";
      }
      memcached_free(&memc);
      exit(EXIT_FAILURE);
  }
  keyval_elapsed = time_clock::now() - keyval_start;

  if (!opt.isset("quiet")) {
    std::cout << "Time to init cache " << align << opt.num_keys
              << " test keys:             " << align << time_format(keyval_elapsed).count()
              << " seconds.\n";
  }

  if (opt.isset("verbose")) {
    std::cout << "- Starting " << concurrency << " threads ...\n";
  }
  auto thread_start = time_clock::now();
  std::vector<thread_context *> threads{};
  threads.reserve(concurrency);
  for (auto i = 0ul; i < concurrency; ++i) {
    threads.push_back(new thread_context(opt, memc, kv));
  }
  auto thread_elapsed = time_clock::now() - thread_start;
  if (!opt.isset("quiet")) {
    std::cout << "Time to start      " << align << concurrency
              << " threads:                  " << time_format(thread_elapsed).count()
              << " seconds.\n";
  }
  if (opt.isset("verbose")) {
    std::cout << "- Starting test: " << test_count << " x " << opt.argof("test") << " x "
              << concurrency << " ...\n";
  }
  auto count = 0ul;
  auto test_start = time_clock::now();
  wakeup.store(true, std::memory_order_release);
  for (auto &thread : threads) {
    count += thread->complete();
    delete thread;
  }
  auto test_elapsed = time_clock::now() - test_start;

  if (!opt.isset("quiet")) {
    std::cout << "--------------------------------------------------------------------\n"
              << "Time to get" << "        " << align << count << " keys by " << std::setw(4)
              << concurrency << " threads:  " << align << time_format(test_elapsed).count()
              << " seconds.\n";

    std::cout << "--------------------------------------------------------------------\n"
              << "Time total:                                    " << align << std::setw(12)
              << time_format(time_clock::now() - total_start).count() << " seconds.\n";
  }
  exit(EXIT_SUCCESS);
}
