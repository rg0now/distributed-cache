#include <iostream>
#include <iomanip>
#include <sstream>
#include <cstring>

#include <postgresql/libpq-fe.h>

int NUM_KEYS = 1000000;

std::string pad_index(int index, const char *prefix, int padding) {
  std::ostringstream oss;
  oss << prefix << std::setfill('0') << std::setw(padding) << index;
  return oss.str();
}

int main(int argc, char *argv[]) {
  if (argc > 1) {
    try {
      NUM_KEYS = std::stoi(argv[1]);
    } catch (const std::exception &e) {
      std::cerr << "Invalid number of keys. Using default: " << NUM_KEYS << std::endl;
    }
  }

  PGconn *conn = PQconnectdb("host=localhost "
                             "port=5432 "
                             "dbname=test "
                             "user=postgres "
                             "password=test");

  if (PQstatus(conn) != CONNECTION_OK) {
    std::cerr << "Connection failed: " << PQerrorMessage(conn) << std::endl;
    PQfinish(conn);
    return 1;
  }

  // flush
  PGresult *res = PQexec(conn, "TRUNCATE TABLE test;");
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    std::cerr << "Drop DB failed (ignored): " << PQerrorMessage(conn) << std::endl;
  }
  PQclear(res);

  res = PQexec(conn,
                         "CREATE TABLE IF NOT EXISTS test ("
                         "    key TEXT PRIMARY KEY, "
                         "    value TEXT NOT NULL"
                         ");"
                         "CREATE INDEX IF NOT EXISTS test_key_idx ON test(key);");
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    std::cerr << "Table creation failed: " << PQerrorMessage(conn) << std::endl;
    PQclear(res);
    PQfinish(conn);
    return 1;
  }
  PQclear(res);

  // prepare
  PGresult *prep_res =
      PQprepare(conn, "insert_key", "INSERT INTO test (key, value) VALUES ($1, $2)", 2, NULL);
  if (PQresultStatus(prep_res) != PGRES_COMMAND_OK) {
    std::cerr << "Prepare failed: " << PQerrorMessage(conn) << std::endl;
    PQclear(prep_res);
    PQfinish(conn);
    return 1;
  }
  PQclear(prep_res);

  const char *paramValues[2];

  for (int i = 0; i < NUM_KEYS; ++i) {
    if((i+1) % 1000 == 0){
        std::cout << "Inserted " << i << " keys" << std::endl;
      }
 
    std::string key = pad_index(i, "KEY_", 11); // pad to 15 bytes
    std::string value = pad_index(i, "VALUE_", 25); // pad to 31 bytes

    paramValues[0] = key.c_str();
    paramValues[1] = value.c_str();

    res = PQexecPrepared(conn, "insert_key",
                         2, // number of parameters
                         paramValues,
                         NULL, // parameter lengths (NULL for text)
                         NULL, // parameter formats
                         0);   // result format

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
      std::cerr << "Insert failed: " << PQerrorMessage(conn) << std::endl;
      PQclear(res);
      PQfinish(conn);
      return 1;
    }
    PQclear(res);
  }

  res = PQexec(conn, "COMMIT");
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    std::cerr << "COMMIT failed: " << PQerrorMessage(conn) << std::endl;
    PQclear(res);
    PQfinish(conn);
    return 1;
  }
  PQclear(res);

  PQfinish(conn);
  std::cout << "Inserted " << NUM_KEYS << " keys successfully." << std::endl;

  return 0;
}
