#define ENABLE_NLS

extern "C" {
#include <postgres.h>

#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <miscadmin.h>
#include <utils/builtins.h>
}
#include <algorithm>
#include <cassert>
#include <exception>
#include <filesystem>
#include <fstream>
#include <ranges>
#include <stdexcept>

#include "tpch_constants.hpp"
#include "tpch_dsdgen.h"
#include "tpch_wrapper.hpp"

namespace tpch {

static auto get_extension_external_directory(void) {
  char sharepath[MAXPGPATH];

  get_share_path(my_exec_path, sharepath);
  return std::string(sharepath) + "/extension/tpch";
}

class Executor {
 public:
  Executor(const Executor &other) = delete;
  Executor &operator=(const Executor &other) = delete;

  Executor() {
    if (SPI_connect() != SPI_OK_CONNECT)
      throw std::runtime_error("SPI_connect Failed");
  }

  ~Executor() { SPI_finish(); }

  void execute(const std::string &query) const {
    if (auto ret = SPI_exec(query.c_str(), 0); ret < 0)
      throw std::runtime_error("SPI_exec Failed, get " + std::to_string(ret));
  }
};

[[maybe_unused]] static double exec_spec(const auto &path, const Executor &executor) {
  if (std::filesystem::exists(path)) {
    std::ifstream file(path);
    std::string sql((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    const auto start = std::chrono::high_resolution_clock::now();
    executor.execute(sql);
    const auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
  }
  return 0;
}

void TPCHWrapper::CreateTPCHSchema() {
  const std::filesystem::path extension_dir = get_extension_external_directory();

  Executor executor;

  exec_spec(extension_dir / "pre_prepare.sql", executor);

  auto schema = extension_dir / "schema";
  if (std::filesystem::exists(schema)) {
    std::ranges::for_each(std::filesystem::directory_iterator(schema), [&](const auto &entry) {
      std::ifstream file(entry.path());
      std::string sql((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
      executor.execute(sql);
    });
  } else
    throw std::runtime_error("Schema file does not exist");

  exec_spec(extension_dir / "post_prepare.sql", executor);
}

uint32_t TPCHWrapper::QueriesCount() {
  return TPCH_QUERIES_COUNT;
}

const char *TPCHWrapper::GetQuery(int query) {
  if (query <= 0 || query > TPCH_QUERIES_COUNT) {
    throw std::runtime_error("Out of range TPC-H query number " + std::to_string(query));
  }

  const std::filesystem::path extension_dir = get_extension_external_directory();

  char qname[16]; snprintf(qname, sizeof(qname), "%02d.sql", query);
  auto queries = extension_dir / "queries" / qname;
  if (std::filesystem::exists(queries)) {
    std::ifstream file(queries);
    std::string sql((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

    return strdup(sql.c_str());
  }
  throw std::runtime_error("Queries file does not exist");
}

tpch_runner_result *TPCHWrapper::RunTPCH(int qid) {
  if (qid < 0 || qid > TPCH_QUERIES_COUNT) {
    throw std::runtime_error("Out of range TPC-H query number " + std::to_string(qid));
  }

  const std::filesystem::path extension_dir = get_extension_external_directory();

  char qname2[16]; snprintf(qname2, sizeof(qname2), "%02d.sql", qid);
  auto queries = extension_dir / "queries" / qname2;

  if (!std::filesystem::exists(queries))
    throw std::runtime_error("Queries file for qid: " + std::to_string(qid) + " does not exist");

  // Run query inside its own scope so Executor destructor (SPI_finish) fires
  // before palloc — palloc must happen in the outer (caller's) memory context,
  // not the SPI context that SPI_finish would free.
  double duration;
  {
    Executor executor;
    duration = exec_spec(queries, executor);
  }

  auto *result = (tpch_runner_result *)palloc(sizeof(tpch_runner_result));
  result->qid = qid;
  result->duration = duration;
  result->checked = true;

  return result;
}

std::pair<int, int> TPCHWrapper::DBGen(double scale, char *table, int children, int step) {
  if (step >= children)
    return {0, 0};

  const std::filesystem::path extension_dir = get_extension_external_directory();

#define CALL_GENTBL(tbl, tbl_id, fuc)                                                  \
  if (strcasecmp(table, #tbl) == 0) {                                                  \
    TPCHTableGenerator generator(scale, table, tbl_id, children, step, extension_dir); \
    return generator.fuc();                                                            \
  }

  CALL_GENTBL(customer, CUST, generate_customer)
  CALL_GENTBL(nation, NATION, generate_nation)
  CALL_GENTBL(region, REGION, generate_region)
  CALL_GENTBL(supplier, SUPP, generate_supplier)
  CALL_GENTBL(orders, ORDER_LINE, generate_orders_and_lineitem)
  CALL_GENTBL(part, PART_PSUPP, generate_part_and_partsupp)

#undef CALL_GENTBL

  if (strcasecmp(table, "lineitem") || strcasecmp(table, "partsupp"))
    throw std::runtime_error(
        "Table lineitem is a child; it is populated during the build of its parent");

  throw std::runtime_error(std::string("Table ") + table + " does not exist");
}  // namespace tpch

}  // namespace tpch
