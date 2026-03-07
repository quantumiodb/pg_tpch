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

  std::vector<std::string> execute_and_capture(const std::string &query) const {
    if (auto ret = SPI_exec(query.c_str(), 0); ret < 0)
      throw std::runtime_error("SPI_exec Failed, get " + std::to_string(ret));

    std::vector<std::string> rows;
    if (SPI_tuptable && SPI_processed > 0) {
      SPITupleTable *tuptable = SPI_tuptable;
      TupleDesc tupdesc = tuptable->tupdesc;
      int natts = tupdesc->natts;

      rows.reserve(SPI_processed);
      for (uint64 i = 0; i < SPI_processed; i++) {
        HeapTuple tuple = tuptable->vals[i];
        std::string row;
        for (int col = 1; col <= natts; col++) {
          if (col > 1) row += '|';
          char *val = SPI_getvalue(tuple, tupdesc, col);
          if (val) {
            row += val;
            pfree(val);
          }
        }
        rows.push_back(std::move(row));
      }
      std::sort(rows.begin(), rows.end());
    }
    return rows;
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

struct exec_result {
  double duration;
  std::vector<std::string> rows;
};

[[maybe_unused]] static exec_result exec_spec_capture(const auto &path, const Executor &executor) {
  if (std::filesystem::exists(path)) {
    std::ifstream file(path);
    std::string sql((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    const auto start = std::chrono::high_resolution_clock::now();
    auto rows = executor.execute_and_capture(sql);
    const auto end = std::chrono::high_resolution_clock::now();
    double dur = std::chrono::duration<double, std::milli>(end - start).count();
    return {dur, std::move(rows)};
  }
  return {0.0, {}};
}

static std::vector<std::string> load_answer_file(const std::filesystem::path &path) {
  std::vector<std::string> lines;
  if (!std::filesystem::exists(path))
    return lines;

  std::ifstream file(path);
  std::string line;
  while (std::getline(file, line)) {
    if (!line.empty())
      lines.push_back(std::move(line));
  }
  std::sort(lines.begin(), lines.end());
  return lines;
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
  tpch_runner_result tmp;
  tmp.qid = qid;
  std::vector<std::string> actual_rows;
  {
    Executor executor;
    auto er = exec_spec_capture(queries, executor);
    tmp.duration = er.duration;
    actual_rows = std::move(er.rows);
  }

  // Validate against answer file
  char aname[16];
  snprintf(aname, sizeof(aname), "%02d.ans", qid);
  auto answer_path = std::filesystem::path(TPCH_ANSWERS_DIR) / aname;

  if (!std::filesystem::exists(answer_path)) {
    tmp.checked = false;
  } else {
    auto expected_rows = load_answer_file(answer_path);
    tmp.checked = (actual_rows == expected_rows);
  }

  auto *result = (tpch_runner_result *)palloc(sizeof(tpch_runner_result));
  *result = tmp;
  return result;
}

int TPCHWrapper::CollectAnswers() {
  const std::filesystem::path extension_dir = get_extension_external_directory();
  auto ans_dir = std::filesystem::path(TPCH_ANSWERS_DIR);

  // Skip if answer files already exist
  if (std::filesystem::exists(ans_dir)) {
    int existing = 0;
    for (auto &e : std::filesystem::directory_iterator(ans_dir)) {
      if (e.path().extension() == ".ans") existing++;
    }
    if (existing >= (int)TPCH_QUERIES_COUNT)
      return 0;
  }

  std::filesystem::create_directories(ans_dir);

  Executor executor;

  // Use planner (optimizer=off) for deterministic baseline results
  executor.execute("SET optimizer = off");

  int count = 0;
  for (int qid = 1; qid <= (int)TPCH_QUERIES_COUNT; qid++) {
    char qname[16];
    snprintf(qname, sizeof(qname), "%02d.sql", qid);
    auto qpath = extension_dir / "queries" / qname;
    if (!std::filesystem::exists(qpath)) continue;

    char aname[16];
    snprintf(aname, sizeof(aname), "%02d.ans", qid);
    auto apath = ans_dir / aname;

    if (std::filesystem::exists(apath)) { count++; continue; }

    std::ifstream qfile(qpath);
    std::string sql((std::istreambuf_iterator<char>(qfile)), std::istreambuf_iterator<char>());
    auto rows = executor.execute_and_capture(sql);

    std::ofstream out(apath);
    for (const auto &row : rows)
      out << row << '\n';
    out.close();
    count++;
  }

  executor.execute("RESET optimizer");
  return count;
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
