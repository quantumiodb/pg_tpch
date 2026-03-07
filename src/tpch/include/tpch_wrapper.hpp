
#pragma once

#include <cstdint>
#include <string>

namespace tpch {

struct tpch_runner_result {
  int qid;
  double duration;
  bool checked;
};

struct TPCHWrapper {
  static std::pair<int, int> DBGen(double scale, char* table, int children, int step);

  static uint32_t QueriesCount();
  static const char* GetQuery(int query);

  static void CreateTPCHSchema();

  static tpch_runner_result* RunTPCH(int qid);

  static int CollectAnswers();
};

}  // namespace tpch
