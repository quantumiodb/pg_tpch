#include <algorithm>
#include <chrono>
#include <climits>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#define DECLARER

#include "dbgen_gunk.hpp"
#include "tpch_constants.hpp"
#include "tpch_dsdgen.h"

extern "C" {
#include <postgres.h>

#include <access/table.h>
#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <miscadmin.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
}

#include "dss.h"
#include "dsstypes.h"

namespace tpch {

template <typename DSSType, typename MKRetType, typename... Args>
void call_dbgen_mk(size_t idx, DSSType& value, MKRetType (*mk_fn)(DSS_HUGE, DSSType* val, DBGenContext* ctx, Args...),
                   int dbgen_table_id, DBGenContext* ctx, Args... args) {
  row_start(dbgen_table_id, ctx);

  mk_fn(idx, &value, ctx, std::forward<Args>(args)...);

  row_stop_h(dbgen_table_id, ctx);
}

void skip(int table, int children, DSS_HUGE step, DBGenContext& dbgen_ctx) {
  switch (table) {
    case CUST:
      sd_cust(children, step, &dbgen_ctx);
      break;
    case SUPP:
      sd_supp(children, step, &dbgen_ctx);
      break;
    case NATION:
      sd_nation(children, step, &dbgen_ctx);
      break;
    case REGION:
      sd_region(children, step, &dbgen_ctx);
      break;
    case ORDER_LINE:
      sd_line(children, step, &dbgen_ctx);
      sd_order(children, step, &dbgen_ctx);
      break;
    case PART_PSUPP:
      sd_part(children, step, &dbgen_ctx);
      sd_psupp(children, step, &dbgen_ctx);
      break;
  }
}

TPCHTableGenerator::TPCHTableGenerator(double scale_factor, const std::string& table, int table_id, int children,
                                       int step, std::filesystem::path resource_dir, int rng_seed)
    : table_{std::move(table)}, table_id_{table_id} {
  tdef* tdefs = ctx_.tdefs;
  tdefs[PART].base = 200000;
  tdefs[PSUPP].base = 200000;
  tdefs[SUPP].base = 10000;
  tdefs[CUST].base = 150000;
  tdefs[ORDER].base = 150000 * ORDERS_PER_CUST;
  tdefs[LINE].base = 150000 * ORDERS_PER_CUST;
  tdefs[ORDER_LINE].base = 150000 * ORDERS_PER_CUST;
  tdefs[PART_PSUPP].base = 200000;
  tdefs[NATION].base = NATIONS_MAX;
  tdefs[REGION].base = NATIONS_MAX;

  if (scale_factor < MIN_SCALE) {
    int i;
    int int_scale;

    ctx_.scale_factor = 1;
    int_scale = (int)(1000 * scale_factor);
    for (i = PART; i < REGION; i++) {
      tdefs[i].base = (DSS_HUGE)(int_scale * tdefs[i].base) / 1000;
      if (ctx_.tdefs[i].base < 1) {
        tdefs[i].base = 1;
      }
    }
  } else {
    ctx_.scale_factor = (long)scale_factor;
  }
  load_dists(10 * 1024 * 1024, &ctx_);  // 10MiB
  tdefs[NATION].base = nations.count;
  tdefs[REGION].base = regions.count;

  {
    if (table_id_ < NATION)
      rowcnt_ = tdefs[table_id_].base * ctx_.scale_factor;
    else
      rowcnt_ = tdefs[table_id_].base;

    if (children > 1 && step != -1) {
      size_t part_size = std::ceil((double)rowcnt_ / (double)children);
      part_offset_ = part_size * step;
      auto part_end = part_offset_ + part_size;
      rowcnt_ = part_end > rowcnt_ ? rowcnt_ - part_offset_ : part_size;
      skip(table_id_, children, part_offset_, ctx_);
    }
  }
}

TPCHTableGenerator::~TPCHTableGenerator() {
  cleanup_dists();
}

class TableLoader {
 public:
  static const int BATCH_SIZE = 1000;

  TableLoader(const std::string& table) : table_{table} {
    reloid_ = DirectFunctionCall1(regclassin, CStringGetDatum(table_.c_str()));
    rel_ = try_table_open(reloid_, AccessShareLock, false);
    if (!rel_)
      throw std::runtime_error("try_table_open Failed");

    auto tupDesc = RelationGetDescr(rel_);
    natts_ = tupDesc->natts;
    Oid in_func_oid;

    in_functions = new FmgrInfo[natts_];
    typioparams = new Oid[natts_];
    out_func_oids_ = new Oid[natts_];
    typisvarlena_ = new bool[natts_];

    for (int attnum = 1; attnum <= natts_; attnum++) {
      Form_pg_attribute att = TupleDescAttr(tupDesc, attnum - 1);

      getTypeInputInfo(att->atttypid, &in_func_oid, &typioparams[attnum - 1]);
      fmgr_info(in_func_oid, &in_functions[attnum - 1]);
      getTypeOutputInfo(att->atttypid, &out_func_oids_[attnum - 1], &typisvarlena_[attnum - 1]);
    }

    slot = MakeSingleTupleTableSlot(tupDesc, &TTSOpsMinimalTuple);
    slot->tts_tableOid = RelationGetRelid(rel_);

    initStringInfo(&batch_buf_);
    batch_count_ = 0;
  };

  ~TableLoader() {
    flush();
    table_close(rel_, AccessShareLock);
    delete[] in_functions;
    delete[] typioparams;
    delete[] out_func_oids_;
    delete[] typisvarlena_;
    ExecDropSingleTupleTableSlot(slot);
    pfree(batch_buf_.data);
  }

  template <typename T>
  auto& addItem(T value) {
    if constexpr (std::is_same_v<T, char*> || std::is_same_v<T, const char*> || std::is_same_v<T, char>)
      slot->tts_values[current_item_] = DirectFunctionCall3(
          in_functions[current_item_].fn_addr, CStringGetDatum(value), ObjectIdGetDatum(typioparams[current_item_]),
          TupleDescAttr(RelationGetDescr(rel_), current_item_)->atttypmod);
    else
      slot->tts_values[current_item_] = value;

    current_item_++;
    return *this;
  }

  auto& start() {
    ExecClearTuple(slot);
    MemSet(slot->tts_values, 0, natts_ * sizeof(Datum));
    /* all tpch table is not null */
    MemSet(slot->tts_isnull, false, natts_ * sizeof(bool));
    current_item_ = 0;
    return *this;
  }

  auto& end() {
    ExecStoreVirtualTuple(slot);

    if (batch_count_ > 0)
      appendStringInfoChar(&batch_buf_, ',');
    appendStringInfoChar(&batch_buf_, '(');

    for (int i = 0; i < natts_; i++) {
      if (i > 0)
        appendStringInfoChar(&batch_buf_, ',');
      if (slot->tts_isnull[i]) {
        appendStringInfoString(&batch_buf_, "NULL");
      } else {
        char* outstr = OidOutputFunctionCall(out_func_oids_[i], slot->tts_values[i]);
        appendStringInfoChar(&batch_buf_, '\'');
        for (char* p = outstr; *p; p++) {
          if (*p == '\'')
            appendStringInfoChar(&batch_buf_, '\'');
          appendStringInfoChar(&batch_buf_, *p);
        }
        appendStringInfoChar(&batch_buf_, '\'');
        pfree(outstr);
      }
    }
    appendStringInfoChar(&batch_buf_, ')');
    batch_count_++;
    row_count_++;

    if (batch_count_ >= BATCH_SIZE)
      flush();

    return *this;
  }

  void flush() {
    if (batch_count_ == 0)
      return;

    StringInfoData sql;
    initStringInfo(&sql);
    appendStringInfo(&sql, "INSERT INTO %s VALUES %s", table_.c_str(), batch_buf_.data);

    SPI_connect();
    int ret = SPI_exec(sql.data, 0);
    SPI_finish();

    if (ret < 0)
      elog(ERROR, "pg_tpch: SPI INSERT failed for table %s: %d", table_.c_str(), ret);

    pfree(sql.data);
    resetStringInfo(&batch_buf_);
    batch_count_ = 0;
  }

  auto row_count() const { return row_count_; }

  Oid reloid_;
  Relation rel_;
  std::string table_;
  size_t row_count_ = 0;
  size_t current_item_ = 0;
  int natts_ = 0;

  FmgrInfo* in_functions;
  Oid* typioparams;
  Oid* out_func_oids_;
  bool* typisvarlena_;
  TupleTableSlot* slot;

  StringInfoData batch_buf_;
  int batch_count_ = 0;
};

std::string convert_money_str(DSS_HUGE cents) {
  char buf[32];
  if (cents < 0) {
    cents = std::abs(cents);
    snprintf(buf, sizeof(buf), "-%lld.%02lld", (long long)(cents / 100), (long long)(cents % 100));
  } else {
    snprintf(buf, sizeof(buf), "%lld.%02lld", (long long)(cents / 100), (long long)(cents % 100));
  }
  return buf;
}

std::string convert_str(char c) {
  return std::string(1, c);
}

std::pair<int, int> TPCHTableGenerator::generate_customer() {
  TableLoader loader(table_);

  customer_t customer{};
  for (auto row_idx = part_offset_; rowcnt_; rowcnt_--, row_idx++) {
    call_dbgen_mk<customer_t>(row_idx + 1, customer, mk_cust, CUST, &ctx_);

    loader.start()
        .addItem(customer.custkey)
        .addItem(customer.name)
        .addItem(customer.address)
        .addItem(customer.nation_code)
        .addItem(customer.phone)
        .addItem(convert_money_str(customer.acctbal).data())
        .addItem(customer.mktsegment)
        .addItem(customer.comment)
        .end();
  }

  return {loader.row_count(), 0};
}

std::pair<int, int> TPCHTableGenerator::generate_orders_and_lineitem() {
  TableLoader order_loader("orders");
  TableLoader lineitem_loader("lineitem");

  order_t order{};
  // auto start = std::chrono::high_resolution_clock::now();
  // std::chrono::duration<double> o_elapsed_time = std::chrono::duration<double>::zero();
  // std::chrono::duration<double> o_elapsed_time1 = std::chrono::duration<double>::zero();
  // std::chrono::duration<double> o_elapsed_time2 = std::chrono::duration<double>::zero();
  for (auto order_idx = part_offset_; rowcnt_; rowcnt_--, ++order_idx) {
    // auto o_start = std::chrono::high_resolution_clock::now();
    call_dbgen_mk<order_t>(order_idx + 1, order, mk_order, ORDER_LINE, &ctx_, 0l);
    // auto o_end = std::chrono::high_resolution_clock::now();
    // o_elapsed_time += (o_end - o_start);

    // auto o_start1 = std::chrono::high_resolution_clock::now();
    order_loader.start()
        .addItem(order.okey)
        .addItem(order.custkey)
        .addItem(convert_str(order.orderstatus).data())
        .addItem(convert_money_str(order.totalprice).data())
        .addItem(order.odate)
        .addItem(order.opriority)
        .addItem(order.clerk)
        .addItem(order.spriority)
        .addItem(order.comment)
        .end();
    // auto o_end1 = std::chrono::high_resolution_clock::now();
    // o_elapsed_time1 += (o_end1 - o_start1);

    // auto o_start2 = std::chrono::high_resolution_clock::now();
    for (auto line_idx = int64_t{0}; line_idx < order.lines; ++line_idx) {
      const auto& lineitem = order.l[line_idx];

      lineitem_loader.start()
          .addItem(lineitem.okey)
          .addItem(lineitem.partkey)
          .addItem(lineitem.suppkey)
          .addItem(lineitem.lcnt)
          .addItem(convert_money_str(lineitem.quantity).data())
          .addItem(convert_money_str(lineitem.eprice).data())
          .addItem(convert_money_str(lineitem.discount).data())
          .addItem(convert_money_str(lineitem.tax).data())
          .addItem(convert_str(lineitem.rflag[0]).data())
          .addItem(convert_str(lineitem.lstatus[0]).data())
          .addItem(lineitem.sdate)
          .addItem(lineitem.cdate)
          .addItem(lineitem.rdate)
          .addItem(lineitem.shipinstruct)
          .addItem(lineitem.shipmode)
          .addItem(lineitem.comment)
          .end();
    }
    // auto o_end2 = std::chrono::high_resolution_clock::now();
    // o_elapsed_time2 += (o_end2 - o_start2);
  }
  /*
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end - start;
    整个循环执行时间: 50.3691 秒 order:2.89651 秒 xx1:5.08051 秒 xx2:42.0685 秒
    lineitem de 耗时最久，需要加入 批量 insert 操作，当前先实现 并行任务的分割
    std::cout << "整个循环执行时间: " << elapsed_time.count() << " 秒" << " order:" << o_elapsed_time.count() << " 秒"
              << " xx1:" << o_elapsed_time1.count() << " 秒" << " xx2:" << o_elapsed_time2.count() << " 秒" <<
    std::endl;
  */
  return {order_loader.row_count(), lineitem_loader.row_count()};
}

std::pair<int, int> TPCHTableGenerator::generate_nation() {
  TableLoader loader(table_);

  code_t nation{};
  for (auto nation_idx = part_offset_; rowcnt_; rowcnt_--, ++nation_idx) {
    call_dbgen_mk<code_t>(nation_idx + 1, nation, mk_nation, NATION, &ctx_);
    loader.start().addItem(nation.code).addItem(nation.text).addItem(nation.join).addItem(nation.comment).end();
  }

  return {loader.row_count(), 0};
}

std::pair<int, int> TPCHTableGenerator::generate_part_and_partsupp() {
  TableLoader part_loader("part");
  TableLoader partsupp_loader("partsupp");

  part_t part{};
  for (auto part_idx = part_offset_; rowcnt_; rowcnt_--, ++part_idx) {
    call_dbgen_mk<part_t>(part_idx + 1, part, mk_part, PART_PSUPP, &ctx_);

    part_loader.start()
        .addItem(part.partkey)
        .addItem(part.name)
        .addItem(part.mfgr)
        .addItem(part.brand)
        .addItem(part.type)
        .addItem(part.size)
        .addItem(part.container)
        .addItem(convert_money_str(part.retailprice).data())
        .addItem(part.comment)
        .end();

    for (const auto& partsupp : part.s) {
      partsupp_loader.start()
          .addItem(partsupp.partkey)
          .addItem(partsupp.suppkey)
          .addItem(partsupp.qty)
          .addItem(convert_money_str(partsupp.scost).data())
          .addItem(partsupp.comment)
          .end();
    }
  }

  return {part_loader.row_count(), partsupp_loader.row_count()};
}

std::pair<int, int> TPCHTableGenerator::generate_region() {
  TableLoader loader(table_);

  code_t region{};
  for (auto region_idx = part_offset_; rowcnt_; rowcnt_--, ++region_idx) {
    call_dbgen_mk<code_t>(region_idx + 1, region, mk_region, REGION, &ctx_);
    loader.start().addItem(region.code).addItem(region.text).addItem(region.comment).end();
  }

  return {loader.row_count(), 0};
}

std::pair<int, int> TPCHTableGenerator::generate_supplier() {
  TableLoader loader(table_);

  supplier_t supplier{};
  for (auto supplier_idx = part_offset_; rowcnt_; rowcnt_--, ++supplier_idx) {
    call_dbgen_mk<supplier_t>(supplier_idx + 1, supplier, mk_supp, SUPP, &ctx_);

    loader.start()
        .addItem(supplier.suppkey)
        .addItem(supplier.name)
        .addItem(supplier.address)
        .addItem(supplier.nation_code)
        .addItem(supplier.phone)
        .addItem(convert_money_str(supplier.acctbal).data())
        .addItem(supplier.comment)
        .end();
  }

  return {loader.row_count(), 0};
}

}  // namespace tpch
