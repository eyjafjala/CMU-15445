//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() { 
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  iter_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
 }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    while (true) {
      if (iter_->IsEnd()) {
        return false;
      }
      auto tp = iter_->GetTuple();
      *tuple = tp.second;
      *rid = iter_->GetRID();
      ++(*iter_);
      if (tp.first.is_deleted_) {
        continue;
      }
      if (plan_->filter_predicate_ != nullptr && !plan_->filter_predicate_->Evaluate(tuple, table_info_->schema_).GetAs<bool>()) {
        continue;
      }
      return true;
    }
    return false;
 }

}  // namespace bustub
