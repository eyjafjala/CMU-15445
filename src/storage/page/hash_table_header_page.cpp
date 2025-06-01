//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_header_page.cpp
//
// Identification: src/storage/page/hash_table_header_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_header_page.h"

namespace bustub {
auto HashTableHeaderPage::GetBlockPageId(size_t index) -> page_id_t { return block_page_ids_[index]; }

auto HashTableHeaderPage::GetPageId() const -> page_id_t { return page_id_; }

void HashTableHeaderPage::SetPageId(bustub::page_id_t page_id) { page_id_ = page_id; }

auto HashTableHeaderPage::GetLSN() const -> lsn_t { return lsn_; }

void HashTableHeaderPage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

void HashTableHeaderPage::AddBlockPageId(page_id_t page_id) { block_page_ids_[next_ind_++] = page_id; }

auto HashTableHeaderPage::NumBlocks() -> size_t { return next_ind_; }

void HashTableHeaderPage::SetSize(size_t size) { size_ = size; }

auto HashTableHeaderPage::GetSize() const -> size_t { return size_; }

}  // namespace bustub
