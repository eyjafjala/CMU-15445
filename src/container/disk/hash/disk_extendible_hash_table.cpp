//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  index_name_ = name;
  auto guard = bpm_->NewPageGuarded(&header_page_id_);  //此处应该不需要加锁，因为不会被其他线程访问到
  auto temp = guard.AsMut<ExtendibleHTableHeaderPage>();
  temp->Init(header_max_depth);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  uint32_t hash_value = Hash(key);
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_ptr = header_guard.As<ExtendibleHTableHeaderPage>();
  page_id_t dir_id = header_ptr->GetDirectoryPageId(header_ptr->HashToDirectoryIndex(hash_value));
  if (dir_id == INVALID_PAGE_ID) {
    return false;
  }
  header_guard.Drop();
  auto dir_guard = bpm_->FetchPageRead(dir_id);
  auto dir_ptr = dir_guard.As<ExtendibleHTableDirectoryPage>();
  page_id_t bucket_id = dir_ptr->GetBucketPageId(dir_ptr->HashToBucketIndex(hash_value));
  if (bucket_id == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket_guard = bpm_->FetchPageRead(bucket_id);
  auto bucket_ptr = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  V ans;
  bool is_find = bucket_ptr->Lookup(key, ans, cmp_);
  if (is_find) {
    result->emplace_back(std::move(ans));
  }
  return is_find;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>  //插入数据的逻辑
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  uint32_t hash_value = Hash(key);
  auto head_guard = bpm_->FetchPageWrite(header_page_id_);
  auto head_ptr = head_guard.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t dir_idx = head_ptr->HashToDirectoryIndex(hash_value);
  auto dir_page = static_cast<page_id_t>(head_ptr->GetDirectoryPageId(dir_idx));
  if (dir_page == INVALID_PAGE_ID) {
    return InsertToNewDirectory(head_ptr, dir_idx, hash_value, key, value);
  }

  //已经有对应的Dir了
  auto dir_guard = bpm_->FetchPageWrite(dir_page);
  head_guard.Drop();
  auto dir_ptr = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_idx = dir_ptr->HashToBucketIndex(hash_value);
  page_id_t bucket_page = dir_ptr->GetBucketPageId(bucket_idx);
  if (bucket_page == INVALID_PAGE_ID) {
    return InsertToNewBucket(dir_ptr, bucket_idx, key, value);
  }

  //也有对应的bucket了
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page);
  auto bucket_ptr = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (bucket_ptr->Insert(key, value, cmp_)) {
    return true;
  }

  V tmp;
  if (bucket_ptr->Lookup(key, tmp, cmp_)) {
    return false;  //已经存在了这个键
  }

  //此时只存在一种可能，即桶满了，但需要插入
  assert(bucket_ptr->IsFull());
  if (dir_ptr->GetLocalDepth(bucket_idx) == dir_ptr->GetGlobalDepth()) {
    dir_ptr->IncrGlobalDepth();
    // inc 失败了，说明local长度已满，不能再分裂了
    if (dir_ptr->GetLocalDepth(bucket_idx) == dir_ptr->GetGlobalDepth()) {
      return false;
    }
  }
  dir_ptr->IncrLocalDepth(bucket_idx);
  page_id_t split_id;
  auto split_guard = bpm_->NewPageGuarded(&split_id);
  auto split_ptr = split_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  split_ptr->Init(bucket_max_size_);
  uint32_t mask = dir_ptr->GetLocalDepthMask(bucket_idx);
  uint32_t split_idx = dir_ptr->GetSplitImageIndex(bucket_idx);
  dir_ptr->IncrLocalDepth(split_idx);
  assert(dir_ptr->GetLocalDepth(bucket_idx) == dir_ptr->GetLocalDepth(split_idx));  //验证初始化正确性
  UpdateDirectoryMapping(dir_ptr, split_idx, split_id, dir_ptr->GetLocalDepth(split_idx), mask);

  //重新映射完了再进行插入
  uint32_t size = bucket_ptr->Size();
  std::pair<K, V> temp_arr[size];
  for (uint32_t i = 0; i < size; i++) {
    temp_arr[i] = bucket_ptr->EntryAt(i);
  }

  for (uint32_t i = 0; i < size; i++) {
    K k = temp_arr[i].first;
    V v = temp_arr[i].second;
    uint32_t hash_v = Hash(k);
    uint32_t insert_idx = dir_ptr->HashToBucketIndex(hash_v);
    page_id_t insert_page = dir_ptr->GetBucketPageId(insert_idx);
    //该page一定属于俩page之一
    if (insert_page == split_id) {
      split_ptr->Insert(k, v, cmp_);
      bucket_ptr->Remove(k, cmp_);
    } else {
      assert(insert_page == bucket_page);
    }
  }

  uint32_t insert_idx = dir_ptr->HashToBucketIndex(hash_value);
  page_id_t insert_page = dir_ptr->GetBucketPageId(insert_idx);
  bool is_succ = false;
  if (insert_page == split_id) {
    is_succ = split_ptr->Insert(key, value, cmp_);
  } else if (insert_page == bucket_page) {
    is_succ = bucket_ptr->Insert(key, value, cmp_);
  } else {
    assert(false);
  }

  //若不成功，则分桶后还是满，再次调用进行分桶，直到global增长满
  if (!is_succ) {
    return Insert(key, value, transaction);
  }
  return is_succ;
}

template <typename K, typename V,
          typename KC>  // 对于header这个头，创造一个新dir并往里面塞东西。那么新dir必然要有新bucket
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t new_dir_id;
  auto dir_guard = bpm_->NewPageGuarded(&new_dir_id);
  auto dir_write = dir_guard.UpgradeWrite();
  auto dir_ptr = dir_write.AsMut<ExtendibleHTableDirectoryPage>();
  dir_ptr->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, new_dir_id);
  uint32_t bucket_id = dir_ptr->HashToBucketIndex(hash);
  return InsertToNewBucket(dir_ptr, bucket_id, key, value);
}

template <typename K, typename V, typename KC>  //对于dir这个目录，分裂一个新桶并且往里面塞东西
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t new_id;
  auto bucket_guard = bpm_->NewPageGuarded(&new_id);
  auto bucket_write = bucket_guard.UpgradeWrite();
  auto bucket_ptr = bucket_write.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_ptr->Init(bucket_max_size_);
  directory->SetBucketPageId(bucket_idx, new_id);
  return bucket_ptr->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  uint32_t hash_value = Hash(key);
  auto head_guard = bpm_->FetchPageRead(header_page_id_);
  auto head_ptr = head_guard.As<ExtendibleHTableHeaderPage>();
  uint32_t dir_idx = head_ptr->HashToDirectoryIndex(hash_value);
  auto dir_page = static_cast<page_id_t>(head_ptr->GetDirectoryPageId(dir_idx));
  if (dir_page == INVALID_PAGE_ID) {
    return false;
  }

  //已经有对应的Dir了
  head_guard.Drop();
  auto dir_guard = bpm_->FetchPageWrite(dir_page);
  auto dir_ptr = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_idx = dir_ptr->HashToBucketIndex(hash_value);
  page_id_t bucket_page = dir_ptr->GetBucketPageId(bucket_idx);
  if (bucket_page == INVALID_PAGE_ID) {
    return false;
  }

  //也有对应的bucket了
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page);
  auto bucket_ptr = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bool is_succ = bucket_ptr->Remove(key, cmp_);
  if (!is_succ) {
    return is_succ;
  }
  std::vector<uint32_t> indexes;
  indexes.push_back(bucket_idx);
  //外层不判断bucket_ptr指向的页面为空的原因是，有可能镜像页面为空，此时也要合成
  while (dir_ptr->GetLocalDepth(bucket_idx) > 0) {
    uint32_t split_idx = dir_ptr->GetSplitImageIndex(bucket_idx);
    if (dir_ptr->GetLocalDepth(bucket_idx) != dir_ptr->GetLocalDepth(split_idx)) {
      break;
    }
    page_id_t split_page = dir_ptr->GetBucketPageId(split_idx);
    auto split_guard = bpm_->FetchPageWrite(split_page);
    auto split_ptr = split_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

    if (!bucket_ptr->IsEmpty() && !split_ptr->IsEmpty()) {
      break;
    }
    indexes.push_back(split_idx);
    // bucket空了，则指向bucket的都得指向split
    auto set_page = bucket_ptr->IsEmpty() ? split_page : bucket_page;
    for (auto idx : indexes) {
      dir_ptr->DecrLocalDepth(idx);
      dir_ptr->SetBucketPageId(idx, set_page);
    }
    //后续旧的bucket_idx和bucket_page都废了
    if (bucket_ptr->IsEmpty()) {
      bpm_->DeletePage(bucket_page);
      bucket_idx = split_idx;
      bucket_page = split_page;
      bucket_guard = std::move(split_guard);
      bucket_ptr = split_ptr;
    } else {
      bpm_->DeletePage(split_page);
    }
  }

  while (dir_ptr->CanShrink()) {
    dir_ptr->DecrGlobalDepth();
  }
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
