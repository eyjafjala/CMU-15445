//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  int f_id = -1;
  size_t dist = INT32_MAX;
  bool less_k = false;
  std::lock_guard<std::mutex> guard(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  for (auto &[u, v] : node_store_) {
    if (!v.GetEvictable()) {
      continue;
    }
    if (v.GetK() < k_) {
      if (!less_k) {
        less_k = true;
        f_id = u;
        dist = v.GetKDistance(k_);
      } else if (v.GetKDistance(k_) < dist) {
        dist = v.GetKDistance(k_);
        f_id = u;
      }
    } else {
      if (!less_k && v.GetKDistance(k_) < dist) {
        dist = v.GetKDistance(k_);
        f_id = u;
      }
    }
  }

  *frame_id = f_id;
  node_store_.erase(f_id);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> guard(latch_);
  current_timestamp_++;
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw ExecutionException("Frame ID is invalid.");
  }
  if (node_store_.count(frame_id) == 0U) {
    node_store_.emplace(frame_id, LRUKNode(frame_id));
  }
  auto &temp = node_store_.at(frame_id);
  temp.AddHistory(current_timestamp_);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  auto node_store_iter = node_store_.find(frame_id);
  if (node_store_iter != node_store_.end()) {
    // Frame exists in the node store, update its evictable status
    if (node_store_iter->second.GetEvictable() == set_evictable) {
      // No change in evictable status, do nothing
      return;
    }
    node_store_iter->second.SetEvictable(set_evictable);
    curr_size_ += set_evictable ? 1 : -1;
  } else {
    throw ExecutionException("Frame ID not found in node store. Cannot set evictable status.");
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (node_store_.count(frame_id) == 0) {
    return;
  }
  if (!node_store_.at(frame_id).GetEvictable()) {
    throw ExecutionException("The Frame ID is not evictable.");
  }
  node_store_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);
  return curr_size_;
}

}  // namespace bustub
