//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t fit;
  if (free_list_.empty()) {
    if (!replacer_->Evict(&fit)) {
      page_id = nullptr;
      return nullptr;
    }
    //如果脏页面被置换出去了，需要写到磁盘
    Page &page = pages_[fit];
    page_id_t temp = page.GetPageId();
    page_table_.erase(temp);
    if (page.IsDirty()) {
      auto prom = disk_scheduler_->CreatePromise();
      auto future = prom.get_future();
      disk_scheduler_->Schedule({true, page.data_, temp, std::move(prom)});
      page.is_dirty_ = false;
      future.get();
    }
    page.ResetMemory();
  } else {
    fit = *free_list_.begin();
    free_list_.pop_front();
  }
  page_id_t pit = AllocatePage();
  *page_id = pit;
  page_table_[pit] = fit;
  replacer_->RecordAccess(fit);
  replacer_->SetEvictable(fit, false);
  Page &page = pages_[fit];
  page.page_id_ = pit;
  page.pin_count_ = 0;
  page.is_dirty_ = false;
  page.pin_count_++;
  return &page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t fit;
  if (page_table_.count(page_id) != 0U) {
    fit = page_table_.at(page_id);
    replacer_->RecordAccess(fit, access_type);
    replacer_->SetEvictable(fit, false);
    pages_[fit].pin_count_++;
    return &pages_[fit];
  }
  if (free_list_.empty()) {
    if (!replacer_->Evict(&fit)) {
      return nullptr;
    }
    Page &page = pages_[fit];
    page_id_t temp = page.GetPageId();
    page_table_.erase(temp);
    //如果脏页面被置换出去了，需要写到磁盘
    if (page.IsDirty()) {
      auto prom = disk_scheduler_->CreatePromise();
      auto future = prom.get_future();
      disk_scheduler_->Schedule({true, page.data_, temp, std::move(prom)});
      page.is_dirty_ = false;
      future.get();
    }
    page.ResetMemory();
  } else {
    fit = *free_list_.begin();
    free_list_.pop_front();
  }
  page_table_[page_id] = fit;
  replacer_->RecordAccess(fit);
  replacer_->SetEvictable(fit, false);
  Page &page = pages_[fit];
  page.page_id_ = page_id;
  page.pin_count_++;
  auto prom = disk_scheduler_->CreatePromise();
  auto future = prom.get_future();
  disk_scheduler_->Schedule({false, page.data_, page_id, std::move(prom)});
  future.get();
  return &page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0U) {
    return false;
  }
  frame_id_t fit = page_table_.at(page_id);
  Page &page = pages_[fit];
  if (page.GetPinCount() == 0) {
    return false;
  }
  page.pin_count_--;
  if (is_dirty) {
    page.is_dirty_ = is_dirty;
  }
  if (page.GetPinCount() == 0) {
    replacer_->SetEvictable(fit, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0U) {
    return false;
  }
  frame_id_t fit = page_table_.at(page_id);
  Page &page = pages_[fit];
  auto prom = disk_scheduler_->CreatePromise();
  auto future = prom.get_future();
  disk_scheduler_->Schedule({true, page.data_, page_id, std::move(prom)});
  page.is_dirty_ = false;
  future.get();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);
  for (auto &[page_id, frame_id] : page_table_) {
    Page &page = pages_[frame_id];
    auto prom = disk_scheduler_->CreatePromise();
    auto future = prom.get_future();
    disk_scheduler_->Schedule({true, page.data_, page_id, std::move(prom)});
    page.is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0U) {
    return true;
  }
  frame_id_t fit = page_table_.at(page_id);
  Page &page = pages_[fit];
  if (page.GetPinCount() > 0) {
    return false;
  }
  page.ResetMemory();
  page.is_dirty_ = false;
  page.page_id_ = INVALID_PAGE_ID;
  page_table_.erase(page_id);
  free_list_.push_back(fit);
  replacer_->Remove(fit);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  // std::lock_guard<std::mutex> lock(latch_);
  if (!free_ids_.empty()) {
    auto id = *free_ids_.begin();
    free_ids_.pop_front();
    return id;
  }
  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
