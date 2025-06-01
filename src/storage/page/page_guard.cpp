#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr) {
    if (bpm_->UnpinPage(PageId(), is_dirty_)) {
      bpm_ = nullptr;
      page_ = nullptr;
      is_dirty_ = false;
    } else {
      throw Exception("the page is already unpin");
    }
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & { 
  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
  return *this; 
}

BasicPageGuard::~BasicPageGuard(){
    Drop();
};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  if (bpm_ != nullptr) {
    page_->RLatch();
  }
  ReadPageGuard temp = {bpm_, page_};
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
  return temp;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  if (bpm_ != nullptr) {
    page_->WLatch();
  }
  WritePageGuard temp = {bpm_, page_};
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
  return temp;
}


ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  return *this; 
}

void ReadPageGuard::Drop() {
  if (guard_.bpm_ != nullptr) {
    guard_.page_->RUnlatch();
    guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() {
  Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.bpm_ != nullptr) {
    guard_.page_->WUnlatch();    
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() {
  Drop();
}  // NOLINT

}  // namespace bustub
