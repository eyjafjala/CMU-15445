//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// trie.cpp
//
// Identification: src/primer/trie.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/trie.h"
#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

/**
 * @brief Get the value associated with the given key.
 * 1. If the key is not in the trie, return nullptr.
 * 2. If the key is in the trie but the type is mismatched, return nullptr.
 * 3. Otherwise, return the value.
 */
template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");
  auto root_copy = root_;
  if (root_copy == nullptr) {
    return nullptr;
  }
  if (key.empty()) {
    if (!root_copy->is_value_node_) {
      return nullptr;
    }
    auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(root_copy.get());
    return value_node->value_.get();
  }
  for (char c : key) {
    if (root_copy->children_.find(c) == root_copy->children_.end()) {
      return nullptr;
    }
    root_copy = root_copy->children_.find(c)->second;
  }
  if (!root_copy->is_value_node_) {
    return nullptr;
  }
  auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(root_copy.get());
  if (value_node == nullptr) {
    return nullptr;
  }

  return value_node->value_.get();

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

/**
 * @brief Put a new key-value pair into the trie. If the key already exists, overwrite the value.
 * @return the new trie.
 */
template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");
  std::shared_ptr<TrieNode> new_root;
  if (root_ == nullptr) {
    // If the root is null, create a new root node.
    new_root = std::make_shared<TrieNode>();
  } else {
    // If the root is not null, create a new root node with the existing root as its child.
    new_root = std::shared_ptr<TrieNode>(root_->Clone());
  }

  auto root_copy = new_root;
  std::shared_ptr<TrieNode> new_node;
  //此处是无值的节点
  for (int i = 0; i < static_cast<int>(key.size() - 1); i++) {
    char c = key[i];
    //旧的树有没有信息需要复制
    if (root_copy->children_.find(c) == root_copy->children_.end()) {
      new_node = std::make_shared<TrieNode>();
    } else {
      new_node = std::shared_ptr<TrieNode>(root_copy->children_[c]->Clone());
    }
    root_copy->children_[c] = new_node;
    root_copy = new_node;
  }

  //此处是有值的节点
  if (!key.empty()) {
    char c = key[key.size() - 1];
    if (root_copy->children_.find(c) == root_copy->children_.end()) {
      // If the node doesn't exist, create a new node with the value.
      root_copy->children_[c] = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    } else {
      // If the node exists, create a new node with the existing value.
      auto new_node = std::make_shared<TrieNodeWithValue<T>>(root_copy->children_[c]->children_,
                                                             std::make_shared<T>(std::move(value)));
      root_copy->children_[c] = new_node;
    }
  } else {
    auto root_with_val =
        std::make_shared<TrieNodeWithValue<T>>(new_root->children_, std::make_shared<T>(std::move(value)));
    // auto new_trie = std::make_shared<Trie>(std::shared_ptr<TrieNode>(root_with_val));
    auto new_trie = new Trie(root_with_val);
    auto shard = std::shared_ptr<Trie>(new_trie);
    return *shard;
  }

  // auto new_trie = std::make_shared<Trie>(new_root);
  auto new_trie = new Trie(new_root);
  auto shard = std::shared_ptr<Trie>(new_trie);
  return *shard;
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

/**
 * @brief Remove the key from the trie.
 * @return If the key does not exist, return the original trie. Otherwise, returns the new trie.
 */
auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  if (root_ == nullptr) {
    return *this;
  }
  auto new_root = std::shared_ptr<TrieNode>(root_->Clone());
  auto root_copy = new_root;
  if (key.empty()) {
    if (!root_copy->is_value_node_) {
      return *this;
    }
    auto new_node = std::make_shared<TrieNode>(root_copy->children_);
    auto new_trie = new Trie(new_node);
    auto shard = std::shared_ptr<Trie>(new_trie);
    return *shard;
  }
  std::stack<std::pair<std::shared_ptr<TrieNode>, char>> node_stack;
  node_stack.push({new_root, '\0'});
  for (int i = 0; i < static_cast<int>(key.size() - 1); i++) {
    char c = key[i];
    if (root_copy->children_.find(c) == root_copy->children_.end()) {
      return *this;
    }
    auto new_node = std::shared_ptr<TrieNode>(root_copy->children_[c]->Clone());
    root_copy->children_[c] = new_node;
    root_copy = new_node;
    node_stack.push({new_node, c});
  }
  char c = key[key.size() - 1];
  if (root_copy->children_.find(c) == root_copy->children_.end()) {
    return *this;
  }
  auto new_node = std::make_shared<TrieNode>(root_copy->children_[c]->children_);
  root_copy->children_[c] = new_node;
  node_stack.push({new_node, c});
  while (!node_stack.empty() && node_stack.top().first->children_.empty() && !node_stack.top().first->is_value_node_) {
    char c = node_stack.top().second;
    node_stack.pop();
    if (node_stack.empty()) {
      break;
    }
    auto now = node_stack.top().first;
    now->children_.erase(c);
  }
  if (new_root->children_.empty() && !new_root->is_value_node_) {
    return {};
  }

  auto new_trie = new Trie(new_root);
  auto shard = std::shared_ptr<Trie>(new_trie);
  return *shard;
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
