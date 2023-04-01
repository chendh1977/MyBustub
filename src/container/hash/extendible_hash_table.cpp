//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  dir_.push_back(std::shared_ptr<Bucket>(new Bucket(bucket_size_)));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return (dir_[dir_index])->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto dir_index = IndexOf(key);
  return (dir_[dir_index])->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto dir_index = IndexOf(key);
  return (dir_[dir_index])->Remove(key);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket, size_t dir_index) -> void {
  std::shared_ptr<Bucket> bucket0(new Bucket(bucket_size_, bucket->GetDepth() + 1));
  std::shared_ptr<Bucket> bucket1(new Bucket(bucket_size_, bucket->GetDepth() + 1));
  auto list = bucket->GetItems();
  int mask = 1 << bucket->GetDepth();
  num_buckets_++;
  for (const std::pair<K, V> &item : list) {
    auto key = item.first;
    auto value = item.second;
    if (std::hash<K>()(key) & mask) {
      bucket1->Insert(key, value);
    } else {
      bucket0->Insert(key, value);
    }
  }
  for (size_t i = dir_index & (mask - 1); i < dir_.size(); i += mask) {
    if ((i & mask) != 0) {
      dir_[i] = bucket1;
    } else {
      dir_[i] = bucket0;
    }
  }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_index;
  while (!((dir_[IndexOf(key)])->Insert(key, value))) {
    dir_index = IndexOf(key);
    if ((dir_[dir_index])->GetDepth() == GetGlobalDepthInternal()) {
      int len = dir_.size();
      dir_.reserve(len * 2);
      std::copy_n(dir_.begin(), len, std::back_inserter(dir_));
      global_depth_++;
    }
    RedistributeBucket(dir_[dir_index], dir_index);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const std::pair<K, V> &item : list_) {
    if (item.first == key) {
      value = item.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto it = list_.begin();
  for (; it != list_.end(); it++) {
    if ((*it).first == key) {
      break;
    }
  }
  if (it == list_.end()) {
    return false;
  }
  list_.erase(it);
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {
    return false;
  }
  auto it = list_.begin();
  for (; it != list_.end(); it++) {
    if ((*it).first == key) {
      break;
    }
  }
  if (it == list_.end()) {
    list_.push_front(std::pair<K, V>(key, value));
  } else {
    (*it).second = value;
  }
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
