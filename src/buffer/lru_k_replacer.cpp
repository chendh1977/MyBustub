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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), max_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  for (auto it = lru_less_k_.rbegin(); it != lru_less_k_.rend(); it++) {
    if (evictable_[(*it)]) {
      *frame_id = *it;
      access_times_[*frame_id] = 0;
      history_list_[*frame_id].clear();
      lru_less_k_.remove(*it);
      less_k_map_.erase(*frame_id);
      curr_size_--;
      return true;
    }
  }
  for (auto it = lru_k_.begin(); it != lru_k_.end(); it++) {
    if (evictable_[(*it).first]) {
      *frame_id = (*it).first;
      access_times_[*frame_id] = 0;
      history_list_[*frame_id].clear();
      lru_k_.erase(it);
      k_map_.erase(*frame_id);
      curr_size_--;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  access_times_[frame_id]++;
  current_timestamp_++;
  history_list_[frame_id].push_back(current_timestamp_);
  if (access_times_[frame_id] == 1) {
    if (curr_size_ == max_size_) {
      frame_id_t frame;
      if (!Evict(&frame)) {
        throw std::exception();
      }
    }
    evictable_[frame_id] = true;
    lru_less_k_.push_front(frame_id);
    less_k_map_[frame_id] = lru_less_k_.begin();
    curr_size_++;

  } else if (access_times_[frame_id] == k_) {
    lru_less_k_.erase(less_k_map_[frame_id]);
    less_k_map_.erase(frame_id);
    kth_timestamp kth({frame_id, (history_list_[frame_id]).front()});
    auto it = std::upper_bound(lru_k_.begin(), lru_k_.end(), kth,
                               [](const kth_timestamp &left, const kth_timestamp &right) -> bool {
                                 return static_cast<int>(left.second) < static_cast<int>(right.second);
                               });
    it = lru_k_.insert(it, kth);
    k_map_[frame_id] = it;

  } else if (access_times_[frame_id] > k_) {
    history_list_[frame_id].push_back(current_timestamp_);
    history_list_[frame_id].pop_front();
    lru_k_.erase(k_map_[frame_id]);
    kth_timestamp kth({frame_id, (history_list_[frame_id]).front()});
    auto it = std::upper_bound(lru_k_.begin(), lru_k_.end(), kth,
                               [](const kth_timestamp &left, const kth_timestamp &right) -> bool {
                                 return static_cast<int>(left.second) < static_cast<int>(right.second);
                               });
    it = lru_k_.insert(it, kth);
    k_map_[frame_id] = it;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  if (access_times_[frame_id] == 0) {
    return;
  }
  auto oldstatus = evictable_[frame_id];
  evictable_[frame_id] = set_evictable;
  if (oldstatus && !set_evictable) {
    curr_size_--;
    max_size_--;
  }
  if (!oldstatus && set_evictable) {
    curr_size_++;
    max_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  if (access_times_[frame_id] == 0) {
    return;
  }
  if (!evictable_[frame_id]) {
    throw std::exception();
  }
  if (access_times_[frame_id] < k_) {
    access_times_[frame_id] = 0;
    history_list_[frame_id].clear();
    lru_less_k_.erase(less_k_map_[frame_id]);
    less_k_map_.erase(frame_id);
    evictable_.erase(frame_id);
  } else {
    access_times_[frame_id] = 0;
    history_list_[frame_id].clear();
    lru_k_.erase(k_map_[frame_id]);
    k_map_.erase(frame_id);
    evictable_.erase(frame_id);
  }
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
