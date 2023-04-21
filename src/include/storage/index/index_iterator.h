//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(page_id_t page_id = INVALID_PAGE_ID, int index_in_page = -1, BufferPoolManager* bufferpool = nullptr);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { 
    return page_id_==itr.page_id_ && index_in_page_==itr.index_in_page_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return !(*this==itr);
  }

 private:
  // add your own private member variables here
  page_id_t page_id_;
  B_PLUS_TREE_LEAF_PAGE_TYPE* page_;
  int index_in_page_;
  BufferPoolManager* bufferpool_;
};

}  // namespace bustub
