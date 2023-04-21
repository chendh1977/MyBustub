/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int index_in_page, BufferPoolManager* bufferpool) {
    page_id_ = page_id;
    index_in_page_ = index_in_page;
    bufferpool_ = bufferpool;
    if(page_id_!=INVALID_PAGE_ID) {
        page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>((bufferpool_->FetchPage(page_id_))->GetData());
    } else {
        page_ = nullptr;
    }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
    bufferpool_->UnpinPage(page_id_, true);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
    return index_in_page_==page_->GetSize()-1 && page_->GetNextPageId()==INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
    return (page_->Getarray_())[index_in_page_];
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
    if(page_id_ == INVALID_PAGE_ID) {
        return *this;
    }
    if(index_in_page_ < page_->GetSize()-1) {
        index_in_page_++;
    } else {
        index_in_page_ = 0;
        page_id_t pre_page_id = page_id_;
        page_id_ = page_->GetNextPageId();
        if(page_id_ == INVALID_PAGE_ID) {
            page_ = nullptr;
            index_in_page_ = -1;
        } else {
            page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>((bufferpool_->FetchPage(page_id_))->GetData());
        }
        bufferpool_->UnpinPage(pre_page_id, false);
    }
    return *this;

}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
