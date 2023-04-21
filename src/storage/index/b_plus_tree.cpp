#include <string>
#include <algorithm>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return cur_size_==0; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  auto tmp_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(GetRootPageId())->GetData());
  InternalPage* internal_page;
  LeafPage* leaf_page;
  page_id_t page_id;
  while(!tmp_page->IsLeafPage()) {
    page_id = tmp_page->GetPageId();
    internal_page = reinterpret_cast<InternalPage *>(tmp_page);
    page_id_t value = internal_page->Find(key, comparator_);
    tmp_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(value)->GetData());
    buffer_pool_manager_->UnpinPage(page_id, false);
  }
  ValueType value;
  leaf_page = reinterpret_cast<LeafPage *>(tmp_page);
  if(!leaf_page->Find(key, comparator_, value)){
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  result->push_back(value);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return true;
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::TreeTraversal(const KeyType& key, BPlusTreePage** bPage) ->bool {
  auto tmp_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(GetRootPageId())->GetData());
  InternalPage* internal_page;
  LeafPage* leaf_page;
  page_id_t page_id;
  while(!tmp_page->IsLeafPage()) {
    page_id = tmp_page->GetPageId();
    internal_page = reinterpret_cast<InternalPage *>(tmp_page);
    page_id_t value = internal_page->Find(key, comparator_);
    tmp_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(value)->GetData());
    buffer_pool_manager_->UnpinPage(page_id, false);
  }  
  ValueType value;
  leaf_page = reinterpret_cast<LeafPage *>(tmp_page);
  if(leaf_page->Find(key, comparator_, value)){
    *bPage = leaf_page;
    return false;
  }  
  *bPage = leaf_page;
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SimpleInsert(const MappingType* pair, BPlusTreePage* bPage) {
  if(bPage->IsLeafPage()) {
    LeafPage* leaf_page = reinterpret_cast<LeafPage *>(bPage);
    if(leaf_page->GetSize() == 0) {
      *(leaf_page->Getarray_()) = *(pair);
      leaf_page->IncreaseSize(1);
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
      return;
    }
    leaf_page->GetNextPageId();
    auto result = std::upper_bound(leaf_page->Getarray_(), leaf_page->Getarray_()+leaf_page->GetSize(), pair->first, [this] (const KeyType& pair, const MappingType& key) ->bool {
      return this->comparator_(pair, key.first) < 0;
    });
    for(auto it = leaf_page->Getarray_()+leaf_page->GetSize(); it>result; it--) {
      *it = *(it-1);
    }
    *(result) = *pair;
    leaf_page->IncreaseSize(1);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return;
  }
  if(bPage->IsInternalPage()) {
    InternalPage* internal_page = reinterpret_cast<InternalPage *>(bPage);
    if(internal_page->GetSize() == 1) {
      *(internal_page->Getarray_()+1) = *(reinterpret_cast<const InternalMappingType*>(pair));
      internal_page->IncreaseSize(1);
      buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), true);
      return;
    }
    auto result = std::upper_bound(internal_page->Getarray_()+1, internal_page->Getarray_()+internal_page->GetSize(), pair->first, [this] (const KeyType& pair, const InternalMappingType& key) ->bool {
      return this->comparator_(pair, key.first) < 0;
    });
    for(auto it = internal_page->Getarray_()+internal_page->GetSize(); it>result; it--) {
      *it = *(it-1);
    }
    *(result) = *(reinterpret_cast<const InternalMappingType*>(pair));
    internal_page->IncreaseSize(1);
    buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), true);
    return;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SimpleSplit(const MappingType* pair, BPlusTreePage* bPage, BPlusTreePage* parent_page) {
  page_id_t page_id;
  BPlusTreePage* newpage;
  if(bPage->IsLeafPage()) {
    LeafPage* leaf_page = reinterpret_cast<LeafPage *>(bPage);
    Page* page = buffer_pool_manager_->NewPage(&page_id);   
    newpage = reinterpret_cast<BPlusTreePage *>(page->GetData());
    auto tmpnewpage = reinterpret_cast<LeafPage *>(newpage);
    tmpnewpage->Init(page_id, INVALID_PAGE_ID, leaf_max_size_); 
    int old_size = tmpnewpage->GetMinSize();
    int new_size = tmpnewpage->GetMaxSize()-tmpnewpage->GetMinSize()+1;
    MappingType tmpmap[leaf_max_size_+1];
    std::copy(leaf_page->Getarray_(), leaf_page->Getarray_()+leaf_max_size_ ,tmpmap);
    tmpmap[leaf_max_size_] = *pair;
    std::sort(tmpmap, tmpmap+leaf_max_size_+1, [this] (const MappingType& left, const MappingType& right){
      return this->comparator_(left.first, right.first) < 0;
    });
    std::copy(tmpmap+old_size, tmpmap+leaf_max_size_+1, tmpnewpage->Getarray_());
    std::copy(tmpmap, tmpmap+old_size, leaf_page->Getarray_());
    tmpnewpage->SetSize(new_size);
    leaf_page->SetSize(old_size);
    tmpnewpage->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page->SetNextPageId(page_id);
  }
  if(bPage->IsInternalPage()) {
    InternalPage* internal_page = reinterpret_cast<InternalPage *>(bPage);
    Page* page = buffer_pool_manager_->NewPage(&page_id);   
    newpage = reinterpret_cast<BPlusTreePage *>(page->GetData());
    auto tmpnewpage = reinterpret_cast<InternalPage *>(newpage);
    tmpnewpage->Init(page_id, INVALID_PAGE_ID, internal_max_size_); 
    int old_size = tmpnewpage->GetMinSize();
    int new_size = tmpnewpage->GetMaxSize()-tmpnewpage->GetMinSize()+1;
    InternalMappingType tmpmap[internal_max_size_+1];
    std::copy(internal_page->Getarray_(), internal_page->Getarray_()+internal_max_size_ ,tmpmap);
    tmpmap[internal_max_size_] = *(reinterpret_cast<const InternalMappingType*>(pair));
    std::sort(tmpmap, tmpmap+internal_max_size_+1, [this] (const InternalMappingType& left, const InternalMappingType& right){
      return this->comparator_(left.first, right.first) < 0;
    });
    std::copy(tmpmap+old_size, tmpmap+internal_max_size_+1, tmpnewpage->Getarray_());
    for(int i = 0; i<new_size; i++) {
      page_id_t tmpid = (tmpnewpage->Getarray_())[i].second;
      auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(tmpid)->GetData());
      child_page->SetParentPageId(tmpnewpage->GetPageId());
      buffer_pool_manager_->UnpinPage(tmpid, true);
    }
    std::copy(tmpmap, tmpmap+old_size, internal_page->Getarray_());
    tmpnewpage->SetSize(new_size);
    internal_page->SetSize(old_size);
  }
  if(!parent_page) {
    page_id_t page_id1;
    Page* page = buffer_pool_manager_->NewPage(&page_id1);   
    parent_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    auto tmppage = reinterpret_cast<InternalPage *>(parent_page);
    tmppage->Init(page_id1, INVALID_PAGE_ID, internal_max_size_);
    (tmppage->Getarray_())[0].second = bPage->GetPageId();
    tmppage->IncreaseSize(1);
    root_page_id_ = page_id1;
    UpdateRootPageId(0);
  }
  bPage->SetParentPageId(parent_page->GetPageId());
  newpage->SetParentPageId(parent_page->GetPageId());
  InternalMappingType new_pair;
  if(newpage->IsInternalPage()) {
    new_pair = std::make_pair((reinterpret_cast<InternalPage *>(newpage)->Getarray_())[0].first, page_id);
  } else if(newpage->IsLeafPage()) {
    new_pair = std::make_pair((reinterpret_cast<LeafPage *>(newpage)->Getarray_())[0].first, page_id);
  }
  buffer_pool_manager_->UnpinPage(page_id, true);
  buffer_pool_manager_->UnpinPage(bPage->GetPageId(), true);
  SimpleInsert(reinterpret_cast<MappingType*>(&new_pair), parent_page);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MultipleSplit(const MappingType* pair, BPlusTreePage* bPage, BPlusTreePage* parent_page) {
  page_id_t page_id;
  BPlusTreePage* newpage;
  if(bPage->IsLeafPage()) {
    LeafPage* leaf_page = reinterpret_cast<LeafPage *>(bPage);
    Page* page = buffer_pool_manager_->NewPage(&page_id);   
    newpage = reinterpret_cast<BPlusTreePage *>(page->GetData());
    auto tmpnewpage = reinterpret_cast<LeafPage *>(newpage);
    tmpnewpage->Init(page_id, INVALID_PAGE_ID, leaf_max_size_); 
    int old_size = tmpnewpage->GetMinSize();
    int new_size = tmpnewpage->GetMaxSize()-tmpnewpage->GetMinSize()+1;
    MappingType tmpmap[leaf_max_size_+1];
    std::copy(leaf_page->Getarray_(), leaf_page->Getarray_()+leaf_max_size_ ,tmpmap);
    tmpmap[leaf_max_size_] = *pair;
    std::sort(tmpmap, tmpmap+leaf_max_size_+1, [this] (const MappingType& left, const MappingType& right){
      return this->comparator_(left.first, right.first) < 0;
    });
    std::copy(tmpmap+old_size, tmpmap+leaf_max_size_+1, tmpnewpage->Getarray_());
    std::copy(tmpmap, tmpmap+old_size, leaf_page->Getarray_());
    tmpnewpage->SetSize(new_size);
    leaf_page->SetSize(old_size);
    tmpnewpage->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page->SetNextPageId(page_id);
  }
  if(bPage->IsInternalPage()) {
    InternalPage* internal_page = reinterpret_cast<InternalPage *>(bPage);
    Page* page = buffer_pool_manager_->NewPage(&page_id);   
    newpage = reinterpret_cast<BPlusTreePage *>(page->GetData());
    auto tmpnewpage = reinterpret_cast<InternalPage *>(newpage);
    tmpnewpage->Init(page_id, INVALID_PAGE_ID, internal_max_size_); 
    int old_size = tmpnewpage->GetMinSize();
    int new_size = tmpnewpage->GetMaxSize()-tmpnewpage->GetMinSize()+1;
    InternalMappingType tmpmap[internal_max_size_+1];
    std::copy(internal_page->Getarray_(), internal_page->Getarray_()+internal_max_size_ ,tmpmap);
    tmpmap[internal_max_size_] = *(reinterpret_cast<const InternalMappingType*>(pair));
    std::sort(tmpmap, tmpmap+internal_max_size_+1, [this] (const InternalMappingType& left, const InternalMappingType& right){
      return this->comparator_(left.first, right.first) < 0;
    });
    std::copy(tmpmap+old_size, tmpmap+internal_max_size_+1, tmpnewpage->Getarray_());
    for(int i = 0; i<new_size; i++) {
      page_id_t tmpid = (tmpnewpage->Getarray_())[i].second;
      auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(tmpid)->GetData());
      child_page->SetParentPageId(tmpnewpage->GetPageId());
      buffer_pool_manager_->UnpinPage(tmpid, true);
    }
    std::copy(tmpmap, tmpmap+old_size, internal_page->Getarray_());
    tmpnewpage->SetSize(new_size);
    internal_page->SetSize(old_size);
  }
  newpage->SetParentPageId(parent_page->GetPageId());
  InternalMappingType new_pair;
  if(newpage->IsInternalPage()) {
    new_pair = std::make_pair((reinterpret_cast<InternalPage *>(newpage)->Getarray_())[0].first, page_id);
  } else if(newpage->IsLeafPage()) {
    new_pair = std::make_pair((reinterpret_cast<LeafPage *>(newpage)->Getarray_())[0].first, page_id);
  }  
  if(parent_page->IsRootPage()) {
    buffer_pool_manager_->UnpinPage(page_id, true);
    buffer_pool_manager_->UnpinPage(bPage->GetPageId(), true);
    SimpleSplit(reinterpret_cast<MappingType*>(&new_pair), parent_page, nullptr);
    return;
  }
  BPlusTreePage* new_parent_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(parent_page->GetParentPageId())->GetData());
  buffer_pool_manager_->UnpinPage(page_id, true);
  buffer_pool_manager_->UnpinPage(bPage->GetPageId(), true);
  new_parent_page = reinterpret_cast<InternalPage* >(new_parent_page);
  if(new_parent_page->GetMaxSize() > new_parent_page->GetSize()) {
    SimpleSplit(reinterpret_cast<MappingType*>(&new_pair), parent_page, new_parent_page);
  }
  else {
    MultipleSplit(reinterpret_cast<MappingType*>(&new_pair), parent_page, new_parent_page);  
  }

}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  if(GetRootPageId() == INVALID_PAGE_ID) {
    page_id_t page_id;
    Page* page = buffer_pool_manager_->NewPage(&page_id);
    root_page_id_ = page_id;
    auto newpage = reinterpret_cast<BPlusTreePage *>(page->GetData());
    reinterpret_cast<LeafPage *>(newpage)->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
    UpdateRootPageId(1);
    buffer_pool_manager_->UnpinPage(page_id, true);
  }
  BPlusTreePage* bPage;
  if(!TreeTraversal(key, &bPage)) {
    buffer_pool_manager_->UnpinPage(bPage->GetPageId(), false);
    return false;
  }
  MappingType pair(key, value);
  if(bPage->GetMaxSize() > bPage->GetSize()) {
    SimpleInsert(&pair, bPage);
    cur_size_++;
    return true;
  }
  if(bPage->IsRootPage()) {
    SimpleSplit(&pair, bPage, nullptr);
    cur_size_++;
    return true;
  }
  BPlusTreePage* parent_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(bPage->GetParentPageId())->GetData());
  parent_page = reinterpret_cast<InternalPage* >(parent_page);
  if(parent_page->GetMaxSize() > parent_page->GetSize()) {
    SimpleSplit(&pair, bPage, parent_page);
  }
  else {
    MultipleSplit(&pair, bPage, parent_page);  
  }
  cur_size_++;
  return true;

}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SimpleDelete(const MappingType* pair, BPlusTreePage* bPage) {
  
} 


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if(GetRootPageId() == INVALID_PAGE_ID) {
    return;
  }
  BPlusTreePage* bPage;
  if(TreeTraversal(key, &bPage)) {
    
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto tmp_page = reinterpret_cast<BPlusTreePage*>((buffer_pool_manager_->FetchPage(GetRootPageId()))->GetData());
  while(tmp_page->IsInternalPage()) {
    page_id_t pre_page_id = tmp_page->GetPageId();
    page_id_t page_id = reinterpret_cast<InternalPage*>(tmp_page)->ValueAt(0);
    tmp_page = reinterpret_cast<BPlusTreePage*>((buffer_pool_manager_->FetchPage(page_id))->GetData());
    buffer_pool_manager_->UnpinPage(pre_page_id, false);
  }
  return INDEXITERATOR_TYPE(tmp_page->GetPageId(), 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto tmp_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(GetRootPageId())->GetData());
  InternalPage* internal_page;
  LeafPage* leaf_page;
  page_id_t page_id;
  while(!tmp_page->IsLeafPage()) {
    page_id = tmp_page->GetPageId();
    internal_page = reinterpret_cast<InternalPage *>(tmp_page);
    page_id_t value = internal_page->Find(key, comparator_);
    tmp_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(value)->GetData());
    buffer_pool_manager_->UnpinPage(page_id, false);
  }
  leaf_page = reinterpret_cast<LeafPage *>(tmp_page);
  int index = 0;
  int size = leaf_page->GetSize();
  MappingType* array = leaf_page->Getarray_();
  while(index<size&&comparator_(array[index].first, key)<0) {
    index++;
  }
  return INDEXITERATOR_TYPE(leaf_page->GetPageId(), index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(INVALID_PAGE_ID, -1, buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
