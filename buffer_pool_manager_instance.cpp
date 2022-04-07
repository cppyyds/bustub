//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "buffer/lru_k_replacer.h"

#include "common/macros.h"

#include "tools/MutexLockGuard.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, 
                                                    DiskManager *disk_manager,
                                                    LogManager *log_manager)
    : pool_size_(pool_size),disk_manager_(disk_manager),
      log_manager_(log_manager)
    {
        assert(pool_size > 0);
        pages_ = new Page[pool_size];
        //replacer_ =  new lru_replacer<Page*,2>(pool_size,&pages_[0]);
        replacer_ =  new LRUReplacer<Page*>();

        for(size_t i=0;i<pool_size;++i){
            free_list_.emplace_back(&pages_[i]);
        }
    }

/*
BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}
*/
BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = page_table_.find(page_id);
  if(it == page_table_.end()){   //如果页不在缓冲区中
    return false;
  }
  disk_manager_->WritePage(page_id,it->second->GetData());
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for(auto& page_pair:page_table_){
    //page_pair.
    FlushPgImp(page_pair.first);
  }
}

/*
bool BufferPoolManagerInstance::find_replace(Page** page)
{

  if(!free_list_.empty()){
    *page = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  else{
    if(!replacer_->Victim(page)) return false;
  }
  return true;
}
*/


Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  Page* res = nullptr;
  if(!free_list_.empty()){
    res = free_list_.front();
    free_list_.pop_front();
  }
  else{
    if(!replacer_->Victim(&res)) return nullptr;
  }

  assert(res->pin_count_==0);
  *page_id = disk_manager_->AllocatePage();

  if(res->IsDirty()){
    disk_manager_->WritePage(res->page_id_,res->GetData());
  }
  //update page_table_
  page_table_.erase(res->page_id_);  //每次换出页面，都要删除表中的对应项

  page_table_.insert({*page_id,res});

  //initial meta data
  res->page_id_ = *page_id;
  res->is_dirty_ = false;
  res->pin_count_ = 1;
  res->ResetMemory();  //因为是新分配的一个页面，
                       //不是根据逻辑id拿的页面，所以页面数据都要置为0
  return res;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  /*
   这个函数就是一个进程/线程要访问一个逻辑id为page_id的页面。
   这个函数可以分以下情况分析
1.如果该页在缓冲池中直接访问并且记得把它的pin_count++，然后把调用Pin函数通知replacer
2.否则查找空闲链表，如果空间链表为空，尝试替换一页到磁盘，如果没有可替换的页，
直接返回空指针，如果有可换出的页，其页面为脏则要先将旧页上的数据写回磁盘
再将磁盘上page_id对应的逻辑页面数据写到缓存页面上来
3.当然如果替换页为空，择要建立新的page_table映射关系

*/
  std::lock_guard<std::mutex> lock(mutex_);
  assert(page_id!=INVALID_PAGE_ID);

  Page* res = nullptr;
  auto it = page_table_.find(page_id);
  if(it!=page_table_.end()){
    res = page_table_[page_id];

    //不加if(res->pin_count_++==0)语句的原因是为了兼容lru-k算法(无论如何都要调用Pin)
    replacer_->Pin(res);  //different
    return res;
  }
  if(!free_list_.empty()) //查找空闲链表
  {
    res = free_list_.front();
    free_list_.pop_front();
  }
  else{
    if(!replacer_->Victim(&res))  return nullptr;
  }
  assert(res->pin_count_==0);   //断言要被换出的页面没有被引用

  if(res->is_dirty_){
     //
     disk_manager_->WritePage(res->page_id_,res->GetData());
   }

  page_table_.erase(res->page_id_);
  page_table_.insert({page_id,res});

  disk_manager_->ReadPage(page_id,res->data_);

  res->pin_count_=1;
  res->page_id_=page_id;
  res->is_dirty_=false;
  
  return res;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  assert(page_id!=INVALID_PAGE_ID);
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = page_table_.find(page_id);
  if(it == page_table_.end()) return false;
  Page* page=it->second;
  if(page->GetPinCount()>0) return false;
  //if(page->is_dirty_) disk_manager_->WritePage(page_id,page->GetData());
  page_table_.erase(page_id);
  replacer_->Pin(page);
  disk_manager_->DeallocatePage(page_id);

  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  free_list_.push_back(page);
  
  return true;
}
/*
  如果一个进程/线程对已经完成对这个页的操作，我们需要unpin操作
  1.如果这个页的pin_count_>0，直接--
  2.如果pin_count_==0，我们需要将其添加到lru_replacer中，
  让它可以被替换出去
*/
bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) { 
  assert(page_id != INVALID_PAGE_ID);
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = page_table_.find(page_id);
  if(it==page_table_.end()) 
    return false;
  Page* page = it->second;
  if(page->pin_count_<=0){  //表明此页面已经被添加进replacer_
    return false;
  }    
  if(--page->pin_count_==0) replacer_->Unpin(page);
  if(is_dirty) page->is_dirty_=true;
  return true; 
}

/*
page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}
*/
void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
