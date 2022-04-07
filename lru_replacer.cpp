
#include "buffer/lru_replacer.h"
#include "tools/MutexLockGuard.h"
#include <assert.h>

/*
    完成此文件的编写加深了我对双链表和unique_ptr的认知和使用
*/
namespace bustub {
template <typename T>
LRUReplacer<T>::LRUReplacer():size_(0),
                            head_(std::make_unique<node>()) {
                            tail_=head_.get();
                            }

template <typename T>
LRUReplacer<T>::~LRUReplacer() = default;


//从头部开始淘汰
template <typename T>
bool LRUReplacer<T>::Victim(T *value) { 
    MutexLockGuard lock(mutex_);

    if(size_==0){
        assert(head_.get()==tail_);
        return false;
    }

    *value = head_->next->data;
    head_->next = std::move(head_->next->next);  //原先地址为head_->next的结点空间会被回收释放
    if(head_->next != nullptr){
        head_->next->pre = head_.get();
    }
    table_.erase(*value);
    if(--size_==0){
        tail_ = head_.get();
    }
    check();
    return true;
}

template <typename T>
void LRUReplacer<T>::Pin(const T& value) {
    MutexLockGuard lock(mutex_);
    
    auto it=table_.find(value);
    if(it==table_.end()) return ;

        //版本一
        if(it->second!= tail_)
        {
            node*pre = it->second->pre;
            pre->next = std::move(pre->next->next);
            if(pre->next!=nullptr) pre->next->pre = pre;
        }
        else  //如果删除的是tail_指向的结点，需要移动tail_指针 ***
        { 
            tail_=tail_->pre;
            tail_->next.release();
        }
   /*
    //版本二，两个版本都可以
    if (it->second != tail_) {

      node *pre = it->second->pre;
      std::unique_ptr<node> cur = std::move(pre->next);
      pre->next = std::move(cur->next);
      pre->next->pre = pre;

    } else {
      tail_ = tail_->pre;
      tail_->next.release();
    }
    */

    table_.erase(value);
    if(--size_==0)
        tail_ = head_.get();
    check();

}

template <typename T>
void LRUReplacer<T>::Unpin(const T& value) {
    MutexLockGuard lock(mutex_);

    auto it=table_.find(value);
    if(it==table_.end()){   //如果value对应的结点不存在
        tail_->next = std::make_unique<node>(value,tail_);
        tail_ = tail_->next.get();
        table_.emplace(value,tail_);
        ++size_;
    }
    else{
        //如果存在，不是tail_结点。就将其移动到tail_来
        if(it->second!=tail_){
            node* pre = it->second->pre;
            std::unique_ptr<node> cur=std::move(pre->next);
            pre->next = std::move(cur->next);
            pre->next->pre = pre;

            cur->pre=tail_;
            tail_->next = std::move(cur);
            tail_ = tail_->next.get();
       }
    }

    check();
}

template<typename T> 
size_t LRUReplacer<T>::Size() { 
    MutexLockGuard lock(mutex_);
    return size_;
}

template<typename T> 
void LRUReplacer<T>::check()
{
    node *pointer = head_.get();
    while(pointer&&pointer->next!=nullptr){
        assert(pointer==pointer->next->pre);
        pointer = pointer->next.get();
    }
    assert(pointer==tail_);
    assert(table_.size()==size_);
}

template class LRUReplacer<int>;

}  // namespace bustub
