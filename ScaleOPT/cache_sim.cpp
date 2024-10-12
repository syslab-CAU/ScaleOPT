#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <cstdlib>
#include <thread>
#include <mutex>
#include <algorithm>
#include <chrono>
#include <queue>
#include <atomic>
#include <functional>
#include <thread>
#include <unordered_map>
#include <cmath>

#include <sched.h>

#include "../readerwriterqueue/readerwriterqueue.h"
#include "../readerwriterqueue/readerwritercircularbuffer.h"
#include "../minmaxheap-cpp/MinMaxHeap.hpp"

// #define HIT_MISS_LOG
// #define EVICT_LOG
// #define DEBUG_LOG
// #define RUNTIME_LOG
// #define ASSIGN_TIME_LOG
// #define ASSIGNED_WORK_LOG
// #define THREAD_UPTIME_LOG

#define QUEUE_MAX_ENTRY (1<<16)-1
#define WAIT_DURATION 10

#define CORE_THREAD 18
#define CPU_SOCKET 4

#define POLLING_WORK

#define EMPTY_ENTRY -1
#define EMPTY_WORK -1

#define INIT_TRACE_SIZE 1<<20
#define INIT_AM_QUEUE_CAPACITY 16
#define AM_QUEUE_BLOCK_CAPACITY 16

using namespace std;

typedef long long offset_t;
typedef long long index_t;
typedef unsigned long long ULL;

ULL MIN_CACHE_SIZE = 0;
ULL MAX_CACHE_SIZE = 0;
ULL BLOCK_SIZE = 1;
ULL STEP_COUNT = 0;
int THREAD_NUM = 1;

ULL MAX_OFFSET = 0;

#ifdef RUNTIME_LOG
unsigned long long total_access_time = 0;
#endif
#ifdef ASSIGN_TIME_LOG
unsigned long long total_assign_time = 0;
#endif

class QueueBlock {
private:
  index_t *buf = NULL;
  QueueBlock *next_block = NULL;
  ULL size=0;

public:
  QueueBlock() {
    buf = (index_t *)malloc(sizeof(index_t) * AM_QUEUE_BLOCK_CAPACITY);
  }
  ~QueueBlock() { 
    if(next_block != NULL)
      delete next_block;
    free(buf); 
  }

  QueueBlock *insert(index_t tnr) { 
    if(size<AM_QUEUE_BLOCK_CAPACITY) {
      buf[size]=tnr;
      size++;
      return this;

    } else {
      next_block = new QueueBlock();
      next_block->insert(tnr);
      return next_block;
    }
  }
  index_t get_value(int idx) {
    if(idx < size)
      return buf[idx];
    else 
      return -1;
  }

  void set_next_block(QueueBlock *_next) { next_block = _next; }
  QueueBlock *get_next_block() { return next_block; }
};

class TNRQueue {
private:
  QueueBlock *head = NULL;
  QueueBlock *tail = NULL;

  QueueBlock *head_block = NULL;
  int block_cursor = 0;

public:
  TNRQueue() { }
  ~TNRQueue() { if(head != NULL) delete head; }

  void push(index_t entry) {
    if(head == NULL) {
      tail = head = new QueueBlock();
      head_block = head;
    }

    tail = tail->insert(entry);
  }

  index_t front() { 
    return empty() ? INT64_MAX : head_block->get_value(block_cursor); 
  }
  void pop() { 
    block_cursor++;
    if(head_block->get_value(block_cursor)==-1) {
      head_block = head_block->get_next_block();
      block_cursor = 0;
    }
  }

  bool empty() { return head_block==NULL; }

  void concat(TNRQueue *src) {
    if(head == NULL) {
      head = src->head;
      tail = src->tail;
      head_block = src->head_block;
    } else {
      tail->set_next_block(src->head);
      tail = src->tail;
    }

    src->head = NULL;
    src->tail = NULL;
    src->head_block = NULL;
  }
};


class NextAccessMap {
private:
  void create_part_map(int thread_no, bool *ready_flag, index_t begin_index, index_t end_index) {
    while(__atomic_load_n(ready_flag, __ATOMIC_RELAXED)==false);
    parts[thread_no] = new NextAccessMap(access_log, begin_index, end_index, distinct_blocks);
  }  

  void delete_part_map(int part_no) {
    delete parts[part_no];
  }

  TNRQueue *get_queue(index_t idx) {
    auto it = part_map.find(idx);
    if(it != part_map.end())
      return it->second;
    else
      return NULL;
  }

  void merge_worker(offset_t *counter, int total_thread) {
    offset_t my_number;
    
    my_number = __atomic_fetch_add(counter, 1, __ATOMIC_RELAXED);
    while(my_number <= max_val) {
      bool any_insertion = false;
      TNRQueue *q;
      for(int i=0; i<total_thread; i++) {
        auto part_q = parts[i]->get_queue(my_number);
        if(part_q != NULL) {
          if(!any_insertion) {
            any_insertion = true;
            q = new TNRQueue();
          }
          q->concat(part_q);
        }
      }

      if(any_insertion) {
        map[my_number] = q;
      }
      my_number = __atomic_fetch_add(counter, 1, __ATOMIC_RELAXED);
    }
  }

  index_t count_distinct_datablock(index_t begin_index, index_t end_index) {
    unordered_map<offset_t, bool> m;
    m.reserve(1<<22);

    index_t count = 0;
    for(index_t i = begin_index; i<end_index; i++) {
      offset_t cur_offset = access_log->at(i);
      if(m.find(cur_offset) == m.end()) {
        count++;
        m.insert(make_pair(cur_offset, true));
      }
    }

    return count;
  }

  vector<offset_t> *access_log;
  index_t access_count;
  offset_t max_val;
  vector<TNRQueue *> map;
  unordered_map<offset_t, TNRQueue*> part_map;
  offset_t distinct_blocks = 0;

  NextAccessMap *parts[CORE_THREAD];

  NextAccessMap(vector<offset_t> *_access_log, index_t begin, index_t end, offset_t _dblocks) 
  : access_log(_access_log), access_count(end-begin), distinct_blocks(_dblocks) {
    map.reserve(distinct_blocks);
    init(begin, end);
  }

public:
  NextAccessMap(vector<offset_t> *_access_log, offset_t _max_val, int _thread_num) 
  : access_log(_access_log), access_count(access_log->size()), max_val(_max_val), 
  map(max_val+1, NULL) {
    distinct_blocks = max_val+1;
    init_with_threads(_thread_num);

#ifdef DEBUG_LOG
    cout << "AccessMap init. completed." << endl;
#endif
  }

  ~NextAccessMap() {
    for(auto i : part_map) delete i.second;
  }

  void add_num(index_t idx, index_t acceess_num);
  void pop_idx(index_t idx);
  void pop_until_cursor(index_t idx, index_t cursor);
  index_t get_head_idx(index_t idx);

  index_t cast_to_map_index(index_t idx) { return idx; }

  void init(index_t begin, index_t end);
  void init_with_threads(int num_thread);
  index_t get_access_count() { return access_count; }
};


struct Work {
  index_t cur_access;
  index_t put_entry;
  bool is_cold;

  Work() : cur_access(-1), put_entry(-1), is_cold(false) {}
  Work(const index_t _cur_access, const index_t _put_entry, const bool _is_cold) 
  : cur_access(_cur_access), put_entry(_put_entry) , is_cold(_is_cold) {}
  Work(const Work &_work) 
  : cur_access(_work.cur_access), put_entry(_work.put_entry), is_cold(_work.is_cold) {}
};

class MultiCache;

class PartCache {
private:
  MultiCache *parent_cache;
  PartCache *next_level_cache;

  index_t part_num;
#ifdef POLLING_WORK
  moodycamel::ReaderWriterQueue<Work> *next_wq = NULL;
#else
  moodycamel::BlockingReaderWriterCircularBuffer<Work> *next_wq = NULL;
#endif 

  minmax::MinMaxHeap<index_t> tree;
#ifdef POLLING_WORK
  moodycamel::ReaderWriterQueue<Work> wq;
#else
  moodycamel::BlockingReaderWriterCircularBuffer<Work> wq;
#endif

  index_t total_cache_block_num;
  index_t total_access_count;

  ULL hit=0, miss=0, cold_miss=0;

#ifdef ASSIGNED_WORK_LOG
  ULL processed_work = 0;
#endif
#ifdef THREAD_UPTIME_LOG
  unsigned int uptime = 0;
#endif

  bool direct_operate_next_level = false;

  void worker_function();
  inline void process_work(index_t cur_access, index_t put_entry, bool is_cold);

public:
  PartCache(MultiCache *_parent_cache, index_t _part_num, 
  index_t _total_cache_block_num, index_t _total_access_count,
  bool is_direct_to_next) 
  : parent_cache(_parent_cache), part_num(_part_num),
  total_cache_block_num(_total_cache_block_num),
  total_access_count(_total_access_count), wq(QUEUE_MAX_ENTRY),
  direct_operate_next_level(is_direct_to_next) {}

  void put_work(index_t cur_access, index_t put_entry, bool is_cold=false) { 
  #ifdef POLLING_WORK
    while(!wq.try_enqueue(Work(cur_access, put_entry, is_cold))); 
  #else
    wq.wait_enqueue(Work(cur_access, put_entry, is_cold)); 
  #endif
  }

  void put_work_to_next_level(index_t cur_access, index_t put_entry, bool is_cold=false)
  { 
    if(direct_operate_next_level) {
      next_level_cache->process_work(cur_access, put_entry, is_cold);
      return;
    }
    if(next_wq != NULL) 
    #ifdef POLLING_WORK
      while(!next_wq->try_enqueue(Work(cur_access, put_entry, is_cold))); 
    #else
      next_wq->wait_enqueue(Work(cur_access, put_entry, is_cold)); 
    #endif
  }

  bool search(index_t access_time) { return tree.findMin() == access_time; }
  index_t put(index_t cur_access);
  void search_and_put(index_t cur_access, index_t put_entry, bool is_cold=false);

  void update(index_t next_access);

  void run_thread() { 
    worker_function();
  }

  void set_next_level(PartCache *next_part) { 
    next_level_cache = next_part;
    next_wq = &(next_part->wq); 
  }

  void cache_hit() { hit++; }
  void cache_miss() { miss++; }
  void cache_cold_miss() { cold_miss++; }
  ULL get_hit() { return hit; }
  ULL get_miss() { return miss; }
  ULL get_cold_miss() { return cold_miss; }
  ULL get_total_processed() { return hit+miss+cold_miss; }

#ifdef ASSIGNED_WORK_LOG
  ULL get_processed_work() { return processed_work; }
#endif
#ifdef THREAD_UPTIME_LOG
  void set_uptime(int _uptime) { uptime = _uptime; }
  int get_uptime() { return uptime; }
#endif
};

class MultiCache {
private:
  index_t min_size;
  index_t max_size;
  index_t step_size;
  ULL block_size;

  index_t part_cache_num;
  int thread_num;

  vector<PartCache *> caches;
  vector<thread *> threads;
  vector<int> affinity_set;

  vector<index_t> target_size_caches;
  vector<ULL> pcache_sizes;

  NextAccessMap &access_map;
  
  ULL hit=0, miss=0;
  ULL cold_miss=0;

  bool *ref_map;


  void create_and_run_part_cache_with_affinity(int core_num, ULL cache_size) {
    while(__atomic_load_n(&affinity_set[core_num], __ATOMIC_RELAXED)==0);
    auto p = new PartCache(this, core_num, cache_size, access_map.get_access_count(), false);
    __atomic_exchange_n(&caches[core_num], p, __ATOMIC_RELAXED);
    if(core_num != thread_num-1){
      while((PartCache *)__atomic_load_n(&caches[core_num+1], __ATOMIC_RELAXED) == NULL);
      caches[core_num]->set_next_level(caches[core_num+1]);
    }

    caches[core_num]->run_thread();
  }

  void create_and_run_part_caches_with_affinity(int core_num, int cache_num_begin, int cache_num_end) {
    while(__atomic_load_n(&affinity_set[core_num], __ATOMIC_RELAXED)==0);
    for(int i=cache_num_begin; i<cache_num_end; i++) {
      ULL cache_size = (pcache_sizes[i] - pcache_sizes[i-1])/block_size;
      auto p = new PartCache(this, i, cache_size, access_map.get_access_count(), i<(cache_num_end-1));
      __atomic_exchange_n(&caches[i], p, __ATOMIC_RELAXED);
    }

    for(int i=cache_num_begin; i<cache_num_end-1; i++) {
      caches[i]->set_next_level(caches[i+1]);
    }

    if(cache_num_end != part_cache_num) {
      while((PartCache *)__atomic_load_n(&caches[cache_num_end], __ATOMIC_RELAXED)==NULL);
      caches[cache_num_end-1]->set_next_level(caches[cache_num_end]);
    }

    caches[cache_num_begin]->run_thread();
  }

  void create_new_thread_multi_pcache(int core_num, int cache_num_begin, int cache_num_end) {
    ULL cache_size = (pcache_sizes[cache_num_begin]-pcache_sizes[cache_num_begin-1])/block_size;
    auto nt = new thread(&MultiCache::create_and_run_part_caches_with_affinity, this, core_num, cache_num_begin, cache_num_end);
    cpu_set_t cs;
    CPU_ZERO(&cs);
    CPU_SET(core_num, &cs);
    int rc = pthread_setaffinity_np(nt->native_handle(), sizeof(cpu_set_t), &cs);
    if(rc != 0) {
      cout << "Affinity set error: " << rc << endl;
      exit(-1);
    }
    threads.push_back(nt);
    __atomic_exchange_n(&affinity_set[core_num], 1, __ATOMIC_RELAXED);

    for(int i=cache_num_begin+1; i<cache_num_end; i++) {

    }
  }

  void create_new_thread_single_pcache(int core_num, ULL cache_size) {
    auto nt = new thread(&MultiCache::create_and_run_part_cache_with_affinity, this, core_num, cache_size);
    cpu_set_t cs;
    CPU_ZERO(&cs);
    CPU_SET(core_num, &cs);
    int rc = pthread_setaffinity_np(nt->native_handle(), sizeof(cpu_set_t), &cs);
    if(rc != 0) {
      cout << "Affinity set error: " << rc << endl;
      exit(-1);
    }
    threads.push_back(nt);
    __atomic_exchange_n(&affinity_set[core_num], 1, __ATOMIC_RELAXED);
  }

public:
  MultiCache(index_t _min_size, index_t _max_size, ULL _block_size, 
    index_t _part_cache_num, offset_t max_offset, NextAccessMap &_access_map,
    int _thread_num) 
  : min_size(_min_size), max_size(_max_size), part_cache_num(_part_cache_num),
  access_map(_access_map), affinity_set(_thread_num, 0), 
  caches(_part_cache_num, NULL), block_size(_block_size), thread_num(_thread_num) {
    ref_map = (bool *)calloc((max_offset) + 1, sizeof(bool));
    
    if(min_size == max_size) { // Single size simulation
      part_cache_num = thread_num;
      caches.resize(thread_num, NULL);
      target_size_caches.push_back(thread_num-1);
      ULL min_pcache_size = (ULL)ceil((double)(max_size/block_size)/(double)thread_num);
      step_size = (double)(max_size/block_size)/(double)thread_num;

      caches[0] = new PartCache(this, 0,  min_pcache_size, access_map.get_access_count(), false);
      pcache_sizes.push_back(min_pcache_size*block_size);
      for(int i=1; i<thread_num-1; i++) {
        create_new_thread_single_pcache(i, step_size);
        pcache_sizes.push_back(pcache_sizes[i-1] + (step_size*block_size));
      }
      if(thread_num-1 > 0) {
        ULL last_step_pcache_size = (max_size/block_size)-(pcache_sizes[thread_num-2]/block_size);
        create_new_thread_single_pcache(thread_num-1, last_step_pcache_size);
        pcache_sizes.push_back(max_size);
      }


      for(int i=1; i<part_cache_num; i++) {
        while((PartCache *)__atomic_load_n(&caches[i], __ATOMIC_RELAXED) == NULL);
      }
      if(part_cache_num > 1)
        caches[0]->set_next_level(caches[1]);
      return;
    }


    if(thread_num==1) { // Single thread - multi size simulation
      caches[0] = new PartCache(this, 0,  min_size/block_size, access_map.get_access_count(), true);
      ULL target_step_size = (ULL)floor(((max_size-min_size)/block_size)/(part_cache_num-1)) * block_size;
      pcache_sizes.push_back(min_size);
      target_size_caches.push_back(0);
      for(int i=1; i<part_cache_num-1; i++) {
        caches[i] = new PartCache(this, i, target_step_size/block_size, access_map.get_access_count(), true);
        caches[i-1]->set_next_level(caches[i]);
        pcache_sizes.push_back(pcache_sizes[i-1]+target_step_size);
        target_size_caches.push_back(i);
      }
      ULL last_step_size = max_size - pcache_sizes[part_cache_num-2];
      caches[part_cache_num-1] = new PartCache(this, 0,  last_step_size/block_size, access_map.get_access_count(), false);
      caches[part_cache_num-2]->set_next_level(caches[part_cache_num-1]);
      pcache_sizes.push_back(max_size);
      target_size_caches.push_back(part_cache_num-1);
      return;
    }

    // Multi thread - multi size simulation
    if(thread_num < part_cache_num) {
      ULL target_step_size = (ULL)floor(((max_size-min_size)/block_size)/(part_cache_num-1)) * block_size;
      int pcache_per_thread = (int)floor((double)part_cache_num/(double)thread_num);
      vector<int> pcache_num(thread_num, pcache_per_thread);
      vector<int> pcache_thread_mapping(part_cache_num, 0);
      int alloc_pcache_sum = pcache_per_thread*thread_num;

      if(min_size > target_step_size * pcache_per_thread) {
        pcache_per_thread = (int)floor((double)(part_cache_num-1)/(double)(thread_num-1));
        pcache_num[0]=1;
        for(int i=1; i<pcache_num.size(); i++) pcache_num[i]=pcache_per_thread;
        alloc_pcache_sum = 1 + pcache_per_thread * (thread_num-1);
      }

      int left_pcache_num = part_cache_num - alloc_pcache_sum;
      for(int i=0; i<left_pcache_num; i++) {
        pcache_num[thread_num-1-i]++;
      }
      for(int i=1; i<thread_num; i++) {
        pcache_num[i] += pcache_num[i-1];
      }

      for(int i=1, pos=pcache_num[0]; i<thread_num; i++) {
        for(; pos<pcache_num[i]; pos++) {
          pcache_thread_mapping[pos] = i;
        }
      }

      pcache_sizes.push_back(min_size);
      target_size_caches.push_back(0);
      for(int i=1; i<part_cache_num; i++) {
        pcache_sizes.push_back(pcache_sizes[i-1]+target_step_size);
        target_size_caches.push_back(i);
      }
      pcache_sizes[part_cache_num-1] = max_size;

      caches[0] = new PartCache(this, 0, min_size/block_size, access_map.get_access_count(), pcache_num[0]>1);
      for(int i=1; i<pcache_num[0]; i++) {
          caches[i] = new PartCache(this, i, target_step_size/block_size, access_map.get_access_count(), i<(pcache_num[0]-1));
          caches[i-1]->set_next_level(caches[i]);
      }

      for(int i=1; i<thread_num; i++) {
        create_new_thread_multi_pcache(i, pcache_num[i-1], pcache_num[i]);
      }

      for(int i=1; i<part_cache_num; i++) {
        while((PartCache *)__atomic_load_n(&caches[i], __ATOMIC_RELAXED) == NULL);
      }
      caches[pcache_num[0]-1]->set_next_level(caches[pcache_num[0]]);
      return;

    } else {
      caches.resize(thread_num, NULL);

      double min_per_max = ((double)min_size/(double)max_size);
      double target_ratio = 1 - min_per_max;
      int target_pcache_interval_count = (int)round((target_ratio*(double)thread_num)/(part_cache_num-1));
      if(target_pcache_interval_count == 0) target_pcache_interval_count = 1;
      int target_pcache_count = target_pcache_interval_count * (part_cache_num-1);
      int before_min_pcache_count = thread_num - target_pcache_count;
      if(before_min_pcache_count == 0) {
        target_pcache_interval_count--;
        target_pcache_count = target_pcache_interval_count * (part_cache_num - 1);
        before_min_pcache_count = thread_num - target_pcache_count;
      }

      // 0~min pcaches
      int i=0;
      ULL before_min_pcache_size = (ULL)floor((double)(min_size/block_size)/(double)before_min_pcache_count) * block_size;
      if(before_min_pcache_count > 1) {   
        caches[0] = new PartCache(this, 0, before_min_pcache_size/block_size, access_map.get_access_count(), false);
        pcache_sizes.push_back(before_min_pcache_size);
        i++;
        for(;i<before_min_pcache_count-1;i++) {
          create_new_thread_single_pcache(i, before_min_pcache_size/block_size);
          pcache_sizes.push_back(before_min_pcache_size*(i+1));
        }

        create_new_thread_single_pcache(i, (min_size-pcache_sizes[i-1])/block_size);
        pcache_sizes.push_back(min_size);
        target_size_caches.push_back(i); // min pcache index

      } else { // min size pcache is the first pcache
        caches[0] = new PartCache(this, 0, min_size/block_size, access_map.get_access_count(), false);
        pcache_sizes.push_back(min_size);
        target_size_caches.push_back(0);
      }

      i++;

      // min~max pcaches
      ULL target_pcache_step_size = (ULL)floor((double)((max_size-min_size)/block_size)/(double)target_pcache_count) * block_size;
      ULL target_step_size = (((max_size-min_size)/block_size)/(part_cache_num-1)) * block_size;
      ULL target_pcache_last_step_size = target_step_size-(target_pcache_step_size*(target_pcache_interval_count-1));
      for(; i<thread_num; i++) {
        for(int j=0; j<(target_pcache_interval_count-1); j++, i++) {
          create_new_thread_single_pcache(i, target_pcache_step_size/block_size);
          pcache_sizes.push_back(pcache_sizes[i-1]+target_pcache_step_size);
        }

        if(i==thread_num-1) {
          create_new_thread_single_pcache(i, (max_size-pcache_sizes[i-1])/block_size);
          pcache_sizes.push_back(max_size);
        } else {
          create_new_thread_single_pcache(i, target_pcache_last_step_size/block_size);
          pcache_sizes.push_back(pcache_sizes[i-1]+target_pcache_last_step_size);
        }
        target_size_caches.push_back(i);
      }

      for(int i=1; i<thread_num; i++) {
        while((PartCache *)__atomic_load_n(&caches[i], __ATOMIC_RELAXED) == NULL);
      }
      caches[0]->set_next_level(caches[1]);
      return;
    }

  }

  ~MultiCache() { 
    for(auto *i : caches) delete i;
    free(ref_map);
  }

  void hit_from(index_t cache_num) {
    __atomic_add_fetch(&hit, 1, __ATOMIC_RELAXED);
  }
  void miss_in(index_t cache_num) {
    if(cache_num == part_cache_num - 1)
      miss++;
  }


  void access(offset_t offset);
  void assign_task(index_t cache_num, index_t cur_access, index_t put_entry, bool is_cold) { 
    if(cache_num < part_cache_num - 1) {
      caches[cache_num]->put_work(cur_access, put_entry, is_cold); 
    }
  }

  index_t get_total_access_of_part(index_t cache_num) 
    { return hit+miss+cold_miss; }

  void join_part_cache_threads();
  void join_part_cache_thread(index_t cache_num);

  void print_statistics();

  ULL get_miss(index_t cache_num) { return miss; }
  ULL get_cold_miss() { return cold_miss; }

#ifdef ASSIGNED_WORK_LOG
  void print_work_distribution() {
    cout << "[Work distribution]" << endl;
    cout << "  Cache#0 : " << access_map.get_access_count() << endl;
    for(int i=1; i<caches.size(); i++) {
      cout << "  Cache#" << i << " : ";
      cout << caches[i]->get_processed_work() << endl;
    }
    cout << endl;
  }
#endif

#ifdef THREAD_UPTIME_LOG
  void set_uptime_of_first_part(unsigned int uptime) {
    caches[0]->set_uptime(uptime);
  }

  void print_thread_uptime() {
    cout << "[Thread uptime]" << endl;
    for(int i=0; i<caches.size(); i++) {
      cout << "  Cache#" << i << ": ";
      cout << caches[i]->get_uptime() << " ms" << endl;
    }
    cout << endl;
  }
#endif
};




// ********************************************
/* ---- Member functions of NextAccessMap ---- */
// ********************************************
void NextAccessMap::add_num(index_t idx, index_t access_num) {
  map[idx]->push(access_num);
}

void NextAccessMap::pop_idx(index_t idx) {
  map[idx]->pop();
}

void NextAccessMap::pop_until_cursor(index_t idx, index_t cursor) {
  if(map[idx]->empty()) return;
  index_t cur = map[idx]->front();
  while(cur <= cursor) {
    map[idx]->pop();
    if(map[idx]->empty()) return;
    cur = map[idx]->front();
  }
}

index_t NextAccessMap::get_head_idx(index_t idx) {
  return map[idx]->front();
}


void NextAccessMap::init(index_t begin, index_t end) {
  offset_t cur_offset;
  for(index_t i=begin; i<end; i++) {
    cur_offset = access_log->at(i);
    auto it = part_map.find(cur_offset);
    if(it == part_map.end()) {
      auto ins_ret = part_map.insert(make_pair(cur_offset, new TNRQueue()));
      it = ins_ret.first;
    }
    it->second->push(i);
  }
}



void NextAccessMap::init_with_threads(int num_thread) {
  if(num_thread > CORE_THREAD) {
    num_thread = CORE_THREAD;
  }

  vector<thread *> ths;
  bool run_ready[CORE_THREAD] = {false, };

  index_t begin, end;
  for(int i=1; i<num_thread; i++) {
    begin = (access_count/num_thread) * i;
    end = i == num_thread-1 ? access_count : (access_count/num_thread) * (i+1);
    auto nt = new thread(&NextAccessMap::create_part_map, this, i, run_ready+i, begin, end);
    cpu_set_t cs;
    CPU_ZERO(&cs);
    CPU_SET(i, &cs);
    int rc = pthread_setaffinity_np(nt->native_handle(), sizeof(cpu_set_t), &cs);
    if(rc != 0) {
      cout << "Affinity set error: " << rc << endl;
      exit(-1);
    }
    __atomic_exchange_n(run_ready+i, true, __ATOMIC_RELAXED);
    ths.push_back(nt);
  }
  begin = 0;
  end = access_count/num_thread;
  parts[0] = new NextAccessMap(access_log, begin, end, distinct_blocks);
  for(auto th : ths) {
    th->join();
  }
  ths.clear();
  cout << "Part AccessMap creation complete." << endl;


  offset_t counter = 0;
  for(int i=1; i<num_thread; i++) {
    auto nt = new thread(&NextAccessMap::merge_worker, this, &counter, num_thread);
    cpu_set_t cs;
    CPU_ZERO(&cs);
    CPU_SET(i, &cs);
    int rc = pthread_setaffinity_np(nt->native_handle(), sizeof(cpu_set_t), &cs);
    if(rc != 0) {
      cout << "Affinity set error: " << rc << endl;
      exit(-1);
    }
    ths.push_back(nt);
  }
  merge_worker(&counter, num_thread);
  for(auto th : ths) {
    th->join();
  }
  ths.clear();
  cout << "AccessMap aggregation complete." << endl;

  for(int i=1; i<num_thread; i++) {
    ths.push_back(new thread(&NextAccessMap::delete_part_map, this, i));
  }
  delete parts[0];
  for(auto th :ths) {
    th->join();
  }
  cout << "Free-up part. AccessMap complete." << endl;

  return;
}





// *****************************************
/* ---- Member functions of MultiCache ---- */
// *****************************************


void MultiCache::access(offset_t offset) {
  index_t cur_access = access_map.get_head_idx(offset);
  access_map.pop_idx(offset);
  index_t next_access = access_map.get_head_idx(offset);
  index_t evicted_block;
  
  
#ifdef RUNTIME_LOG
  auto start = chrono::high_resolution_clock::now();
#endif
  bool is_cold = !ref_map[offset];
  if(!is_cold) {
    if(caches[0]->search(cur_access)) { // hit from MIN size
      caches[0]->update(next_access);
      // hit_from(0);
      caches[0]->cache_hit();

      cur_access = EMPTY_WORK;
      evicted_block = 0;
    }
    else { // miss in MIN size
      evicted_block = caches[0]->put(next_access);
      // miss_in(0);
      caches[0]->cache_miss();
    }
  } else { // cold miss
    ref_map[offset] = true;
    // cold_miss++;
    caches[0]->cache_cold_miss();
    evicted_block = caches[0]->put(next_access);

    if(evicted_block == EMPTY_ENTRY) {
      cur_access = EMPTY_WORK;
      evicted_block = 0;
    }
  }

  if(part_cache_num > 1) {
#ifdef ASSIGN_TIME_LOG
    auto start = chrono::high_resolution_clock::now();
#endif
    caches[0]->put_work_to_next_level(cur_access, evicted_block, is_cold);
#ifdef ASSIGN_TIME_LOG
    auto elapsed = chrono::high_resolution_clock::now() - start;
    total_assign_time += chrono::duration_cast<chrono::nanoseconds>(elapsed).count();
#endif  
  }
    

#ifdef RUNTIME_LOG
  auto elapsed = chrono::high_resolution_clock::now() - start;
  total_access_time += chrono::duration_cast<chrono::nanoseconds>(elapsed).count();
#endif
}

void MultiCache::join_part_cache_threads() {
  for(auto t : threads) t->join();
}

void MultiCache::join_part_cache_thread(index_t cache_num) {
  if(cache_num < part_cache_num - 1)
    threads[cache_num]->join();
}


void MultiCache::print_statistics() {
  cout << "[Cache statistics]"<< endl << endl;

  for(int i=0; i<target_size_caches.size(); i++) {
    index_t cache_index = target_size_caches[i];
    cout << "Cache size: " << pcache_sizes[cache_index] << endl;

    ULL cache_hit = caches[cache_index]->get_hit();
    ULL cache_miss = caches[cache_index]->get_miss();
    cold_miss = caches[cache_index]->get_cold_miss();

    cout << "  Total cache access: " << cache_hit + cache_miss + cold_miss << endl;
    cout << "  Cache hit: " << cache_hit << endl;
    cout << "  Cache miss(w/o cold miss): " << cache_miss << endl;
    cout << "  Cache hit ratio: " << (double)cache_hit / (double)(cache_hit + cache_miss) << endl << endl;     
  }

  cout << "--------- end ---------" << endl;
}




// ****************************************
/* ---- Member functions of PartCache ---- */
// ****************************************

index_t PartCache::put(index_t cur_access) {
  if(tree.size() < total_cache_block_num) {
    tree.push(cur_access);
    return EMPTY_ENTRY;
  }

  if(tree.findMax() < cur_access)
    return cur_access;

  index_t evicted_block = tree.popMax();
  tree.push(cur_access);
  return evicted_block;
}

void PartCache::search_and_put(index_t cur_access, index_t put_entry, bool is_cold) {
#ifdef ASSIGNED_WORK_LOG
  processed_work++;
#endif
  
  if(tree.empty()) { // cold miss
    tree.push(put_entry);
    cache_cold_miss();
    put_work_to_next_level(EMPTY_WORK, 0, true);
    return;
  }
  
  if(search(cur_access)) { // Hit from current cache size
    update(put_entry);

    cache_hit();

    cur_access = EMPTY_WORK;
    put_entry = 0;
  } else { // Passes evicted block to the next level cache
    if(!is_cold) cache_miss();
    else cache_cold_miss();

    index_t new_put_entry = put(put_entry);
    if(new_put_entry != EMPTY_ENTRY)
      put_entry = new_put_entry;
    else {
      cur_access = EMPTY_WORK;
      put_entry = 0;
    }
  }

  put_work_to_next_level(cur_access, put_entry, is_cold);
}

void PartCache::update(index_t next_access) {
  tree.popMin();
  tree.push(next_access);
}

inline void PartCache::process_work(index_t cur_access, index_t put_entry, bool is_cold) {
  if(cur_access == EMPTY_WORK) {
    if(!is_cold) cache_hit();
    else cache_cold_miss();
    put_work_to_next_level(cur_access, put_entry, is_cold);
  }
  else search_and_put(cur_access, put_entry, is_cold);
}

void PartCache::worker_function() {
  Work w;
#ifdef THREAD_UPTIME_LOG
  auto start = chrono::high_resolution_clock::now();
#endif

  while(get_total_processed() < total_access_count) {
  #ifdef POLLING_WORK
    if(wq.try_dequeue(w)) {
  #else
    wq.wait_dequeue(w);
  #endif
  
      process_work(w.cur_access, w.put_entry, w.is_cold);
    
  #ifdef POLLING_WORK
    }
  #endif

  }
#ifdef THREAD_UPTIME_LOG
  auto elapsed = chrono::high_resolution_clock::now() - start;
  set_uptime(chrono::duration_cast<chrono::milliseconds>(elapsed).count());
#endif
}

// *************************
/* ---- main functions ---- */
// *************************


void calculate_optimal_hit_ratio(vector<offset_t> *access_log) {
  cout << "Offset max value: " << MAX_OFFSET * BLOCK_SIZE << endl;

  auto start = chrono::high_resolution_clock::now();
  NextAccessMap my_access_map(access_log, MAX_OFFSET, THREAD_NUM);
  auto elapsed = chrono::high_resolution_clock::now() - start;
  MultiCache my_cache(MIN_CACHE_SIZE, MAX_CACHE_SIZE, BLOCK_SIZE, STEP_COUNT,
    MAX_OFFSET, my_access_map, THREAD_NUM);
  cout << "Elapsed time for initializing AccessMap: " << chrono::duration_cast<chrono::milliseconds>(elapsed).count() << " ms" << endl;

  cout << "Simulation start." << endl;
  start = chrono::high_resolution_clock::now();
  for(index_t i=0; i < access_log->size(); i++)
    my_cache.access(access_log->at(i));
#ifdef THREAD_UPTIME_LOG
  elapsed = chrono::high_resolution_clock::now() - start;
  my_cache.set_uptime_of_first_part(chrono::duration_cast<chrono::milliseconds>(elapsed).count());
#endif

  my_cache.join_part_cache_threads();
  elapsed = chrono::high_resolution_clock::now() - start;

#ifdef THREAD_UPTIME_LOG
  my_cache.print_thread_uptime();
#endif
#ifdef ASSIGNED_WORK_LOG
  my_cache.print_work_distribution();
#endif

  my_cache.print_statistics();

#ifdef RUNTIME_LOG
  cout << "Total time in access: " << total_access_time << "ns" << endl;
  cout << "Avg. time per access: " << total_access_time / access_log->size() << "ns" << endl;
#endif
#ifdef ASSIGN_TIME_LOG
  cout << "Total assign time: " << total_assign_time << "ns" << endl;
  cout << "Avg. time per assign: " << total_assign_time / (my_cache.get_miss(0)+my_cache.get_cold_miss()) << "ns" << endl;
#endif

  cout << "Elapsed time for simulation: " << chrono::duration_cast<chrono::milliseconds>(elapsed).count() << " ms" << endl;
}

ULL extract_access_log(ifstream &log_file, vector<offset_t> &access_log) {
  string line;
  while(getline(log_file, line)) {
    access_log.push_back(stoull(line)/BLOCK_SIZE);
  }

  MAX_OFFSET = *max_element(access_log.begin(), access_log.end());

  return access_log.size();
}


int main(int argc, char** argv) {
  if(argc < 7) {
    cout << "Usage: " << argv[0] << " [access log file]" << " [min cache size]" 
      << " [max cache size]" << " [step count]" << " [block size]" << " [Thread #]" << endl;
    return -1;
  }

  MIN_CACHE_SIZE = stoull(argv[2]);
  MAX_CACHE_SIZE = stoull(argv[3]);
  STEP_COUNT = stoull(argv[4]);
  BLOCK_SIZE = stoull(argv[5]);
  THREAD_NUM = atoi(argv[6]);

  vector<offset_t> access_log;
  access_log.reserve(INIT_TRACE_SIZE);
  ifstream log_file(argv[1]);
  if(log_file.is_open()) cout << "Total access count: " << extract_access_log(log_file, access_log) << endl;
  else {
    cout << "Unable to open file: " << argv[1] << endl;
    return -1;
  }
  log_file.close();


  auto start = chrono::high_resolution_clock::now();
  calculate_optimal_hit_ratio(&access_log);
  auto elapsed = chrono::high_resolution_clock::now() - start;

  cout << endl;

  cout << "Total execution time: " << chrono::duration_cast<chrono::milliseconds>(elapsed).count() << " ms" << endl;

  cout << endl << "Simulator completed." << endl;
  return 0;
}
