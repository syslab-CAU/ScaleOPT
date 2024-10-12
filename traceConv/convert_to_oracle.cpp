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

#pragma pack(4)

using namespace std;

typedef long long offset_t;
typedef long long index_t;
typedef unsigned long long ULL;

ULL BLOCK_SIZE = 1;
int THREAD_NUM = 1;

ULL MAX_OFFSET = 0;

string OUTPUT_FILE_NAME;

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

// *************************
/* ---- main functions ---- */
// *************************


typedef struct oracleGeneral_req {
  uint32_t clock_time;
  uint64_t obj_id;
  uint32_t obj_size;
  int64_t next_access_vtime;
} oracleGeneral_req_t;


void calculate_optimal_hit_ratio(vector<offset_t> *access_log) {
  cout << "Offset max value: " << MAX_OFFSET * BLOCK_SIZE << endl;

  auto start = chrono::high_resolution_clock::now();
  NextAccessMap my_access_map(access_log, MAX_OFFSET, THREAD_NUM);
  auto elapsed = chrono::high_resolution_clock::now() - start;
  cout << "Elapsed time for initializing AccessMap: " << chrono::duration_cast<chrono::milliseconds>(elapsed).count() << " ms" << endl;

  const string output_file = OUTPUT_FILE_NAME;
  std::ofstream ofile(output_file,
                      std::ios::out | std::ios::binary | std::ios::trunc);
  cout << "Creating oracleGeneral trace... filename: " << output_file << endl;

  cout << sizeof(uint32_t) << endl;
  cout << sizeof(oracleGeneral_req_t) << endl;

  for(index_t i=0; i < access_log->size(); i++) {
    offset_t cur_offset = access_log->at(i);
    my_access_map.pop_idx(cur_offset);

    oracleGeneral_req_t req;
    req.clock_time = 0;
    req.obj_id = (uint64_t)cur_offset;
    req.obj_size = 1;
    req.next_access_vtime =
      my_access_map.get_head_idx(cur_offset) == INT64_MAX 
      ? -1 : (int64_t)my_access_map.get_head_idx(cur_offset) + 1;

    ofile.write(reinterpret_cast<char *>(&req), sizeof(oracleGeneral_req_t));
  }

  ofile.close();

  cout << "Completed." << endl;
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
  if(argc < 5) {
    cout << "Usage: " << argv[0] << " [access log file]" << " [block size]" 
      << " [Thread #]" << " [output file name]" << endl;
    return -1;
  }
  
  BLOCK_SIZE = stoull(argv[2]);
  THREAD_NUM = atoi(argv[3]);
  OUTPUT_FILE_NAME = argv[4];

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
  return 0;
}

