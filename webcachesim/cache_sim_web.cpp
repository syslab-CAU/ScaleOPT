#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <map>
#include <cstdint>
#include <cstdlib>
#include <chrono>
#include <thread>
#include <cmath>
#include <sched.h>

#include "../readerwriterqueue/readerwriterqueue.h"


#define INIT_TRACE_SIZE 1<<20

#define CORE_THREAD 18
#define CPU_SOCKET 4

#define SIM_END -1

typedef long long offset_t;
typedef long long index_t;
typedef unsigned long long ULL;

ULL CACHE_SIZE = 0;
ULL BLOCK_SIZE = 1;
ULL STEP_COUNT = 0;
ULL THREAD_NUM = 1;

ULL MAX_OFFSET = 0;

using namespace std;

class BeladyCache {
private:
  // basic cache properties
  ULL _cacheSize; // size of cache in bytes
  ULL _currentSize; // total size of objects in cache in bytes

  // next_request -> id
  multimap<index_t, offset_t, greater<index_t>> _next_req_map;
  // only store in-cache object, value is size
  unordered_map<offset_t, ULL> _size_map;

  bool *ref_map;

  ULL hit=0, miss=0, cold_miss=0;


  class QueueEntry {
  public:
    pair<offset_t, index_t> req;
    index_t time;
  };

  thread *worker = NULL;
  moodycamel::ReaderWriterQueue<QueueEntry> wq;

  void worker_function();

public:
  BeladyCache(ULL sz, offset_t max_offset) : _cacheSize(sz), _currentSize(0) {
    ref_map = (bool *)calloc(max_offset+1, sizeof(bool));
  }

  ~BeladyCache() { free(ref_map); }

  bool lookup(offset_t id, int sz, index_t curtime, index_t tnr);
  void admit(offset_t id, int sz);
  void evict();
  bool has(const offset_t &id) { return _size_map.find(id) != _size_map.end(); }

  bool access(offset_t id, int sz, index_t curtime, index_t tnr);
  void put_work(pair<offset_t, index_t> ref, index_t curtime);

  void cache_hit() { hit++; }
  void cache_miss() { miss++; }
  void cache_cold_miss() { cold_miss++; }
  ULL get_hit() { return hit; }
  ULL get_miss() { return miss; }
  ULL get_cold_miss() { return cold_miss; }
  ULL get_total_processed() { return hit+miss+cold_miss; }

  void init_worker(int core_num);
  void join_worker();

  void print_statistics();
};

void BeladyCache::worker_function() {
  BeladyCache::QueueEntry e;
  while(true) {
    while(!wq.try_dequeue(e));
    if(e.req.first == SIM_END) break;
    access(e.req.first, BLOCK_SIZE, e.time, e.req.second);
  }
}

bool BeladyCache::lookup(offset_t id, int sz, index_t curtime, index_t tnr) {
  _next_req_map.emplace(tnr, id);
  auto if_hit = _size_map.find(id) != _size_map.end();
  //time to delete the past next_seq
  _next_req_map.erase(curtime);

  return if_hit;
}

void BeladyCache::admit(offset_t id, int sz) {
  // admit new object
  _size_map.insert({id, sz});
  _currentSize += sz;

  // check eviction needed
  while (_currentSize > _cacheSize) {
    evict();
  }
}

void BeladyCache::evict() {
  auto it = _next_req_map.begin();
  auto iit = _size_map.find(it->second);
  if (iit != _size_map.end()) {
    _currentSize -= iit->second;
    _size_map.erase(iit);
  }
  _next_req_map.erase(it);
}


bool BeladyCache::access(offset_t id, int sz, index_t curtime, index_t tnr) {
  bool ret = has(id);
  if(ret) { // hit
    cache_hit();
  } else {
    if(ref_map[id]) { // miss
          cache_miss();
    } else { // cold miss
      cache_cold_miss();
      ref_map[id] = true;
    }
    admit(id, sz);
  }
  lookup(id,sz,curtime,tnr);

  return ret;
}

void BeladyCache::put_work(pair<offset_t, index_t> ref, index_t curtime) {
  while(!wq.try_enqueue({ref, curtime}));
}

void BeladyCache::init_worker(int core_num) {
  auto nt = new thread(&BeladyCache::worker_function, this);
  cpu_set_t cs;
  CPU_ZERO(&cs);
  CPU_SET(core_num, &cs);
  int rc = pthread_setaffinity_np(nt->native_handle(), sizeof(cpu_set_t), &cs);
  if(rc != 0) {
    cout << "Affinity set error: " << rc << endl;
    exit(-1);
  }
  worker = nt;
}

void BeladyCache::join_worker() {
  if(worker != NULL)
    worker->join();
}


void BeladyCache::print_statistics() {
  cout << "[Cache statistics]"<< endl << endl;

  ULL cache_hit = get_hit();
  ULL cache_miss = get_miss();
  cold_miss = get_cold_miss();

  cout << "  Total cache access: " << cache_hit + cache_miss + cold_miss << endl;
  cout << "  Cache hit: " << cache_hit << endl;
  cout << "  Cache miss(w/o cold miss): " << cache_miss << endl;
  cout << "  Cache hit ratio: " << (double)cache_hit / (double)(cache_hit + cache_miss) << endl << endl;

  cout << "--------- end ---------" << endl;
}


// *************************
/* ---- main functions ---- */
// *************************

int calc_way(offset_t offset) {
  return offset % THREAD_NUM;
}

// Tag TNR to each reference
void annotate(vector<pair<offset_t, index_t>> &id_and_next_seq) {
  // get nextSeen indicess
  // assume id has same size
  unordered_map<offset_t, index_t> last_seen;
  for (index_t i = id_and_next_seq.size() - 1; i >= 0; --i) {
    offset_t current_id = id_and_next_seq[i].first;
    auto lit = last_seen.find(current_id);
    if (lit != last_seen.end())
      id_and_next_seq[i].second = lit->second;
    else
      id_and_next_seq[i].second = INT64_MAX;
    last_seen[current_id] = i;
    if (!(i % 1000000))
      cerr<<"computing next t: "<<i<<endl;
  }
}

void calculate_optimal_hit_ratio(vector<pair<offset_t, index_t>> *access_log) {
  cout << "Offset max value: " << MAX_OFFSET * BLOCK_SIZE << endl;
  // BeladyCache my_cache(CACHE_SIZE, MAX_OFFSET/BLOCK_SIZE);
  vector<BeladyCache *> caches;
  ULL cache_way_size = (ULL)ceil((CACHE_SIZE/BLOCK_SIZE)/THREAD_NUM)*BLOCK_SIZE;
  
  auto c = new BeladyCache(cache_way_size, MAX_OFFSET/BLOCK_SIZE);
  caches.push_back(c);
  for(int i=1; i<THREAD_NUM; i++) {
    c = new BeladyCache(cache_way_size, MAX_OFFSET/BLOCK_SIZE);
    c->init_worker(i);
    caches.push_back(c);
  }

  auto start = chrono::high_resolution_clock::now();
  annotate(*access_log);
  auto elapsed = chrono::high_resolution_clock::now() - start;
  cout << "Elapsed time for annotate: " << chrono::duration_cast<chrono::milliseconds>(elapsed).count() << " ms" << endl;

  cout << "Simulation start." << endl;
  start = chrono::high_resolution_clock::now();
  // for(index_t i=0; i < access_log->size(); i++) {
  //   auto cur_access = access_log->at(i);
  //   my_cache.access(cur_access.first, BLOCK_SIZE, i, cur_access.second);
  // }

  for(index_t i=0; i < access_log->size(); i++) {
    auto cur_access = access_log->at(i);
    int way = calc_way(cur_access.first);
    if(way) {
      caches[way]->put_work(cur_access, i);
    } else {
      caches[way]->access(cur_access.first, BLOCK_SIZE, i, cur_access.second);
    }
  }

  for(int i=1; i<caches.size(); i++) {
    caches[i]->put_work({-1, -1}, access_log->size());
  }
  for(int i=1; i<caches.size(); i++) {
    caches[i]->join_worker();
  }

  elapsed = chrono::high_resolution_clock::now() - start;

  // my_cache.print_statistics();
  for(int i=0; i<caches.size(); i++)
    caches[i]->print_statistics();


  cout << "Elapsed time for simulation: " << chrono::duration_cast<chrono::milliseconds>(elapsed).count() << " ms" << endl;
  //cout << "Elapsed time for simulation: " << chrono::duration_cast<chrono::seconds>(elapsed).count() << " secs." << endl;
}

ULL extract_access_log(ifstream &log_file, vector<pair<offset_t, index_t>> &access_log) {
  string line;
  while(getline(log_file, line)) {
    offset_t o = stoull(line);
    access_log.push_back(make_pair(o/BLOCK_SIZE, 0));
    if(MAX_OFFSET < o) MAX_OFFSET = o;
  }

  return access_log.size();
}


int main(int argc, char** argv) {
  if(argc < 5) {
    cout << "Usage: " << argv[0] << " [access log file]" << " [cache size]" << " [block size]" << " [thread #]" << endl;
    return -1;
  }

  CACHE_SIZE = stoull(argv[2]);
  BLOCK_SIZE = stoull(argv[3]);
  THREAD_NUM = stoull(argv[4]);

  vector<pair<offset_t,index_t>> access_log;
  access_log.reserve(INIT_TRACE_SIZE);
  ifstream log_file(argv[1]);
  if(log_file.is_open()) cout << "Total access count: " << extract_access_log(log_file, access_log) << endl;
  else {
    cout << "Unable to open file: " << argv[1] << endl;
    return -1;
  }
  log_file.close();

  cout << "Cache set (thread) #: " << THREAD_NUM << endl;
  // cout << "Cache size: " << CACHE_SIZE << ", Page size: " << BLOCK_SIZE << endl;


  auto start = chrono::high_resolution_clock::now();
  calculate_optimal_hit_ratio(&access_log);
  auto elapsed = chrono::high_resolution_clock::now() - start;

  cout << endl;

  cout << "Total execution time: " << chrono::duration_cast<chrono::milliseconds>(elapsed).count() << " ms" << endl;

  cout << endl << "Simulator completed." << endl;
  return 0;
}

