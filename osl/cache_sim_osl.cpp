#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstdlib>
#include <unordered_map>
#include <chrono>
#include <algorithm>
#include <map>
#include <set>
#include <cmath>
#include <cassert>
#include <queue>
#include <list>

#define ALPHA 0.5
#define INITIAL_CAPACITY 32

using namespace std;

typedef long long index_t;
typedef long long offset_t;
typedef unsigned long long ULL;

unsigned int BLOCK_SIZE = 1;

#define ACCESS_PERF_LOG
#ifdef ACCESS_PERF_LOG
unsigned long long total_alloc_time = 0;
unsigned long long total_tick_time = 0;
unsigned long long total_execution_time = 0;
#endif


class Bucket;
class BucketGroup;

class PPUCBlock {
public:
    bool finished;
    offset_t block;
    index_t new_lease;
    double ppuc;
};


class BucketMapper {
public:
    Bucket* bucket;
    list<offset_t>::iterator it;
};

class Bucket {
private:
    BucketGroup *group;
    int my_idx;

    ULL L;
    ULL counter = 0;

    ULL dumping_interval;

    list<offset_t> buffer;

public:
    Bucket(BucketGroup *_group, int _my_idx,  ULL _l, ULL _init_counter=0);

    void tick();
    void flush();

    void set_counter(ULL c) { counter = c; }

    list<offset_t> &get_buffer() { return buffer; }
    ULL get_counter() { return counter; }

    list<offset_t>::iterator insert_block(offset_t block);
    void delete_block(list<offset_t>::iterator it);
};

class BucketGroup {
private:
    BucketGroup *prev_group;
    BucketGroup *next_group = NULL;
    unordered_map<offset_t, BucketMapper> &it_map;

    ULL L;

    vector<Bucket> buckets;
    ULL insert_idx = 0;
    ULL step;

public:
    BucketGroup(ULL _l, double alpha, unordered_map<offset_t, BucketMapper> &_it_map,
        BucketGroup *_prev_group = NULL) : L(_l), it_map(_it_map), prev_group(_prev_group) {
        int total_bucket_count = (int)floor(2/alpha);
        if(L==1) total_bucket_count = 1;
        else if(total_bucket_count > L/2) total_bucket_count = L/2;

        if(prev_group != NULL)
            step = (L - prev_group->L) / total_bucket_count;
        else
            step = 0;
        
        insert_idx = total_bucket_count - 1;
        for(int i=insert_idx; i>=0; i--) {
            buckets.push_back(Bucket(this, insert_idx - i, L, step * i));
        }
    }

    void set_next_group(BucketGroup *_next_group) { next_group = _next_group; }

    ULL get_prev_interval() { return prev_group != NULL ? prev_group->L : 0; }

    void flush_bucket(int bucket_num);
    void insert_buffer_entries(list<offset_t> &buf);
    void update_it_map(int bucket_num);
    void update_it_map(int bucket_num, list<offset_t>::iterator start_it);
    void delete_it_mapping(offset_t block);

    void tick() { for(auto &b : buckets) b.tick(); }

    BucketMapper insert_block(offset_t block, index_t lease);
};


class RIHistogram {
private:
    ULL data_block_count;
    
    ULL N;
    ULL M;
    ULL R=0;
    vector<map<index_t, ULL>> histogram; // <R, count>

public:
    RIHistogram(ULL _data_block_count, vector<offset_t> &trace)
     : M(data_block_count), data_block_count(_data_block_count), N(trace.size()) {

        histogram = vector<map<index_t, ULL>>(_data_block_count+1, map<index_t, ULL>());

        unordered_map<offset_t, index_t> last_access_time;

        ULL cursor = 0;
        for(auto o : trace) {
            auto it = last_access_time.find(o);
            if(it != last_access_time.end()) {
                ULL itv = cursor - last_access_time[o];

                if(itv > R) R = itv;

                auto h_it = histogram[o].find(itv);
                if(h_it != histogram[o].end()) {
                    h_it->second++;
                } else {
                    histogram[o].insert(make_pair(itv, 1));
                }

                last_access_time[o] = cursor;
                
            } else {
                last_access_time.insert(make_pair(o, cursor));
            }

            cursor++;
        }

        cout << "Histogram init. completed" << endl;

    }

    ULL hits(offset_t block, index_t reuse_interval);
    ULL cost(offset_t block, index_t lease);

    double get_ppuc(offset_t block, index_t old_lease, index_t new_lease);
    PPUCBlock get_best_ppuc_of_block(offset_t block, index_t old_lease);

    ULL get_data_block_count() { return data_block_count; }
    ULL get_max_reuse_interval() { return R; }
    ULL get_trace_length() { return N; }

    
    map<index_t, ULL> &get_histogram_of_block(offset_t block) { return histogram[block]; }

};

class RIHeapCollection {
private:
    class RIHeapElement {
    public:
        offset_t block;
        index_t reuse_interval;
        double ppuc;

        bool operator<(const RIHeapElement e) const {
            return this->ppuc < e.ppuc;
        }

    };

    class RIHeapElementComparator {
    public:
        bool operator() (const RIHeapElement &left, const RIHeapElement &right) {
            return left.ppuc > right.ppuc;
        }
    };

    class RIHeapNode {
        offset_t block;
        priority_queue<RIHeapElement> heap;
        RIHistogram &histogram;

    public:
        RIHeapNode(offset_t _block, RIHistogram &_histogram) 
        : block(_block), histogram(_histogram) {
            auto &h = histogram.get_histogram_of_block(block);
            for(auto itv : h) {
                heap.push({block, itv.first, histogram.get_ppuc(block, 0, itv.first)});
            }
        }

        bool operator<(const RIHeapNode &n) const {
            if(this->get_head().ppuc != n.get_head().ppuc)
                return this->get_head().ppuc > n.get_head().ppuc;
            else
                return this->block < n.block;
        }


        RIHeapElement get_head() const {
            if(heap.empty()) return {-1, -1, -1};
            else {
                return heap.top();
            }
        }

        void pop_head() {
            heap.pop();
        }

        void update(index_t lease) {
            heap = priority_queue<RIHeapElement>();
            auto &h = histogram.get_histogram_of_block(block);
            for(auto it = h.begin(); it != h.end(); it++) {
                if(it->first > lease) {
                    double ppuc = histogram.get_ppuc(block, lease, it->first);
                    heap.push({block, it->first, ppuc});
                }
            }
        }
    };

    typedef set<RIHeapNode> RIHeap;


    RIHistogram &histogram;
    RIHeap heap;

public:
    RIHeapCollection(RIHistogram &_histogram)
    : histogram(_histogram), heap() {
        for(int i=1; i<=histogram.get_data_block_count(); i++) {
            heap.insert(RIHeapNode(i, histogram));
        }
    }

    RIHeapCollection(RIHeapCollection &org) 
    : heap(org.heap), histogram(org.histogram) { }

    void update_ppuc(offset_t block, index_t lease);
    void update_ppuc(RIHeapNode &node);
    PPUCBlock get_max_ppuc();

    void pop_max_ppuc();
    
};




class LeaseCache {
private:
    ULL cache_size; // C
    ULL total_cost = 0;
    ULL target_cost;

    double a;

    RIHistogram &histogram; // RI
    RIHeapCollection &ri_heap;
    
    vector<index_t> assigned_leases;

    vector<bool> is_accessed;

    vector<BucketGroup *> buckets;
    
    unordered_map<offset_t, BucketMapper> it_map;


    ULL hit = 0;
    ULL miss = 0;
    ULL cold_miss = 0;

public:
    LeaseCache(ULL _cache_size, double alpha, RIHistogram &_histogram, RIHeapCollection &_ri_heap) 
        : cache_size(_cache_size), histogram(_histogram), ri_heap(_ri_heap), 
        a(alpha), it_map() {
        
        assigned_leases = vector<index_t>(histogram.get_data_block_count()+1, 0);
        is_accessed = vector<bool>(histogram.get_data_block_count()+1, false);
        target_cost = histogram.get_trace_length() * cache_size;
    }

    void process_ppuc();
    PPUCBlock get_max_ppuc();
    index_t get_group_num(index_t lease);

    void update_cache_size(ULL size);
    void reset_counters() { hit = 0; miss = 0; cold_miss = 0; }
    void reset_buckets();

    int access(offset_t block);

    void print_statistics();

    void debug_cache_logical_stat();
};

// ***** Member functions of Bucket *****
Bucket::Bucket(BucketGroup *_group, int _my_idx,  ULL _l, ULL _init_counter)
: group(_group), my_idx(_my_idx), L(_l), counter(_init_counter) {
    dumping_interval = L - group->get_prev_interval();
}


void Bucket::tick() {
    this->counter++;
    if(counter == dumping_interval) {
        group->flush_bucket(my_idx);
        this->counter = 0;
    }
}

void Bucket::flush() {

}

list<offset_t>::iterator Bucket::insert_block(offset_t block) {
    auto it = buffer.insert(buffer.end(), block);
    return it;
}

void Bucket::delete_block(list<offset_t>::iterator it) {
    group->delete_it_mapping(*it);
    buffer.erase(it);
}



// ***** Member functions of BucketGroup *****
void BucketGroup::flush_bucket(int bucket_num) {
    auto &buf = buckets[bucket_num].get_buffer();
    if(prev_group != NULL) {
        if(buf.size() > 0) prev_group->insert_buffer_entries(buf);
        insert_idx = bucket_num;
    } 
    else {
        for(auto e : buf) it_map.erase(e);
    }
    buf.clear();
}

void BucketGroup::insert_buffer_entries(list<offset_t> &buf) {
    auto &ins_buf = buckets[insert_idx].get_buffer();
    ULL before_size = ins_buf.size();

    for(auto o : buf) {
        it_map[o].bucket = &buckets[insert_idx];
    }
    ins_buf.splice(ins_buf.end(), buf);

}

BucketMapper BucketGroup::insert_block(offset_t block, index_t lease) {
    int bucket_idx;

    if(lease == 0) {
        return {&buckets[0], buckets[0].insert_block(block)};
    }
    if(lease <= (L/2)) {
        bucket_idx = (insert_idx+1) % buckets.size();
        it_map[block] = {&buckets[bucket_idx], buckets[bucket_idx].insert_block(block)};
        return it_map[block];
    }

    ULL remaining = lease - (L/2);
    ULL step_count = (ULL)ceil((double)remaining/(double)step);
    bucket_idx = (insert_idx + step_count) % buckets.size();

    // If selected bucket's flush interval is smaller than lease time
    if((L/2)-buckets[bucket_idx].get_counter() < remaining) { 
        if(bucket_idx == insert_idx) // if this is max flush interval bucket
            return next_group->insert_block(block, lease);
        
        bucket_idx = (bucket_idx + 1) % buckets.size();
    }

    auto it = buckets[bucket_idx].insert_block(block);

    it_map[block] = {&buckets[bucket_idx], it};

    return it_map[block];
}

void BucketGroup::delete_it_mapping(offset_t block) {
    auto it = it_map.find(block);
    if(it != it_map.end()) it_map.erase(it);
}



// ***** Member functions of RIHistogram *****

ULL RIHistogram::hits(offset_t block, index_t reuse_interval) {
    if(reuse_interval == 0) return 0;
    
    ULL ret = 0;
    for(auto i : histogram[block]) {
        if(i.first <= reuse_interval) ret += i.second;
    }
    return ret;
}

ULL RIHistogram::cost(offset_t block, index_t lease) {
    if(lease ==0) return 0; 
    
    ULL current=0, after=0;

    for(auto i : histogram[block]) {
        if(i.first <= lease) {
            current += (i.first * i.second);
        } else {
            after += (lease * i.second);
        }
    }

    return current + after;
}

double RIHistogram::get_ppuc(offset_t block, index_t old_lease, index_t new_lease) {
    block = block;

    ULL hit_gap, cost_gap;

    hit_gap = hits(block, new_lease) - hits(block, old_lease);
    cost_gap = cost(block, new_lease) - cost(block, old_lease);

    return (double)hit_gap / (double)cost_gap;
}


PPUCBlock RIHistogram::get_best_ppuc_of_block(offset_t block, index_t old_lease) {
    PPUCBlock best_block = {true, 0, 0, 0.0};
    for(auto itv : histogram[block]) {
        if(itv.first > old_lease) { // if interval is longer than old lease
            double tppuc = get_ppuc(block, old_lease, itv.first);
            if(tppuc > best_block.ppuc) {
                best_block = {false, block, itv.first, tppuc};
            }
        }
    }

    return best_block;
}



// ***** Member functions of RIHeapCollection *****

void RIHeapCollection::update_ppuc(offset_t block, index_t lease) {
    set<RIHeapNode>::iterator target;
    for(auto it=heap.begin(); it!=heap.end(); it++) {
        if(it->get_head().block == block) {
            target = it;
            break;
        }
    }
    auto n = *target;
    heap.erase(target);

    n.update(lease);
    heap.insert(n);
}

void RIHeapCollection::update_ppuc(RIHeapNode &node) {
    index_t lease = node.get_head().reuse_interval;

    node.update(lease);
    heap.insert(node);
}


PPUCBlock RIHeapCollection::get_max_ppuc() {
    RIHeapElement max_ppuc = heap.begin()->get_head();
    if(max_ppuc.ppuc < 0) 
        return {true, 0, 0, 0};
    else 
        return {false, max_ppuc.block, max_ppuc.reuse_interval, max_ppuc.ppuc};
}

void RIHeapCollection::pop_max_ppuc() {
    auto cur_head = *(heap.begin());
    heap.erase(heap.begin());

    update_ppuc(cur_head);
}


// ***** Member functions of LeaseCache *****

void LeaseCache::process_ppuc() {
    while(total_cost <= target_cost) {
        PPUCBlock r = get_max_ppuc();
        if(r.finished) break;
    }

    cout << "PPUC allocation completed." << endl;
}

PPUCBlock LeaseCache::get_max_ppuc() {
    PPUCBlock best_block = {true, 0, 0, 0.0};

    best_block = ri_heap.get_max_ppuc();

    index_t old_lease = assigned_leases[best_block.block];

    ULL cost = histogram.cost(best_block.block, best_block.new_lease) 
                - histogram.cost(best_block.block, old_lease);
    total_cost += cost;
    if(total_cost > target_cost) {
        best_block.finished = true;
        return best_block;
    }

    ri_heap.pop_max_ppuc();
    assigned_leases[best_block.block] = best_block.new_lease + 1;
    
    return best_block;
}

void LeaseCache::update_cache_size(ULL size) {
    cache_size = size;
    target_cost = histogram.get_trace_length() * cache_size;
}

void LeaseCache::reset_buckets() {
    it_map.clear();
    is_accessed = vector<bool>(histogram.get_data_block_count()+1, false);

    auto R = histogram.get_max_reuse_interval() + 1;
    index_t bri_max = ceil(log2((double)R)) + 1;

    for(auto p : buckets) delete p;
    buckets.clear();
    for(ULL i=0; i<=bri_max; i++) {
        buckets.push_back(new BucketGroup((ULL)pow(2,i), a, it_map, 
            i != 0 ? buckets.at(i-1) : NULL));
    }
    for(ULL i=0; i<bri_max; i++)
        buckets[i]->set_next_group(buckets[i+1]);
}

int LeaseCache::access(offset_t block) {
#ifdef ACCESS_PERF_LOG
    auto start = chrono::high_resolution_clock::now();
#endif

    auto &target_bucket = buckets[get_group_num(assigned_leases[block])];
    auto it = it_map.find(block);
    int ret;

    if(it != it_map.end()) {
        ret = 1;
        hit++;

        auto map_entry = it->second;
        map_entry.bucket->delete_block(map_entry.it);
        target_bucket->insert_block(block, assigned_leases[block]);
    } else {
        ret = 0;
        it_map.insert(make_pair(block, 
            target_bucket->insert_block(block, assigned_leases[block])));
        if(is_accessed[block]) {
            miss++;
        }
        else {
            is_accessed[block] = true;
            cold_miss++;
        }
    }

#ifdef ACCESS_PERF_LOG
    auto elapsed = chrono::high_resolution_clock::now() - start;
    total_alloc_time += chrono::duration_cast<chrono::microseconds>(elapsed).count();
    start = chrono::high_resolution_clock::now();
#endif

    for(auto &b : buckets) b->tick();

#ifdef ACCESS_PERF_LOG
    elapsed = chrono::high_resolution_clock::now() - start;
    total_tick_time += chrono::duration_cast<chrono::microseconds>(elapsed).count();
#endif

    return ret;
}

index_t LeaseCache::get_group_num(index_t lease) {
    index_t ret = (index_t)(ceil(log2((double)lease)));
    return !(lease==0) && !(lease & (lease-1)) ? ret+1 : ret;
}

void LeaseCache::print_statistics() {
    cout << "[Cache statistics]"<< endl;
    cout << "  Total cache access: " << hit + miss + cold_miss << endl;
    cout << "  Cache hit: " << hit << endl;
    cout << "  Cache miss(w/o cold miss): " << miss << endl;
    cout << "  Cache hit ratio: " << (double)hit / (double)(hit + miss) << endl << endl;
}

void LeaseCache::debug_cache_logical_stat() {
    ULL total_hit = 0;
    for(offset_t i=1; i<=histogram.get_data_block_count(); i++) {
        auto &h = histogram.get_histogram_of_block(i);
        for(auto c : h) {
            if(c.first <= assigned_leases[i]) {
                total_hit += c.second;
            }
        }
    }

    cout << "  (Logical) Cache hit: " << total_hit << endl;

    ULL l_total_cost = 0;
    for(offset_t i=1; i<=histogram.get_data_block_count(); i++) {
        auto &h = histogram.get_histogram_of_block(i);
        ULL max_itv = 0;
        for(auto c: h) {
            if(c.first > max_itv) {
                max_itv = c.first;
            }
        }
        l_total_cost += histogram.cost(i, max_itv);
    }
    cout << "  Maximum cost: " << l_total_cost << endl;
}



// ***** Driver function *****
int main(int argc, char** argv)
{
    if(argc < 6){
        cout << "[Usage]: " << argv[0] << " [log file] [cache min size] [cache max size] [block size] [step count]" << endl;
        return -1;
    }

    vector<offset_t> pg;
    string line;
    ifstream log_file(argv[1]);
    ULL CACHE_MIN_SIZE = stoull(argv[2]); // C (target cache size)
    ULL CACHE_MAX_SIZE = stoull(argv[3]);
    BLOCK_SIZE = stoi(argv[4]);
    int step_count = atoi(argv[5]);

    ULL count = 0;
    while(getline(log_file, line)) {
        long long num = stoll(line) / BLOCK_SIZE;
        pg.push_back(num);
        count++;
    }

    cout << "Total access count: " << count << endl; // N

    ULL max_num = 0;
    for(ULL i = 0; i<count; i++)
        if(max_num < pg[i]) {
            max_num = pg[i];
        }
    cout << "Offset max num: " << max_num * BLOCK_SIZE << endl; // max_num/BLOCK_SIZE = M

    
    ULL total_datablock_count = max_num;


    auto start = chrono::high_resolution_clock::now();

    RIHistogram hist(total_datablock_count, pg);
    RIHeapCollection heap(hist);
    LeaseCache cache(0, ALPHA, hist, heap);    

    auto elapsed = chrono::high_resolution_clock::now() - start;
    cout << "Execution time for initializing histogram: " << chrono::duration_cast<chrono::milliseconds>(elapsed).count() << " ms" <<endl;

    for(int i=1; i<=step_count; i++) {
        ULL CACHE_SIZE = CACHE_MIN_SIZE + 
            ceil((double)((CACHE_MAX_SIZE-CACHE_MIN_SIZE)/BLOCK_SIZE)/(double)(step_count-1)*(i-1)) 
            * BLOCK_SIZE;

        cout << "===== Cache size: " << CACHE_SIZE << " =====" << endl;

        cache.update_cache_size(CACHE_SIZE/BLOCK_SIZE);
        cache.reset_counters();
        cache.reset_buckets();

        start = chrono::high_resolution_clock::now();

        cache.process_ppuc();

        elapsed = chrono::high_resolution_clock::now() - start;
        cout << "Execution time for assigning leases: " << chrono::duration_cast<chrono::milliseconds>(elapsed).count() << " ms" <<endl;

        start = chrono::high_resolution_clock::now();
        for(auto i : pg)
            cache.access(i);
        elapsed = chrono::high_resolution_clock::now() - start;

        cache.print_statistics();
        cout << "Execution time for simulation: " << chrono::duration_cast<chrono::milliseconds>(elapsed).count() << " ms" <<endl;
        cout << endl;

        total_execution_time += chrono::duration_cast<chrono::milliseconds>(elapsed).count();
    }


#ifdef ACCESS_PERF_LOG
    cout << "[Runtime analysis of simulation]" << endl;
    cout << "  Total time for allocate page: " << total_alloc_time/1000 << " ms" << endl;
    cout << "  Total time for tick: " << total_tick_time/1000 << " ms" << endl;
    cout << "  Total execution time: " << total_execution_time << " ms" << endl << endl;
#endif

    return 0;
}
