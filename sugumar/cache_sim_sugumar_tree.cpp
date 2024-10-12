#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstdlib>
#include <chrono>
#include <queue>
#include <set>
#include <unordered_map>

#define LA_DIST 1000

#define INIT_AM_QUEUE_CAPACITY 16

typedef long long offset_t;
typedef long long index_t;
typedef unsigned long long ULL;

unsigned long long BLOCK_SIZE = 1;

using namespace std;

class NextAccessMap {
private:
  class TNRQueue {
  private:
    index_t *buf = NULL;
    index_t begin=0, end=0;
    ULL capacity;
  public:
    TNRQueue() { 
      capacity = INIT_AM_QUEUE_CAPACITY;
      buf = (index_t *)malloc(sizeof(index_t) * capacity); 
    }
    ~TNRQueue() { if(buf != NULL) free(buf); }

    void push(index_t entry) {
      if(end == capacity) {
          resize(capacity*2);
      }
      buf[end]=entry;
      end++;
    }

    void resize(ULL new_cap) {
      buf = (index_t *)realloc(buf, sizeof(index_t)*new_cap);
      capacity = new_cap;
    }

    index_t front() { 
      return empty() ? INT64_MAX : buf[begin]; 
    }
    index_t at(index_t i) { return buf[i]; }
    void pop() { if(!empty()) begin++; }

    bool empty() { return begin==end; }
  };

  index_t count_distinct_datablock(vector<offset_t> *access_log) {
    unordered_map<offset_t, bool> m;
    m.reserve(1<<22);

    index_t count = 0;
    for(auto i=access_log->begin(); i!=access_log->end(); i++) {
      if(m.find(*i) == m.end()) {
        count++;
        m.insert(make_pair(*i, true));
      }
    }

    return count;
  }

  vector<offset_t> *access_log;
  index_t access_count;
  offset_t max_val;
  unordered_map<offset_t, TNRQueue*> map;

public:
  NextAccessMap(vector<offset_t> *_access_log, offset_t _max_val, int _repeat_num = 1) 
  : access_log(_access_log), access_count(access_log->size()), max_val(_max_val) {
    map.reserve(count_distinct_datablock(access_log));
    init(access_log);

#ifdef DEBUG_LOG
    cout << "AccessMap init. completed." << endl;
#endif
  }

  ~NextAccessMap() {
    for(auto i : map) delete i.second;
  }

  void add_num(index_t idx, index_t acceess_num);
  void pop_idx(index_t idx);
  void pop_until_cursor(index_t idx, index_t cursor);
  index_t get_head_idx(index_t idx);

  index_t cast_to_map_index(index_t idx) { return idx; }

  void init(vector<offset_t> *access_log);
  index_t get_access_count() { return access_count; }

  index_t find_tnr_linear(offset_t target, index_t cur_time) {
    for(index_t cur = cur_time+1; cur < access_log->size(); cur++) {
      if(access_log->at(cur) == target)
        return cur;
    }
    return INT64_MAX;
  }
};

class Entry {
public:
    offset_t offset;
    index_t priority;

    Entry() : offset(-1), priority(INT64_MIN) {}
    Entry(offset_t _offset, index_t _priority) 
        : offset(_offset), priority(_priority) {}
};

class EntryGroup {
private:
    EntryGroup *next_group;
    EntryGroup *prev_group;

    // deque<Entry> entries;
    class EntryComparator {
    public:
        bool operator() (const Entry &left, const Entry &right) {
            if(left.priority != right.priority)
                return left.priority > right.priority;
            else
                return left.offset < right.offset;
        }
    };

    set<Entry, EntryComparator> entries;


public:
    EntryGroup() : next_group(NULL), prev_group(NULL), entries() {}
    EntryGroup(EntryGroup *_prev_group) : 
        next_group(NULL), prev_group(_prev_group), entries() {}


    void create_next_group() { next_group = new EntryGroup(this); }
    void create_prev_group();
    void set_next_group(EntryGroup *g) { next_group = g; }
    void set_prev_group(EntryGroup *g) { prev_group = g; }

    Entry get_head() { return entries.empty() ? Entry(-1, -1) : *(entries.begin()); }
    Entry get_tail();
    int get_group_size() { return entries.size(); }
    EntryGroup *get_next_group() { return next_group; }
    EntryGroup *get_prev_group() { return prev_group; }
    bool is_empty() { return entries.empty(); }

    set<Entry>::iterator get_iter(offset_t offset);
    set<Entry>::iterator get_end_iter() { return entries.end(); }

    void delete_head();
    void delete_tail();

    void put_tail(Entry e) { entries.insert(e); }

    void insert(Entry e) { entries.insert(e); }

    void repair_dummy(offset_t target_offset, Entry de, index_t dp);
};

class ProcessMap {
private:
    bool *status;
public:
    ProcessMap(offset_t max_offset) {
        status = (bool *)calloc(max_offset + 1, sizeof(bool));
    }

    void set_processed(offset_t offset) { status[offset] = true; }
    bool get_processed_status(offset_t offset) { return status[offset]; }
};



class Stack {
private:
    Entry top_entry;
    EntryGroup *next_group;

    ProcessMap &process_status;


public:
    Stack(ProcessMap &_process_status) : process_status(_process_status) {
        top_entry = Entry(-1, 0);
        next_group = new EntryGroup();
    }

    index_t put_top(Entry e);
    index_t find_and_delete(offset_t offset);
    index_t find_and_update(Entry e);
    void add_to_last_group(Entry e);

    void swap(deque<Entry>::iterator e1, deque<Entry>::iterator e2);

    void repair(offset_t offset, index_t cur_time);
    bool is_processed(offset_t offset);

    unsigned long long get_group_count();
};

class Buffer {
private:
    vector<offset_t> &log;
    index_t cursor = 0;

    index_t dummy_counter = -1;

    deque<Entry> buf;
    Stack &stack;

    vector<index_t> depth_seq;

    unordered_map<index_t, index_t> prev_tuple;

    NextAccessMap &am;

public:
    Buffer(vector<offset_t> &_log, Stack &_stack, NextAccessMap &_am) 
        : log(_log), stack(_stack), am(_am) {}

    unsigned long long lookahead(unsigned long long dist);
    unsigned long long process(unsigned long long count);

    void save_output_to_file(string file_name);
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


void NextAccessMap::init(vector<offset_t> *access_log) {

  offset_t cur_offset;
  for(index_t i=0; i<access_log->size(); i++) {
    cur_offset = access_log->at(i);
    auto it = map.find(cur_offset);
    if(it == map.end()) {
      auto ins_ret = map.insert(make_pair(cur_offset, new TNRQueue()));
      it = ins_ret.first;
    }
    add_num(cur_offset, i);
  }
}


/* ***** Member functions of EntryGroup ***** */

Entry EntryGroup::get_tail() {
    if(entries.empty()) {
        cerr << "Critical error: EntryGroup is emtpy! (get_tail)" << endl;
        exit(-1);
    }

    return *(entries.rbegin());
}


void EntryGroup::delete_head() {
    if(!entries.empty()) {
        entries.erase(entries.begin());
    } else {
        cerr << "Critical error: EntryGroup is empty! (delete_head)" << endl;
        exit(-1);
    } 
}

void EntryGroup::delete_tail() {
    if(entries.empty()) {
        cerr << "Critical error: EntryGroup is empty! (delete_tail)" << endl;
        exit(-1);
    }

    entries.erase(prev(entries.end()));
}

void EntryGroup::create_prev_group() {
    if(entries.size() == 0) {
        return;
    }


    // If this is the first group of stack,
    // creating prev. group will break the stack structure.
    // So we have to make group next of this group.
    EntryGroup *original_next = next_group;

    create_next_group();

    // Fix the link information of groups
    if(original_next != NULL) {
        next_group->set_next_group(original_next);
        original_next->set_prev_group(next_group);
    }

    // Swap the new one's buffer and this buffer.
    // By doing this, we don't have to update
    // the first group pointer that managed by the stack.
    entries.swap(next_group->entries);

}

set<Entry>::iterator EntryGroup::get_iter(offset_t offset) {
    auto it = entries.begin();
    for(int i=0; i<entries.size(); i++, it++) {
        if(it->offset == offset)
            break;
    }

    if(it != entries.end()) {
        return it;
    } else if(next_group != NULL){
        it = next_group->get_iter(offset);
        if(it != next_group->entries.end())
            return it;
        else
            return entries.end(); // Error
    }
    else return entries.end();
}

void EntryGroup::repair_dummy(offset_t target_offset, Entry de, index_t dp) {
    for(auto i=entries.begin(); i!=entries.end(); i++) {
        if(i->offset == de.offset) {
            auto tmp = *i;
            entries.erase(i);
            tmp.priority = de.priority;
            entries.insert(tmp);
            return;
        }

        if(i->priority < 0 && i->priority > dp) {
            if(i->priority < de.priority) {
                Entry tmp = *i;
                entries.erase(i);
                entries.insert(de);
                de = tmp;
            }
        }
    }
    next_group->repair_dummy(target_offset, de, dp);
}


/* ***** Member functions of Stack ***** */
index_t Stack::put_top(Entry e) {
    if(top_entry.offset == e.offset) {
        top_entry.priority = e.priority;
        return 1;
    }

    Entry prev_entry = top_entry;
    top_entry = e;
    process_status.set_processed(e.offset);

    if(prev_entry.offset == -1) return -1;
    if(next_group->is_empty()) {
        next_group->put_tail(prev_entry);
        return -1;
    }

    return find_and_update(prev_entry);
}

index_t Stack::find_and_delete(offset_t offset) {
    int depth = 2;

    for(auto cur_group = next_group; cur_group!=NULL; 
        cur_group=cur_group->get_next_group()) {
        if(cur_group->get_head().offset == offset) {
            cur_group->delete_head();
            return depth;
        }
        depth += cur_group->get_group_size();
    }

    return -1; // Cold miss
}

index_t Stack::find_and_update(Entry de) {
    int depth = 2;
    auto cur_group = next_group;
    EntryGroup *prev_group;

    for( ; cur_group!=NULL; cur_group=cur_group->get_next_group()) {
        if(cur_group->get_head().offset == top_entry.offset) {
            cur_group->delete_head();
            if(cur_group->get_prev_group() == NULL) {
                cur_group->create_prev_group(); // Current group is prev group!
                cur_group->insert(de);
            }
            else {
                cur_group->get_prev_group()->insert(de);
                if(cur_group->get_group_size() == 0) {
                    // delete current group
                    cur_group->get_prev_group()->set_next_group(cur_group->get_next_group());
                    if(cur_group->get_next_group() != NULL)
                        cur_group->get_next_group()->set_prev_group(cur_group->get_prev_group());
                    delete cur_group;
                }
            }

            return depth;
        }
        depth += cur_group->get_group_size();

        // If de has the higher priority than the last entry,
        // insert the de to this group and make last entry to de.
        Entry cur_last = cur_group->get_tail();
        if(cur_last.priority < de.priority) {
            cur_group->delete_tail();
            cur_group->insert(de);
            de = cur_last;
        }

        prev_group = cur_group;
    }

    prev_group->insert(de);

    return -1; // Cold miss
}

void Stack::add_to_last_group(Entry e) {
    EntryGroup *cur_group = next_group;
    while(true) {
        if(cur_group->get_next_group() == NULL)
            break;
        cur_group = cur_group->get_next_group();
    }

    cur_group->insert(e);
}

void Stack::swap(deque<Entry>::iterator e1, deque<Entry>::iterator e2) {
    Entry tmp = *e1;
    *e1 = *e2;
    *e2 = tmp;
}


void Stack::repair(offset_t offset, index_t tprty) {
    if(top_entry.offset == offset) {
        top_entry.priority = tprty;
        return;
    }

    offset_t dst;
    auto de = *(next_group->get_iter(offset));

    index_t dp = de.priority; // Dummy priority of target
    de.priority = tprty;

    next_group->repair_dummy(offset, de, dp);
}

bool Stack::is_processed(offset_t offset) {
    return process_status.get_processed_status(offset);
}

unsigned long long Stack::get_group_count() {
    unsigned long long r = 0;
    EntryGroup *next = next_group;
    while(next != NULL) {
        r++;
        next = next->get_next_group();
    }
    return r;
}



/* ***** Member functions of Buffer ***** */

// Read access trace and append buffer.
// Return: read/append count.
unsigned long long Buffer::lookahead(unsigned long long dist) {
    int look_count = 0;
    for(int i=0; i<dist; i++) {
        if(cursor == log.size()) {
            break;
        }

        Entry e(log[cursor], 0);
        index_t priority = INT64_MAX - cursor;
        if(stack.is_processed(e.offset)) {
            stack.repair(e.offset, priority);
        } else {
            if(prev_tuple.find(e.offset) != prev_tuple.end())
                buf[prev_tuple[e.offset]].priority = priority;
        }

        e.priority = dummy_counter--;
        buf.push_back(e);

        prev_tuple[e.offset] = cursor;

        cursor++;
        look_count++;
    }

    return look_count;
}

// Get front 'count' entries and process stack.
// Return: processed entry count.
unsigned long long Buffer::process(unsigned long long count) {
    int i=0;
    for( ; i<count; i++) {
        am.pop_idx(log[i]);
        depth_seq.push_back(stack.put_top(Entry(log[i], INT64_MAX - am.get_head_idx(log[i]))));        
    }

    return i;
}

// Saves hit depth sequence to the file
void Buffer::save_output_to_file(string file_name) {
    ofstream output(file_name);
    for(auto i : depth_seq)
        output << to_string(i) << endl;
    output.close();
}


// ***** Main function ******
int main(int argc, char** argv)
{
    if(argc < 3){
        cout << "[Usage]: " << argv[0] << " [log file] [cache block size]" << endl;
        return -1;
    }

    vector<offset_t> pg;
    string line;
    ifstream log_file(argv[1]);

    BLOCK_SIZE = stoull(argv[2]);

    unsigned long long count = 0;
    while(getline(log_file, line)) {
        offset_t num = stoull(line) / BLOCK_SIZE;
        pg.push_back(num);
        count++;
    }

    cout << "Total access count: " << count << endl;

    offset_t max_num = 0;
    for(index_t i = 0; i<count; i++)
        if(max_num < pg[i]) {
            max_num = pg[i];
        }
    cout << "Offset max value: " << max_num * BLOCK_SIZE << endl;

    auto start = chrono::high_resolution_clock::now();
    NextAccessMap my_access_map(&pg, max_num);
    auto elapsed = chrono::high_resolution_clock::now() - start;
    cout << "Elapsed time for AccessMap Initialization: " << chrono::duration_cast<chrono::milliseconds>(elapsed).count() << " ms" << endl;

    ProcessMap pm(max_num);
    Stack stack(pm);
    Buffer buffer(pg, stack, my_access_map);

    string output_file = "stack_dist.log";

    start = chrono::high_resolution_clock::now();
    buffer.process(count);
    elapsed = chrono::high_resolution_clock::now() - start;

    cout << "Total stack group count: " << stack.get_group_count() << endl;

    cout << "Elapsed time for stack processing: " << chrono::duration_cast<chrono::milliseconds>(elapsed).count() << " ms" << endl;

    return 0;
}
