#include <fstream>
#include <iostream>
#include <chrono>
#include <vector>
#include <string>
#include <cmath>

using namespace std;

int main(int argc, char **argv) {
    if(argc < 6) {
        cout << " Usage: " << argv[0] << "[file name] [min cache size] [max_cache_size] [step count] [cache block size]" << endl;
        return -1;
    }
    //unsigned long long cache_line_count = stoull(string(argv[2])) / stoull(string(argv[3]));
    unsigned long long cache_line_count;

    vector<int> cache_sizes;
    string minsz_str(argv[2]);
    string maxsz_str(argv[3]);
    int block_size = atoi(argv[5]);
    unsigned long long min_csize = stoull(minsz_str)/block_size;
    unsigned long long max_csize = stoull(maxsz_str)/block_size;
    int step_count = atoi(argv[4]);

    cache_sizes.push_back((int)min_csize);
    for(int i=1; i<step_count-1; i++) {
        unsigned long long cur_step_sz = (unsigned long long)ceil((double)(max_csize-min_csize)/(double)(step_count-1)*i);
        cache_sizes.push_back((int)(min_csize+cur_step_sz));
    }
    cache_sizes.push_back((int)max_csize);

    string file_name(argv[1]);

    ifstream fi(file_name);
    string line;

    //unsigned long long hit = 0, miss = 0, cold_miss = 0;
    vector<unsigned long long> hit(step_count, 0), miss(step_count, 0);
    unsigned long long cold_miss = 0;
    long long hit_depth;

    vector<long long> v;
    while(getline(fi, line)) {
        hit_depth = stoll(line);
        v.push_back(hit_depth);
    }

    auto start = chrono::high_resolution_clock::now();
    for(auto d : v) {
        if(d < 0) {
            cold_miss++;
            continue;
        } else {
            for(int i=0; i<step_count; i++) {
              cache_line_count = cache_sizes[i];
              if(d <= cache_line_count) hit[i]++;
              else miss[i]++;
            }
        }
    }
    auto elapsed = chrono::high_resolution_clock::now() - start;

    for(int i=0; i<step_count; i++) {
        cout << "[Cache statistics - size: " << cache_sizes[i] * block_size << "]"<< endl;
        cout << "  Total cache access: " << hit[i] + miss[i] + cold_miss << endl;
        cout << "  Cache hit: " << hit[i] << endl;
        cout << "  Cache miss(w/o cold miss): " << miss[i] << endl;
        cout << "  Cache hit ratio: " << (double)hit[i] / (double)(hit[i] + miss[i]) << endl << endl;
    }

    cout << "Elapsed time for calculating hit ratio: " << chrono::duration_cast<chrono::milliseconds>(elapsed).count() << " ms" << endl;

}
