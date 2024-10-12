# ScaleOPT: A Scalable Optimal Page Replacement Policy Simulator 
ScaleOPT is a simulator for calculating the hit ratio under OPT(Belady, or MIN) replacement policy, utilizing multiple threads while providing accurate simulation result.

ScaleOPT conducts OPT simulation utilizing multiple threads with new techniques called AccessMap and Pipeline-Tree.
This helps to calculate hit ratios faster than traditional stack algorithm-based simulators running in a single thread.
For detailed information, please refer our paper which will be appeared in December, 2024.


## Installation

ScaleOPT needs `g++` compiler and tested on `C++14` version.

Before building ScaleOPT binary, you have to modify the value of `CPU_SOCKET` and `CORE_THREAD` in `ScaleOPT/cache_sim.cpp`.
Please set `CPU_SOCKET` as the number of CPUs installed on your system, and `CORE_THREAD` as the number of physical threads of single CPU.

To build the executable, run the command below.
Be careful not to change the structure of the repository locally because the current source code includes headers as the relative path.

```
g++ -O3 -o $BINARY_NAME ScaleOPT/cache_sim.cpp -lpthread
```

This repository contains the source codes of the other simulators we used for comparison in the experiment. 
If you want to simulate OPT with those simulators, execute the above command to build the executable of each simulator.



## Instruction for Running Simulation

### Parameters
To start simulation, you have to provide following parameters:

* `TRACE_FILE`: The path of the trace file to simulate.
* `MIN_CACHE_SIZE`: The minimum cache size to calculate hit ratio.
* `MAX_CACHE_SIZE`: The maximum cache size to calculate hit ratio.
* `STEP_COUNT`: The number of cache sizes to simulate.
* `BLOCK_SIZE`: The size of single page. For example, if you want to calculate the hit ratio on a 4K page cache system for the trace where the referenced offset for a single file is recorded, set `BLOCK_SIZE` to 4096.
* `THREAD_NUM`: The number of thread to use in the simulation.

ScaleOPT simultaneously simulates the cache size you specify.
For example, if `MIN_CACHE_SIZE` and `MAX_CACHE_SIZE` are 100 and 300 respectively and `STEP_COUNT` is 3, ScaleOPT calculates the hit ratio for cache sizes 100, 200 and 300.

### Trace format
ScaleOPT receives a plain text file as a trace, with the address referenced in each line.

### How to run executable
You can execute the simulator by running the command below:
```
taskset -c 0 ./$BINARY_NAME [TRACE_FILE] [MIN_CACHE_SIZE] [MAX_CACHE_SIZE] [STEP_COUNT] [BLOCK_SIZE] [THREAD_NUM]
```




## Full Paper and Referencing Our Work
Our paper will be appear in Proceedings of the ACM on Measurement and Analysis of Computing Systems (POMACS), Vol. 7, No. 3, Article 44 and SIGMETRICS'25.
