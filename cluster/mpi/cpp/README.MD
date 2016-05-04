# Implementation using C++11 and MPI


To compile the code a compiler that has implemented C++11 regex is required(eg: gcc-4.9+, clang-3.3+, MSVC 2010+)

Dependencies:
* [rapidjson](https://github.com/miloyip/rapidjson)
* [openmpi](https://www.open-mpi.org/) or [mpich](https://www.mpich.org/)

How to compile
==============

For clang with libc++:
```bash
OMPI_CXX=clang++-3.6 mpicxx.openmpi -o parallel_parser parallel_log_parser.cpp -std=c++11 -stdlib=libc++
```
or
```bash
MPICH_CXX=clang++ mpicxx.mpich2 -o parallel_parser parallel_log_parser.cpp -std=c++11 -stdlib=libc++
```

For gcc:
```bash
mpicxx.openmpi -o parallel_parser parallel_log_parser.cpp -std=c++11
```
or
```bash
mpicxx.mpich2 -o parallel_parser parallel_log_parser.cpp -std=c++11 -stdlib=libc++
```

How to run
==============
```bash
mpirun.openmpi -np <number_of_process> parallel_parser
```
or
```bash
mpirun.mpich -np <number_of_process> ./parallel_parser
```

Be sure to configure the cluster environment before running the application
