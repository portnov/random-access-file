# random-access-file README

This package is aimed to provide some number of different implementations of
random file access methods with the same interface.

It can be of use for implementing multithread read-write random access for
large files, for example for DB engines.

The following implementations are provided:

* Simple: trivial wrapper around standard System.IO calls. Created mostly for
  demonstrative purposes. Note: this implementation is not thread-safe at all,
  because it uses one Handle for one file, and that Handle stores current
  position in the file; if different threads would move the pointer forward and
  backward at the same time, you will get garbage.
* Threaded: file access using Posix pread(3), pwrite(3) calls. This
  implementation is thread-safe. It is using block-level locks; each block can
  be accessed for write by single thread, or for read by many threads. Size of
  blocks being locked is adjustable.
* MMaped: file access using mmap(2) call. This implementation is thread-safe.
  It is using block-level locks also.
* Cached: File access using application-level page cache, which can be used
  over Threaded or MMaped file access. Size of cache pages and capacity of the
  cache are adjustable.

## Benchmark results

```
Benchmark random-access-file-benchmark: RUNNING...
benchmarking simple                     
time                 626.6 μs   (606.9 μs .. 647.1 μs)
                     0.989 R²   (0.983 R² .. 0.995 R²)
mean                 657.5 μs   (633.9 μs .. 721.2 μs)
std dev              123.9 μs   (49.51 μs .. 231.3 μs)
variance introduced by outliers: 92% (severely inflated)
                                        
benchmarking threaded                   
time                 413.0 μs   (410.8 μs .. 415.2 μs)
                     0.999 R²   (0.997 R² .. 1.000 R²)
mean                 421.8 μs   (412.8 μs .. 457.2 μs)
std dev              58.30 μs   (3.003 μs .. 124.0 μs)
variance introduced by outliers: 87% (severely inflated)
                                        
benchmarking mmaped                     
time                 282.8 μs   (275.1 μs .. 292.4 μs)
                     0.987 R²   (0.978 R² .. 0.994 R²)
mean                 301.6 μs   (288.8 μs .. 330.9 μs)
std dev              63.22 μs   (30.75 μs .. 114.4 μs)
variance introduced by outliers: 94% (severely inflated)
                                        
benchmarking cached/threaded            
time                 2.763 ms   (2.551 ms .. 2.901 ms)
                     0.971 R²   (0.953 R² .. 0.984 R²)
mean                 2.238 ms   (2.121 ms .. 2.382 ms)
std dev              398.3 μs   (346.2 μs .. 462.2 μs)
variance introduced by outliers: 88% (severely inflated)
                                        
benchmarking cached/mmaped              
time                 1.681 ms   (1.358 ms .. 2.103 ms)
                     0.827 R²   (0.753 R² .. 0.993 R²)
mean                 1.443 ms   (1.382 ms .. 1.664 ms)
std dev              311.7 μs   (119.4 μs .. 657.3 μs)
variance introduced by outliers: 93% (severely inflated)
                                        
Benchmark random-access-file-benchmark: FINISH
```

