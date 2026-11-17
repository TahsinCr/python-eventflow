| Benchmark                             | merged-gil-on-threaded-base-threaded | merged-gil-on-threaded-fifo-threaded |
|---------------------------------------|:------------------------------------:|:------------------------------------:|
| route-cache/256/0/late-hot/none/2p    | 628 ns                               | 556 ns: 1.13x faster                 |
| route-cache/256/0/late-hot/none/8p    | 705 ns                               | 653 ns: 1.08x faster                 |
| route-cache/256/0/uniform/none/2p     | 597 ns                               | 739 ns: 1.24x slower                 |
| route-cache/256/0/uniform/none/4p     | 645 ns                               | 712 ns: 1.10x slower                 |
| route-cache/256/0/uniform/none/8p     | 742 ns                               | 836 ns: 1.13x slower                 |
| route-cache/256/1024/late-hot/none/2p | 43.7 us                              | 38.2 us: 1.14x faster                |
| route-cache/256/1024/late-hot/none/4p | 43.4 us                              | 37.6 us: 1.16x faster                |
| route-cache/256/1024/late-hot/none/8p | 43.5 us                              | 37.7 us: 1.15x faster                |
| route-cache/256/1024/uniform/none/2p  | 41.7 us                              | 44.8 us: 1.07x slower                |
| route-cache/256/1024/uniform/none/4p  | 41.9 us                              | 44.6 us: 1.07x slower                |
| route-cache/256/1024/uniform/none/8p  | 42.1 us                              | 45.4 us: 1.08x slower                |
| Geometric mean                        | (ref)                                | 1.00x faster                         |

Benchmark hidden because not significant (1): route-cache/256/0/late-hot/none/4p
