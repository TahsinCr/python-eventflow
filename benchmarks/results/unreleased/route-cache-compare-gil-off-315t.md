| Benchmark                             | merged-gil-off-315t-base-all | merged-gil-off-315t-fifo-all |
|---------------------------------------|:----------------------------:|:----------------------------:|
| route-cache/1000/0/late-hot/none/1p   | 672 ns                       | 611 ns: 1.10x faster         |
| route-cache/1000/0/uniform/none/1p    | 676 ns                       | 801 ns: 1.18x slower         |
| route-cache/256/0/uniform/none/8p     | 1.12 us                      | 1.70 us: 1.53x slower        |
| route-cache/256/0/zipf-0.8/none/1p    | 646 ns                       | 766 ns: 1.19x slower         |
| route-cache/256/1024/late-hot/none/1p | 59.2 us                      | 51.7 us: 1.15x faster        |
| route-cache/256/1024/late-hot/none/8p | 22.4 us                      | 11.7 us: 1.92x faster        |
| Geometric mean                        | (ref)                        | 1.02x faster                 |
