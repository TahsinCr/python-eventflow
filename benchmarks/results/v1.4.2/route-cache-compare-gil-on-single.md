| Benchmark                              | merged-gil-on-single-base-single | merged-gil-on-single-fifo-single |
|----------------------------------------|:--------------------------------:|:--------------------------------:|
| route-cache/1000/0/late-hot/none/1p    | 486 ns                           | 445 ns: 1.09x faster             |
| route-cache/1000/0/uniform/none/1p     | 493 ns                           | 588 ns: 1.19x slower             |
| route-cache/1000/1024/late-hot/none/1p | 43.1 us                          | 37.3 us: 1.16x faster            |
| route-cache/256/0/hot-shift/none/1p    | 469 ns                           | 430 ns: 1.09x faster             |
| route-cache/256/0/late-hot/none/1p     | 468 ns                           | 425 ns: 1.10x faster             |
| route-cache/256/0/uniform/high/1p      | 484 ns                           | 514 ns: 1.06x slower             |
| route-cache/256/0/uniform/low/1p       | 471 ns                           | 565 ns: 1.20x slower             |
| route-cache/256/0/uniform/none/1p      | 471 ns                           | 570 ns: 1.21x slower             |
| route-cache/256/0/zipf-0.8/none/1p     | 455 ns                           | 530 ns: 1.16x slower             |
| route-cache/256/0/zipf-1.2/low/1p      | 452 ns                           | 477 ns: 1.06x slower             |
| route-cache/256/0/zipf-1.2/none/1p     | 440 ns                           | 479 ns: 1.09x slower             |
| route-cache/256/100/late-hot/none/1p   | 4.63 us                          | 3.97 us: 1.17x faster            |
| route-cache/256/100/uniform/none/1p    | 4.46 us                          | 4.75 us: 1.07x slower            |
| route-cache/256/1024/hot-shift/none/1p | 42.3 us                          | 36.6 us: 1.16x faster            |
| route-cache/256/1024/late-hot/none/1p  | 42.3 us                          | 37.0 us: 1.14x faster            |
| route-cache/256/1024/uniform/none/1p   | 40.8 us                          | 42.9 us: 1.05x slower            |
| route-cache/65/0/late-hot/none/1p      | 467 ns                           | 424 ns: 1.10x faster             |
| route-cache/65/0/uniform/none/1p       | 432 ns                           | 586 ns: 1.36x slower             |
| route-cache/65/100/late-hot/none/1p    | 4.63 us                          | 3.96 us: 1.17x faster            |
| route-cache/65/100/uniform/none/1p     | 3.97 us                          | 4.80 us: 1.21x slower            |
| route-cache/65/1024/late-hot/none/1p   | 42.0 us                          | 36.6 us: 1.15x faster            |
| route-cache/65/1024/uniform/none/1p    | 36.8 us                          | 43.0 us: 1.17x slower            |
| route-cache/80/0/late-hot/none/1p      | 466 ns                           | 425 ns: 1.10x faster             |
| route-cache/80/0/uniform/none/1p       | 438 ns                           | 583 ns: 1.33x slower             |
| route-cache/80/1024/late-hot/none/1p   | 42.2 us                          | 36.9 us: 1.14x faster            |
| route-cache/80/1024/uniform/none/1p    | 37.7 us                          | 42.8 us: 1.14x slower            |
| Geometric mean                         | (ref)                            | 1.02x slower                     |

Benchmark hidden because not significant (16): route-cache/1000/1024/uniform/none/1p, route-cache/16/0/late-hot/none/1p, route-cache/16/0/uniform/none/1p, route-cache/16/1024/late-hot/none/1p, route-cache/16/1024/uniform/none/1p, route-cache/256/0/zipf-1.2/high/1p, route-cache/256/1024/uniform/high/1p, route-cache/256/1024/uniform/low/1p, route-cache/256/1024/zipf-0.8/none/1p, route-cache/256/1024/zipf-1.2/high/1p, route-cache/256/1024/zipf-1.2/low/1p, route-cache/256/1024/zipf-1.2/none/1p, route-cache/64/0/late-hot/none/1p, route-cache/64/0/uniform/none/1p, route-cache/64/1024/late-hot/none/1p, route-cache/64/1024/uniform/none/1p
