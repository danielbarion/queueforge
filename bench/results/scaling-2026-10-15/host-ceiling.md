## Host ceiling, 2026-10-15

`K` independent single-process Bun brokers (`qf-coco:bun-b10`, before B14),
each `--cpus 1` pinned to CPU `0..K-1`, each driven by its own two loadgen
processes with the `weak-t` settings (2 queues, 2 publishers, 2 consumers
each, window 512, prefetch 1024, transient 256 B, BLOCK=1). Same 15-CPU VM and
client CPUs (6-13) as the scaling runs. Two repeats per K; median and range.
Nothing is shared between these brokers but the host, so the loss here is the
host's (VM network path, memory bandwidth, and from 5 or 6 busy CPUs with the
client, the Mac's efficiency cores), not a broker's.

| K brokers | msg/s | range | per broker | efficiency | broker CPU % | us/msg |
| ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 1 | 498,253 | 496,627–499,878 | 498,253 | 1.00 | 99 | 2.00 |
| 2 | 890,005 | 887,637–892,373 | 445,002 | 0.89 | 199 | 2.23 |
| 4 | 1,511,993 | 1,508,541–1,515,445 | 377,998 | 0.76 | 397 | 2.62 |
| 6 | 2,109,003 | 2,108,987–2,109,019 | 351,501 | 0.71 | 595 | 2.82 |

Script: `bench/ceiling.py`:
`python3 bench/ceiling.py qf-coco:bun-b10 bun 1,2,4,6 2`.
