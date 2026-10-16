# Scaling

Broker on CPUs 0..N-1; client on 8 fixed CPUs. Median of repeats with its range.
Efficiency is X(N) / (N X(1)). `client-bound`: the client used over 85% of its CPUs; `unstable`: the range is wider than 15% of the median; `broker-idle`: the broker used under 85% of its CPUs, so latency, not capacity, set the rate. All three are excluded from the fit.
USL: X(N) = X(1) N / (1 + s(N-1) + k N(N-1)). Peak and projections are the model's extrapolation.

## weak, php

| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 118,572 | 117,717–119,050 | 3 | 118,572 | 1.00 | 93 | 6 | 24.50 |  |
| 2 | 208,346 | 208,059–211,213 | 3 | 104,173 | 0.88 | 179 | 12 | 30.80 |  |
| 3 | 293,935 | 283,028–299,112 | 3 | 97,978 | 0.83 | 267 | 18 | 36.00 |  |
| 4 | 373,639 | 341,107–378,157 | 3 | 93,410 | 0.79 | 355 | 24 | 41.70 |  |
| 5 | 444,938 | 444,747–446,550 | 3 | 88,988 | 0.75 | 434 | 29 | 43.90 |  |
| 6 | 510,092 | 455,300–513,984 | 3 | 85,015 | 0.72 | 514 | 34 | 42.10 |  |

USL fit over [1, 2, 3, 4, 5, 6]: s = 0.0849, k = 0.00000, worst point off by 4.9%; peak at never; projected 16: 834,404, 32: 1,044,621, 64: 1,195,175, 128: 1,287,989.

## weak-t, php

| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 153,099 | 145,590–154,454 | 3 | 153,099 | 1.00 | 100 | 8 | 14.60 |  |
| 2 | 285,511 | 285,318–285,626 | 3 | 142,756 | 0.93 | 197 | 15 | 17.10 |  |
| 3 | 405,389 | 403,005–405,704 | 3 | 135,130 | 0.88 | 297 | 24 | 18.50 |  |
| 4 | 510,693 | 507,731–514,607 | 3 | 127,673 | 0.83 | 397 | 32 | 19.20 |  |
| 5 | 607,302 | 602,192–608,064 | 3 | 121,460 | 0.79 | 494 | 40 | 26.20 |  |
| 6 | 697,252 | 682,405–704,281 | 3 | 116,209 | 0.76 | 570 | 43 | 23.50 |  |

USL fit over [1, 2, 3, 4, 5, 6]: s = 0.0648, k = 0.00000, worst point off by 0.7%; peak at never; projected 16: 1,242,002, 32: 1,627,958, 64: 1,927,437, 128: 2,122,681.

## Summary

| scenario | broker | X(1) | X(max N) measured | s | k | peak N | X(32) model | X(128) model |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| weak | php | 118,572 | 510,092 (6) | 0.0849 | 0.00000 | ∞ | 1,044,621 | 1,287,989 |
| weak-t | php | 153,099 | 697,252 (6) | 0.0648 | 0.00000 | ∞ | 1,627,958 | 2,122,681 |
