# Scaling

Broker on CPUs 0..N-1; client on 8 fixed CPUs. Median of repeats with its range.
Efficiency is X(N) / (N X(1)). `client-bound`: the client used over 85% of its CPUs; `unstable`: the range is wider than 15% of the median; `broker-idle`: the broker used under 85% of its CPUs, so latency, not capacity, set the rate. All three are excluded from the fit.
USL: X(N) = X(1) N / (1 + s(N-1) + k N(N-1)). Peak and projections are the model's extrapolation.

## weak, rust

| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 234,339 | 229,359–241,324 | 3 | 234,339 | 1.00 | 99 | 13 | 12.80 |  |
| 2 | 213,355 | 154,669–312,546 | 3 | 106,677 | 0.46 | 197 | 12 | 18.00 | unstable |
| 3 | 390,530 | 389,233–398,650 | 3 | 130,177 | 0.56 | 296 | 24 | 26.50 |  |
| 4 | 548,547 | 548,547–548,547 | 1 | 137,137 | 0.59 | 391 | 33 | 56.40 | 2 failed |
| 5 | 639,832 | 595,837–683,827 | 2 | 127,966 | 0.55 | 488 | 42 | 26.85 | 1 failed |
| 6 | 684,650 | 518,312–845,425 | 3 | 114,108 | 0.49 | 576 | 47 | 49.20 | unstable |

USL fit over [1, 3, 4, 5]: s = 0.2432, k = 0.00000, worst point off by 21.1%; peak at never; projected 16: 806,770, 32: 878,284, 64: 919,016, 128: 940,832.

## weak-t, rust

| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 281,903 | 274,582–287,347 | 3 | 281,903 | 1.00 | 100 | 16 | 9.60 |  |
| 2 | 544,770 | 534,719–556,128 | 3 | 272,385 | 0.97 | 199 | 30 | 11.60 |  |
| 3 | 750,877 | 742,811–761,683 | 3 | 250,292 | 0.89 | 298 | 42 | 12.90 |  |
| 4 | 825,888 | 821,331–835,440 | 3 | 206,472 | 0.73 | 398 | 48 | 15.50 |  |
| 5 | 920,592 | 898,793–942,308 | 3 | 184,118 | 0.65 | 498 | 57 | 17.20 |  |
| 6 | 1,010,452 | 987,279–1,014,455 | 3 | 168,409 | 0.60 | 596 | 65 | 19.20 |  |

USL fit over [1, 2, 3, 4, 5, 6]: s = 0.0348, k = 0.01774, worst point off by 4.2%; peak at N = 7.4; projected 16: 780,629, 32: 458,576, 64: 241,519, 128: 122,845.

## Summary

| scenario | broker | X(1) | X(max N) measured | s | k | peak N | X(32) model | X(128) model |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| weak | rust | 234,339 | 684,650 (6) | 0.2432 | 0.00000 | ∞ | 878,284 | 940,832 |
| weak-t | rust | 281,903 | 1,010,452 (6) | 0.0348 | 0.01774 | 7.4 | 458,576 | 122,845 |
