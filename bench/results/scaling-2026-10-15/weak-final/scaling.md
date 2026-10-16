# Scaling

Broker on CPUs 0..N-1; client on 8 fixed CPUs. Median of repeats with its range.
Efficiency is X(N) / (N X(1)). `client-bound`: the client used over 85% of its CPUs; `unstable`: the range is wider than 15% of the median; `broker-idle`: the broker used under 85% of its CPUs, so latency, not capacity, set the rate. All three are excluded from the fit.
USL: X(N) = X(1) N / (1 + s(N-1) + k N(N-1)). Peak and projections are the model's extrapolation.

## weak, bun

| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 364,658 | 364,222–365,693 | 3 | 364,658 | 1.00 | 98 | 18 | 10.50 |  |
| 2 | 639,914 | 618,835–642,058 | 3 | 319,957 | 0.88 | 195 | 35 | 14.50 |  |
| 3 | 896,435 | 885,770–902,672 | 3 | 298,812 | 0.82 | 292 | 51 | 15.70 |  |
| 4 | 1,228,758 | 1,208,330–1,235,293 | 3 | 307,190 | 0.84 | 389 | 71 | 15.80 |  |
| 5 | 1,441,333 | 1,439,365–1,443,810 | 3 | 288,267 | 0.79 | 475 | 88 | 17.40 |  |
| 6 | 1,685,293 | 1,684,475–1,697,218 | 3 | 280,882 | 0.77 | 578 | 108 | 18.40 |  |

USL fit over [1, 2, 3, 4, 5, 6]: s = 0.0671, k = 0.00000, worst point off by 7.6%; peak at never; projected 16: 2,906,841, 32: 3,786,826, 64: 4,462,254, 128: 4,899,168.

## weak-t, bun

| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 655,838 | 655,594–660,845 | 3 | 655,838 | 1.00 | 99 | 34 | 4.60 |  |
| 2 | 1,090,534 | 1,058,734–1,116,653 | 3 | 545,267 | 0.83 | 197 | 57 | 7.10 |  |
| 3 | 1,469,832 | 1,468,261–1,503,050 | 3 | 489,944 | 0.75 | 297 | 82 | 7.00 |  |
| 4 | 1,892,536 | 1,885,331–1,903,282 | 3 | 473,134 | 0.72 | 396 | 107 | 7.30 |  |
| 5 | 2,302,406 | 2,297,203–2,341,917 | 3 | 460,481 | 0.70 | 495 | 138 | 7.40 |  |
| 6 | 2,653,190 | 2,630,398–2,657,882 | 3 | 442,198 | 0.67 | 593 | 161 | 8.30 |  |

USL fit over [1, 2, 3, 4, 5, 6]: s = 0.1118, k = 0.00000, worst point off by 9.4%; peak at never; projected 16: 3,919,005, 32: 4,698,214, 64: 5,216,842, 128: 5,521,602.

## Summary

| scenario | broker | X(1) | X(max N) measured | s | k | peak N | X(32) model | X(128) model |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| weak | bun | 364,658 | 1,685,293 (6) | 0.0671 | 0.00000 | ∞ | 3,786,826 | 4,899,168 |
| weak-t | bun | 655,838 | 2,653,190 (6) | 0.1118 | 0.00000 | ∞ | 4,698,214 | 5,521,602 |
