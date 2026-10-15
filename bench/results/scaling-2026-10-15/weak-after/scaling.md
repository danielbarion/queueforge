# Scaling

Broker on CPUs 0..N-1; client on 8 fixed CPUs. Median of repeats with its range.
Efficiency is X(N) / (N X(1)). `client-bound`: the client used over 85% of its CPUs; `unstable`: the range is wider than 15% of the median; `broker-idle`: the broker used under 85% of its CPUs, so latency, not capacity, set the rate. All three are excluded from the fit.
USL: X(N) = X(1) N / (1 + s(N-1) + k N(N-1)). Peak and projections are the model's extrapolation.

## weak, bun

| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 305,282 | 303,154–306,208 | 3 | 305,282 | 1.00 | 99 | 15 | 12.00 |  |
| 2 | 534,949 | 518,765–536,008 | 3 | 267,474 | 0.88 | 197 | 30 | 16.60 |  |
| 3 | 753,541 | 752,320–758,294 | 3 | 251,180 | 0.82 | 295 | 44 | 18.00 |  |
| 4 | 1,017,875 | 1,008,237–1,026,730 | 3 | 254,469 | 0.83 | 389 | 62 | 18.30 |  |
| 5 | 1,228,382 | 1,205,794–1,239,691 | 3 | 245,676 | 0.80 | 486 | 78 | 20.40 |  |
| 6 | 1,412,531 | 1,392,621–1,426,102 | 3 | 235,422 | 0.77 | 582 | 94 | 20.80 |  |

USL fit over [1, 2, 3, 4, 5, 6]: s = 0.0659, k = 0.00000, worst point off by 7.4%; peak at never; projected 16: 2,456,091, 32: 3,209,922, 64: 3,791,822, 128: 4,169,773.

## weak-t, bun

| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 494,416 | 491,670–496,096 | 3 | 494,416 | 1.00 | 99 | 26 | 5.70 |  |
| 2 | 823,374 | 811,416–827,858 | 3 | 411,687 | 0.83 | 197 | 45 | 8.40 |  |
| 3 | 1,120,288 | 1,112,534–1,126,523 | 3 | 373,429 | 0.76 | 297 | 65 | 8.20 |  |
| 4 | 1,436,203 | 1,419,198–1,443,782 | 3 | 359,051 | 0.73 | 396 | 86 | 8.80 |  |
| 5 | 1,761,050 | 1,726,053–1,766,654 | 3 | 352,210 | 0.71 | 494 | 109 | 9.10 |  |
| 6 | 2,031,339 | 2,024,357–2,033,925 | 3 | 338,557 | 0.68 | 594 | 129 | 9.30 |  |

USL fit over [1, 2, 3, 4, 5, 6]: s = 0.1072, k = 0.00000, worst point off by 9.0%; peak at never; projected 16: 3,032,976, 32: 3,659,253, 64: 4,080,547, 128: 4,329,794.

## Summary

| scenario | broker | X(1) | X(max N) measured | s | k | peak N | X(32) model | X(128) model |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| weak | bun | 305,282 | 1,412,531 (6) | 0.0659 | 0.00000 | ∞ | 3,209,922 | 4,169,773 |
| weak-t | bun | 494,416 | 2,031,339 (6) | 0.1072 | 0.00000 | ∞ | 3,659,253 | 4,329,794 |
