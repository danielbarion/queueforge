# Scaling

Broker on CPUs 0..N-1; client on 8 fixed CPUs. Median of repeats with its range.
Efficiency is X(N) / (N X(1)). `client-bound`: the client used over 85% of its CPUs; `unstable`: the range is wider than 15% of the median; `broker-idle`: the broker used under 85% of its CPUs, so latency, not capacity, set the rate. All three are excluded from the fit.
USL: X(N) = X(1) N / (1 + s(N-1) + k N(N-1)). Peak and projections are the model's extrapolation.

## fanout-20, bun

| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 393,214 | 386,692–399,735 | 2 | 393,214 | 1.00 | 98 | 160 | 6.86 |  |
| 2 | 457,310 | 456,949–457,670 | 2 | 228,655 | 0.58 | 154 | 206 | 5.64 | broker-idle |
| 3 | 445,391 | 409,570–481,211 | 2 | 148,464 | 0.38 | 176 | 204 | 4.98 | broker-idle, unstable |
| 4 | 449,918 | 341,319–558,517 | 2 | 112,480 | 0.29 | 213 | 216 | 4.75 | broker-idle, unstable |
| 6 | 471,362 | 471,129–471,595 | 2 | 78,560 | 0.20 | 222 | 174 | 4.82 | broker-idle |
| 8 | 277,346 | 217,617–337,074 | 2 | 34,668 | 0.09 | 134 | 80 | 5.51 | broker-idle, unstable |

USL fit: not enough clean sizes (needs 1 CPU and two more).

## fanout-20, rust

| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 158,257 | 157,936–158,579 | 2 | 158,257 | 1.00 | 96 | 116 | 2.74 |  |
| 2 | 141,089 | 120,602–161,576 | 2 | 70,545 | 0.45 | 92 | 99 | 6.06 | broker-idle, unstable |
| 3 | 167,662 | 123,824–211,500 | 2 | 55,887 | 0.35 | 138 | 136 | 5.79 | broker-idle, unstable |
| 4 | 205,023 | 178,134–231,912 | 2 | 51,256 | 0.32 | 182 | 146 | 3.48 | broker-idle, unstable |
| 6 | 166,444 | 131,311–201,577 | 2 | 27,741 | 0.18 | 203 | 118 | 3.14 | broker-idle, unstable |
| 8 | 171,482 | 144,193–198,770 | 2 | 21,435 | 0.14 | 248 | 123 | 2.71 | broker-idle, unstable |

USL fit: not enough clean sizes (needs 1 CPU and two more).

## heavy, bun

| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 125,833 | 123,596–128,069 | 2 | 125,833 | 1.00 | 86 | 126 | 140.95 |  |
| 2 | 213,063 | 200,702–225,424 | 2 | 106,531 | 0.85 | 146 | 205 | 95.44 | broker-idle |
| 3 | 281,693 | 231,865–331,521 | 2 | 93,898 | 0.75 | 206 | 268 | 78.75 | broker-idle, unstable |
| 4 | 416,765 | 355,548–477,982 | 2 | 104,191 | 0.83 | 324 | 390 | 53.74 | broker-idle, unstable |
| 6 | 467,596 | 419,991–515,201 | 2 | 77,933 | 0.62 | 427 | 426 | 79.09 | broker-idle, unstable |
| 8 | 347,994 | 319,033–376,955 | 2 | 43,499 | 0.35 | 380 | 312 | 91.09 | broker-idle, unstable |

USL fit: not enough clean sizes (needs 1 CPU and two more).

## heavy, rust

| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 101,586 | 100,619–102,553 | 2 | 101,586 | 1.00 | 96 | 103 | 203.57 |  |
| 2 | 135,873 | 124,458–147,288 | 2 | 67,936 | 0.67 | 138 | 130 | 116.90 | broker-idle, unstable |
| 3 | 243,235 | 237,566–248,904 | 2 | 81,078 | 0.80 | 272 | 236 | 82.67 |  |
| 4 | 234,680 | 214,286–255,075 | 2 | 58,670 | 0.58 | 290 | 226 | 73.17 | broker-idle, unstable |
| 6 | 249,089 | 219,541–278,637 | 2 | 41,515 | 0.41 | 351 | 238 | 63.49 | broker-idle, unstable |
| 8 | 281,594 | 266,298–296,890 | 2 | 35,199 | 0.35 | 466 | 269 | 66.86 | broker-idle |

USL fit: not enough clean sizes (needs 1 CPU and two more).

## quorum, bun

| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 118,201 | 110,029–126,373 | 2 | 118,201 | 1.00 | 86 | 100 | 11.82 |  |
| 2 | 137,049 | 123,944–150,154 | 2 | 68,525 | 0.58 | 106 | 116 | 15.09 | broker-idle, unstable |
| 3 | 101,398 | 100,909–101,887 | 2 | 33,799 | 0.29 | 84 | 90 | 17.77 | broker-idle |
| 4 | 114,629 | 106,321–122,937 | 2 | 28,657 | 0.24 | 99 | 101 | 16.56 | broker-idle |
| 6 | 109,034 | 93,867–124,201 | 2 | 18,172 | 0.15 | 102 | 94 | 16.88 | broker-idle, unstable |
| 8 | 109,714 | 91,617–127,811 | 2 | 13,714 | 0.12 | 118 | 100 | 16.36 | broker-idle, unstable |

USL fit: not enough clean sizes (needs 1 CPU and two more).

## quorum, rust

| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 132,525 | 116,275–148,775 | 2 | 132,525 | 1.00 | 84 | 113 | 2.84 | broker-idle, unstable |
| 2 | 171,002 | 146,948–195,055 | 2 | 85,501 | 0.65 | 148 | 165 | 2.08 | broker-idle, unstable |
| 3 | 203,733 | 173,551–233,915 | 2 | 67,911 | 0.51 | 228 | 218 | 1.96 | broker-idle, unstable |
| 4 | 148,684 | 148,562–148,806 | 2 | 37,171 | 0.28 | 184 | 156 | 1.85 | broker-idle |
| 6 | 157,761 | 152,357–163,165 | 2 | 26,294 | 0.20 | 307 | 219 | 1.96 | broker-idle |
| 8 | 185,659 | 155,301–216,017 | 2 | 23,207 | 0.18 | 388 | 268 | 1.96 | broker-idle, unstable |

USL fit: not enough clean sizes (needs 1 CPU and two more).

## routing-64, bun

| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 145,730 | 143,805–147,656 | 2 | 145,730 | 1.00 | 82 | 139 | 11.25 | broker-idle |
| 2 | 167,810 | 154,916–180,704 | 2 | 83,905 | 0.58 | 96 | 140 | 7.94 | broker-idle, unstable |
| 3 | 187,849 | 173,885–201,813 | 2 | 62,616 | 0.43 | 126 | 166 | 6.83 | broker-idle |
| 4 | 256,518 | 238,420–274,615 | 2 | 64,129 | 0.44 | 188 | 228 | 7.22 | broker-idle |
| 6 | 246,834 | 236,529–257,138 | 2 | 41,139 | 0.28 | 216 | 212 | 8.37 | broker-idle |
| 8 | 345,237 | 224,566–465,908 | 2 | 43,155 | 0.30 | 422 | 340 | 9.06 | broker-idle, unstable |

USL fit: not enough clean sizes (needs 1 CPU and two more).

## routing-64, rust

| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 89,224 | 81,565–96,884 | 2 | 89,224 | 1.00 | 87 | 88 | 11.29 | unstable |
| 2 | 108,285 | 84,175–132,395 | 2 | 54,143 | 0.61 | 96 | 95 | 7.63 | broker-idle, unstable |
| 3 | 103,304 | 103,003–103,605 | 2 | 34,435 | 0.39 | 102 | 91 | 4.77 | broker-idle |
| 4 | 117,731 | 99,790–135,672 | 2 | 29,433 | 0.33 | 134 | 111 | 4.26 | broker-idle, unstable |
| 6 | 119,090 | 99,221–138,960 | 2 | 19,848 | 0.22 | 144 | 100 | 3.73 | broker-idle, unstable |
| 8 | 122,209 | 111,479–132,938 | 2 | 15,276 | 0.17 | 199 | 118 | 3.30 | broker-idle, unstable |

USL fit: not enough clean sizes (needs 1 CPU and two more).

## work-queue, bun

| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 147,759 | 137,554–157,963 | 2 | 147,759 | 1.00 | 91 | 140 | 4.33 |  |
| 2 | 154,110 | 120,519–187,702 | 2 | 77,055 | 0.52 | 92 | 134 | 3.60 | broker-idle, unstable |
| 3 | 139,995 | 123,666–156,325 | 2 | 46,665 | 0.32 | 82 | 132 | 3.42 | broker-idle, unstable |
| 4 | 166,061 | 157,274–174,847 | 2 | 41,515 | 0.28 | 116 | 160 | 3.42 | broker-idle |
| 6 | 151,161 | 147,981–154,342 | 2 | 25,194 | 0.17 | 112 | 144 | 3.74 | broker-idle |
| 8 | 134,894 | 110,880–158,908 | 2 | 16,862 | 0.11 | 110 | 128 | 3.93 | broker-idle, unstable |

USL fit: not enough clean sizes (needs 1 CPU and two more).

## work-queue, rust

| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 122,152 | 121,190–123,114 | 2 | 122,152 | 1.00 | 80 | 108 | 2.79 | broker-idle |
| 2 | 189,217 | 169,597–208,836 | 2 | 94,608 | 0.77 | 166 | 184 | 2.05 | broker-idle, unstable |
| 3 | 158,787 | 154,687–162,887 | 2 | 52,929 | 0.43 | 153 | 164 | 1.93 | broker-idle |
| 4 | 134,410 | 105,452–163,369 | 2 | 33,603 | 0.28 | 192 | 158 | 1.86 | broker-idle, unstable |
| 6 | 167,519 | 142,993–192,045 | 2 | 27,920 | 0.23 | 323 | 232 | 1.92 | broker-idle, unstable |
| 8 | 152,741 | 139,584–165,898 | 2 | 19,093 | 0.16 | 328 | 226 | 1.96 | broker-idle, unstable |

USL fit: not enough clean sizes (needs 1 CPU and two more).

