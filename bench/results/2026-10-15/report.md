### Paced

#### 128 confirms in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 9,310.34 | 32000 | 4.41 | 7,022.51 | 32000 |
| Rust | 20,267.99 | 48000 | 1.86 | 24,910.08 | 96000 |
| Bun | 20,082.45 | 64000 | 0.87 | 23,904.94 | 96000 |
| PHP | 10,117.22 | 32000 | 6.02 | 12,092.27 | 32000 |

#### One confirm in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 1,604.59 | 2000 | 0.45 | 2,202.27 | 6400 |
| Rust | 2,670.36 | 8000 | 0.22 | 3,495.06 | 12800 |
| Bun | 2,060.76 | 8000 | 0.26 | 3,441.67 | 12800 |
| PHP | 1,858.22 | 4000 | 0.35 | 2,362.23 | 6400 |

### Load

Each cell is the lower of confirm/s and consume/s, the median over repeats with
its range; a cell without a range was measured once.

| Container | Broker | single msg/s | shared msg/s | spread msg/s | spread MiB |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB | RabbitMQ | 55,632.2 | 40,959.9 | 41,172.8 | 180 |
| 1 CPU / 512 MiB | Rust | 192,378.8 [162,464–204,048; n=3] | 69,345.2 [1,966–71,428; n=3] | 167,878.8 [164,638–171,720; n=3] | 369 |
| 1 CPU / 512 MiB | Bun | 237,292.0 [108,632–244,304; n=3] | 206,804.0 [172,964–209,436; n=3] | 262,656.0 [256,488–266,436; n=3] | 210 |
| 1 CPU / 512 MiB | PHP | 43,452.5 [36,896–43,888; n=3] | 78,560.2 [78,386–82,000; n=3] | 80,429.8 [77,712–80,430; n=3] | 195 |
| 1 CPU / 1 GiB | RabbitMQ | 53,795.9 | 38,629.5 | 40,813.8 | 181 |
| 1 CPU / 1 GiB | Rust | 200,970.8 [193,648–202,198; n=3] | 71,694.0 [70,798–73,503; n=3] | 164,285.4 [156,669–169,036; n=3] | 365 |
| 1 CPU / 1 GiB | Bun | 226,228.0 [82,668–240,640; n=3] | 204,344.0 [197,156–210,228; n=3] | 250,152.0 [246,876–253,208; n=3] | 202 |
| 1 CPU / 1 GiB | PHP | 44,720.0 [42,896–44,736; n=3] | 78,800.5 [78,386–79,244; n=3] | 78,290.0 [77,008–81,667; n=3] | 198 |
| 2 CPU / 2 GiB | RabbitMQ | 67,769.0 | 65,547.4 | 81,053.1 | 199 |
| 2 CPU / 2 GiB | Rust | 218,293.4 [210,923–220,917; n=3] | 103,775.4 [98,404–105,642; n=3] | 291,708.5 [282,932–300,315; n=3] | 610 |
| 2 CPU / 2 GiB | Bun | 319,104.0 [237,496–321,940; n=3] | 190,565.4 [141,560–238,212; n=3] | 483,376.0 [423,944–493,468; n=3] | 313 |
| 2 CPU / 2 GiB | PHP | 43,296.0 [41,636–43,456; n=3] | 74,281.2 [74,102–75,843; n=3] | 80,857.1 [79,574–82,141; n=3] | 198 |
| 4 CPU / 4 GiB | RabbitMQ | 76,623.0 | 67,918.0 | 159,492.4 | 245 |
| 4 CPU / 4 GiB | Rust | 221,358.1 [214,224–225,077; n=3] | 121,454.8 [119,023–124,278; n=3] | 414,667.9 [320,586–419,085; n=3] | 780 |
| 4 CPU / 4 GiB | Bun | 218,244.0 [133,096–332,652; n=3] | 233,564.0 [163,276–246,860; n=3] | 703,634.1 [629,512–761,364; n=3] | 591 |
| 4 CPU / 4 GiB | PHP | 43,280.0 [36,928–44,128; n=3] | 79,509.2 [74,530–80,894; n=3] | 80,226.6 [77,006–81,942; n=3] | 201 |
| 4 CPU / 8 GiB | RabbitMQ | 77,596.2 | 65,965.4 | 156,266.4 | 228 |
| 4 CPU / 8 GiB | Rust | 225,685.4 [222,320–226,866; n=3] | 123,360.1 [122,653–124,502; n=3] | 321,875.8 [238,125–428,292; n=3] | 758 |
| 4 CPU / 8 GiB | Bun | 132,460.0 [67,784–324,464; n=3] | 247,492.0 [93,792–261,352; n=3] | 491,232.0 [462,216–752,052; n=3] | 594 |
| 4 CPU / 8 GiB | PHP | 43,968.0 [39,552–44,240; n=3] | 80,100.0 [77,406–80,956; n=3] | 80,001.8 [79,145–81,995; n=3] | 197 |

### Disk ceiling

dd write+fdatasync in a 1 CPU / 1 GiB container on an anonymous volume.

| Written per sync | MB/s (median) | Range |
| --- | ---: | ---: |
| 4 KiB | 10.3 | 9.6–12.2 |
| 64 KiB | 245.3 | 205.2–292.5 |
| 256 KiB | 782.1 | 638.6–810.9 |
| 1 MiB | 1,624.7 | 1,228.8–1,783.2 |
| 4 MiB | 2,528.5 | 1,934.6–2,655.6 |
| 2 GiB, one sync | 3,315.1 | 2,944.2–4,487.8 |

### Scenarios, 1 CPU / 1 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 43,341 [43,074–43,608; n=2] | 142,939 [141,648–144,230; n=2] | 154,566 [153,141–155,990; n=2] | 51,175 [49,847–52,504; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 104,101 [103,745–104,457; n=2] | 210,301 [209,616–210,986; n=2] | 451,902 [442,158–461,646; n=2] | 76,236 [75,941–76,531; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 47,547 [47,106–47,988; n=2] | 151,051 [150,854–151,248; n=2] | 360,278 [358,541–362,015; n=2] | 73,894 [66,596–81,193; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 42,298 [41,914–42,682; n=2] | 92,490 [92,419–92,561; n=2] | 143,457 [141,884–145,030; n=2] | 52,390 [50,769–54,010; n=2] |
| 64 KiB messages | Documents and images: 2 producers of durable 64 KiB bodies, scored in MB/s. | delivered MB/s | 778.0 MB/s [776.9–779.1; n=2] | 1,104.0 MB/s [1,084.4–1,123.5; n=2] | 776.3 MB/s [770.9–781.7; n=2] | 248.9 MB/s [240.6–257.2; n=2] |
| 1 MiB messages | Large payloads: 1 producer of durable 1 MiB bodies, 4 confirms in flight, scored in MB/s. | delivered MB/s | 1,345.8 MB/s [1,336.2–1,355.4; n=2] | 991.8 MB/s [841.0–1,142.5; n=2] | 711.5 MB/s [707.8–715.1; n=2] | 400.1 MB/s [330.6–469.6; n=2] |
| 64 KiB messages, quorum queue | The 64 KiB cell on a quorum queue, which RabbitMQ also fsyncs before the confirm. | delivered MB/s | – | 1,045.7 MB/s [869.3–1,222.1; n=2] | 696.5 MB/s [629.5–763.5; n=2] | 54.8 MB/s [50.4–59.1; n=2] |
| 1 MiB messages, quorum queue | The 1 MiB cell on a quorum queue, which RabbitMQ also fsyncs before the confirm. | delivered MB/s | – | 802.6 MB/s [745.4–859.9; n=2] | 727.4 MB/s [686.8–768.0; n=2] | 152.0 MB/s [141.4–162.6; n=2] |
| Latency at 1,000 msg/s | A steady API workload: 1,000 durable 1 KiB msg/s with confirms. Scored on end-to-end latency. | p99 latency at the offered rate | 1.73 ms [1.30–2.16; n=2] | 2.38 ms [2.32–2.45; n=2] | 1.70 ms [1.56–1.83; n=2] | 24.22 ms [12.64–35.80; n=2] |
| Latency at 10,000 msg/s | A busy API workload: 2 producers at 5,000 durable 1 KiB msg/s each. | p99 latency at the offered rate | 12.80 ms [12.07–13.54; n=2] | 0.67 ms [0.32–1.02; n=2] | 2.80 ms [1.90–3.71; n=2] | 37.24 ms [35.34–39.14; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 16.53 ms [15.29–17.77; n=2] | 5.49 ms [3.89–7.08; n=2] | 2.10 ms [1.98–2.23; n=2] | 36.00 ms [34.24–37.76; n=2] |
| Slow workers, prefetch 1 | 20 workers that each spend 2 ms per job with prefetch 1, fed 5,000 msg/s. | p99 latency at the offered rate | 10.72 ms [10.44–11.01; n=2] | 2.16 ms [0.47–3.85; n=2] | 11.99 ms [8.06–15.92; n=2] | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 129,583 [125,627–133,539; n=2] | 129,627 [127,168–132,086; n=2] | 32,329 [31,768–32,891; n=2] |
| Stream queue | An append-only log read by 2 consumers from the start, over AMQP 0-9-1. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 103,929 [97,556–110,302; n=2] | 103,355 [102,271–104,438; n=2] | 59,964 [59,737–60,191; n=2] |
| Priority queue | x-max-priority=10, every message at priority 5, 2 producers and 2 consumers. | delivered msg/s | 32,635 [32,588–32,682; n=2] | 149,212 [148,512–149,912; n=2] | 154,503 [150,910–158,096; n=2] | 40,324 [40,157–40,491; n=2] |
| Transactions | tx.select publishers committing every 10 messages, transactional consumers acking every 10. | delivered msg/s | 21,134 [20,235–22,033; n=2] | 47,222 [41,469–52,976; n=2] | 45,772 [42,666–48,878; n=2] | 17,072 [16,118–18,026; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 34,843 [34,829–34,858; n=2] | 105,154 [104,207–106,101; n=2] | 140,589 [137,888–143,291; n=2] | 64,726 [64,592–64,861; n=2] |
| Backlog fill and drain | A consumer outage: 300,000 durable 1 KiB messages pile up, then 4 consumers drain them. | fill / drain msg/s | 74,235 / 50,007 [50,002–50,012; n=2] | 176,706 / 149,895 [149,878–149,911; n=2] | 149,420 / 149,743 [149,694–149,792; n=2] | 98,791 / 74,965 [74,912–75,018; n=2] |

Peak container memory, 1 CPU / 1 GiB (MiB, page cache included):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 194 | 156 | 187 | 100 |
| Fire-and-forget telemetry | 254 | 140 | 88 | 26 |
| Broadcast to 20 services | 186 | 1,024 | 154 | 360 |
| 64 queues, own publisher each | 220 | 1,024 | 192 | 246 |
| 64 KiB messages | 368 | 147 | 166 | 56 |
| 1 MiB messages | 335 | 156 | 163 | 154 |
| 64 KiB messages, quorum queue | – | 149 | 302 | 236 |
| 1 MiB messages, quorum queue | – | 155 | 205 | 271 |
| Latency at 1,000 msg/s | 152 | 42 | 96 | 27 |
| Latency at 10,000 msg/s | 153 | 140 | 116 | 36 |
| 500 queues, 1,000 connections | 422 | 1,024 | 154 | 130 |
| Slow workers, prefetch 1 | 176 | 94 | 106 | 58 |
| Quorum queue | 971 | 143 | 123 | 1,017 |
| Stream queue | 1,024 | 1,024 | 1,024 | 1,024 |
| Priority queue | 152 | 142 | 143 | 51 |
| Transactions | 151 | 142 | 106 | 32 |
| Heavy mixed load | 234 | 1,024 | 326 | 1,024 |
| Backlog fill and drain | 534 | 814 | 816 | 995 |

Disk writes and fsyncs over the measured window, 1 CPU / 1 GiB:

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | – | 173.8 MB/s, 1,007 fsync/s at 0.41 ms | 199.7 MB/s, 1,712 fsync/s at 0.25 ms | 62.8 MB/s, 254 fsync/s at 0.54 ms |
| Fire-and-forget telemetry | – | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s |
| Broadcast to 20 services | – | 202.6 MB/s, 1,899 fsync/s at 0.85 ms | 28.1 MB/s, 410 fsync/s at 0.37 ms | 49.7 MB/s, 37 fsync/s at 1.32 ms |
| 64 queues, own publisher each | – | 155.2 MB/s, 4,694 fsync/s at 2.09 ms | 112.5 MB/s, 2,039 fsync/s at 0.17 ms | 35.4 MB/s, 41 fsync/s at 1.06 ms |
| 64 KiB messages | – | 1,105.8 MB/s, 991 fsync/s at 0.50 ms | 783.9 MB/s, 2,050 fsync/s at 0.39 ms | 257.1 MB/s, 1,888 fsync/s at 0.38 ms |
| 1 MiB messages | – | 1,006.5 MB/s, 674 fsync/s at 1.06 ms | 716.0 MB/s, 625 fsync/s at 1.23 ms | 389.3 MB/s, 733 fsync/s at 0.95 ms |
| 64 KiB messages, quorum queue | – | 1,044.0 MB/s, 927 fsync/s at 0.66 ms | 719.0 MB/s, 1,384 fsync/s at 0.57 ms | 111.0 MB/s, 351 fsync/s at 0.85 ms |
| 1 MiB messages, quorum queue | – | 815.3 MB/s, 580 fsync/s at 1.23 ms | 828.8 MB/s, 616 fsync/s at 1.16 ms | 346.9 MB/s, 278 fsync/s at 0.61 ms |
| Latency at 1,000 msg/s | – | 3.2 MB/s, 103 fsync/s at 0.83 ms | 5.4 MB/s, 1,022 fsync/s at 0.21 ms | 8.7 MB/s, 1,817 fsync/s at 0.28 ms |
| Latency at 10,000 msg/s | – | 16.2 MB/s, 99 fsync/s at 0.30 ms | 46.9 MB/s, 8,403 fsync/s at 0.08 ms | 23.9 MB/s, 2,890 fsync/s at 0.27 ms |
| 500 queues, 1,000 connections | – | 65.5 MB/s, 7,613 fsync/s at 25.34 ms | 13.8 MB/s, 2,497 fsync/s at 0.11 ms | 6.4 MB/s, 759 fsync/s at 0.64 ms |
| Slow workers, prefetch 1 | – | 7.3 MB/s, 99 fsync/s at 0.29 ms | 22.6 MB/s, 4,608 fsync/s at 0.08 ms | 9.7 MB/s, 1,391 fsync/s at 0.64 ms |
| Quorum queue | – | 159.6 MB/s, 935 fsync/s at 0.45 ms | 174.0 MB/s, 1,591 fsync/s at 0.26 ms | 99.8 MB/s, 136 fsync/s at 0.60 ms |
| Stream queue | – | 204.8 MB/s | 172.6 MB/s, 0 fsync/s | 36.8 MB/s, 0 fsync/s |
| Priority queue | – | 183.8 MB/s, 1,159 fsync/s at 0.20 ms | 202.1 MB/s, 2,114 fsync/s at 0.20 ms | 50.2 MB/s, 401 fsync/s at 0.39 ms |
| Transactions | – | 92.3 MB/s, 4,698 fsync/s at 0.17 ms | 44.5 MB/s, 1,941 fsync/s at 0.39 ms | 15.1 MB/s, 522 fsync/s at 1.58 ms |
| Heavy mixed load | – | 204.8 MB/s, 1,322 fsync/s at 0.92 ms | 259.4 MB/s, 1,419 fsync/s at 0.27 ms | 76.2 MB/s, 17 fsync/s at 2.45 ms |
| Backlog fill and drain | – | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s | 1.8 MB/s, 3 fsync/s at 0.38 ms |

### Scenarios, 4 CPU / 4 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 78,439 [76,724–80,154; n=2] | 252,266 [248,632–255,900; n=2] | 186,422 [178,364–194,480; n=2] | 54,028 [52,537–55,518; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 326,615 [325,030–328,199; n=2] | 226,795 [215,758–237,832; n=2] | 453,946 [446,651–461,242; n=2] | 84,906 [81,789–88,024; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 161,952 [161,576–162,329; n=2] | 268,424 [266,961–269,887; n=2] | 517,463 [514,722–520,203; n=2] | 70,905 [66,138–75,671; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 139,264 [137,458–141,070; n=2] | 227,505 [224,190–230,820; n=2] | 314,412 [311,882–316,942; n=2] | 58,090 [57,841–58,338; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 3.29 ms [3.01–3.56; n=2] | 1.66 ms [1.55–1.77; n=2] | 1.42 ms [1.16–1.69; n=2] | 33.70 ms [29.50–37.90; n=2] |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | 85,414 [85,270–85,557; n=2] | 198,723 [157,293–240,153; n=2] | 86,567 [84,339–88,796; n=2] | 34,597 [32,985–36,210; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 122,603 [122,138–123,069; n=2] | 255,664 [216,574–294,755; n=2] | 269,674 [236,983–302,364; n=2] | 61,166 [59,867–62,466; n=2] |

Peak container memory, 4 CPU / 4 GiB (MiB, page cache included):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 172 | 148 | 250 | 82 |
| Fire-and-forget telemetry | 238 | 144 | 191 | 22 |
| Broadcast to 20 services | 214 | 2,656 | 530 | 360 |
| 64 queues, own publisher each | 299 | 3,222 | 565 | 240 |
| 500 queues, 1,000 connections | 480 | 2,224 | 522 | 67 |
| Quorum queue | 770 | 148 | 294 | 1,040 |
| Heavy mixed load | 410 | 2,246 | 721 | 1,213 |

Disk writes and fsyncs over the measured window, 4 CPU / 4 GiB:

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | – | 305.3 MB/s, 1,898 fsync/s at 0.21 ms | 242.9 MB/s, 2,342 fsync/s at 0.17 ms | 66.5 MB/s, 269 fsync/s at 0.45 ms |
| Fire-and-forget telemetry | – | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s |
| Broadcast to 20 services | – | 285.2 MB/s, 2,718 fsync/s at 1.56 ms | 104.1 MB/s, 3,215 fsync/s at 0.23 ms | 47.9 MB/s, 35 fsync/s at 1.16 ms |
| 64 queues, own publisher each | – | 373.1 MB/s, 11,412 fsync/s at 2.08 ms | 266.3 MB/s, 10,263 fsync/s at 0.21 ms | 39.1 MB/s, 45 fsync/s at 0.97 ms |
| 500 queues, 1,000 connections | – | 83.2 MB/s, 9,762 fsync/s at 1.77 ms | 22.1 MB/s, 4,540 fsync/s at 0.18 ms | 9.7 MB/s, 1,567 fsync/s at 0.25 ms |
| Quorum queue | – | 237.2 MB/s, 1,467 fsync/s at 0.38 ms | 250.7 MB/s, 89 fsync/s at 1.52 ms | 104.4 MB/s, 143 fsync/s at 0.71 ms |
| Heavy mixed load | – | 379.4 MB/s, 2,074 fsync/s at 2.79 ms | 415.1 MB/s, 2,930 fsync/s at 0.88 ms | 71.9 MB/s, 16 fsync/s at 2.51 ms |

### Connections

| Broker | Size | Held | Memory at hold |
| --- | --- | ---: | ---: |
| RabbitMQ | 1 CPU / 512m | 3,500 | 454.7 MiB |
| RabbitMQ | 2 CPU / 2g | 18,750 | 1.93 GiB |
| RabbitMQ | 4 CPU / 4g | 41,622 | 3.579 GiB |
| RabbitMQ | 4 CPU / 8g | 62,065 | 5.55 GiB |
| Rust | 1 CPU / 512m | 8,980 | 502.5 MiB |
| Rust | 2 CPU / 2g | 34,600 | 1.874 GiB |
| Rust | 4 CPU / 4g | 67,147 | 3.636 GiB |
| Rust | 4 CPU / 8g | 95,000 | 5.122 GiB |
| Bun | 1 CPU / 512m | 40,500 | 356.3 MiB |
| Bun | 2 CPU / 2g | 125,057 | 1.462 GiB |
| Bun | 4 CPU / 4g | 255,189 | 3.005 GiB |
| Bun | 4 CPU / 8g | 260,000 | 2.897 GiB |
| PHP | 1 CPU / 512m | 42,000 | 316.1 MiB |
| PHP | 2 CPU / 2g | 40,000 | 302.4 MiB |
| PHP | 4 CPU / 4g | 40,000 | 300.7 MiB |
| PHP | 4 CPU / 8g | 40,000 | 300.7 MiB |
