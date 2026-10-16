### Paced

#### 128 confirms in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 9,310.34 | 32000 | 4.41 | 7,022.51 | 32000 |
| Rust | 20,860.18 | 64000 | 1.79 | 24,656.21 | 96000 |
| Bun | 20,139.61 | 64000 | 0.81 | 23,667.83 | 96000 |
| PHP | 12,391.87 | 32000 | 4.27 | 16,041.45 | 48000 |

#### One confirm in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 1,604.59 | 2000 | 0.45 | 2,202.27 | 6400 |
| Rust | 2,642.34 | 8000 | 0.23 | 3,439.80 | 12800 |
| Bun | 2,047.31 | 8000 | 0.27 | 3,437.64 | 12800 |
| PHP | 1,848.26 | 4000 | 0.35 | 2,363.57 | 6400 |

### Load

Each cell is the lower of confirm/s and consume/s, the median over repeats with
its range; a cell without a range was measured once.

| Container | Broker | single msg/s | shared msg/s | spread msg/s | spread MiB |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB | RabbitMQ | 55,632.2 | 40,959.9 | 41,172.8 | 180 |
| 1 CPU / 512 MiB | Rust | 214,106.5 [211,428–216,891; n=3] | 163,136.9 [156,714–165,646; n=3] | 210,140.0 [205,696–214,029; n=3] | 504 |
| 1 CPU / 512 MiB | Bun | 123,620.0 [68,460–272,492; n=3] | 153,924.0 [136,336–243,950; n=3] | 234,344.0 [209,688–312,204; n=3] | 176 |
| 1 CPU / 512 MiB | PHP | 33,840.0 [28,000–45,920; n=3] | 100,231.9 [98,090–101,946; n=3] | 106,525.5 [103,104–106,954; n=3] | 252 |
| 1 CPU / 1 GiB | RabbitMQ | 53,795.9 | 38,629.5 | 40,813.8 | 181 |
| 1 CPU / 1 GiB | Rust | 224,416.0 [218,288–224,491; n=3] | 149,161.1 [146,739–149,272; n=3] | 207,986.2 [207,160–210,169; n=3] | 509 |
| 1 CPU / 1 GiB | Bun | 240,716.0 [118,504–264,608; n=3] | 138,010.5 [85,932–218,100; n=3] | 327,688.0 [288,596–348,764; n=3] | 193 |
| 1 CPU / 1 GiB | PHP | 47,424.0 [46,912–47,728; n=3] | 99,374.0 [98,090–102,374; n=3] | 106,098.2 [103,959–108,665; n=3] | 254 |
| 2 CPU / 2 GiB | RabbitMQ | 67,769.0 | 65,547.4 | 81,053.1 | 199 |
| 2 CPU / 2 GiB | Rust | 250,607.6 [246,544–251,723; n=3] | 232,988.2 [232,750–239,894; n=3] | 366,514.1 [178,783–374,902; n=3] | 807 |
| 2 CPU / 2 GiB | Bun | 166,700.0 [124,608–201,360; n=3] | 254,208.0 [247,705–259,600; n=3] | 426,776.0 [380,020–448,556; n=3] | 290 |
| 2 CPU / 2 GiB | PHP | 46,464.0 [44,336–46,752; n=3] | 101,089.9 [98,946–101,518; n=3] | 107,381.9 [105,670–107,382; n=3] | 258 |
| 4 CPU / 4 GiB | RabbitMQ | 76,623.0 | 67,918.0 | 159,492.4 | 245 |
| 4 CPU / 4 GiB | Rust | 251,925.4 [195,110–259,872; n=3] | 174,724.4 [88,070–175,335; n=3] | 361,737.8 [317,639–503,795; n=3] | 1193 |
| 4 CPU / 4 GiB | Bun | 242,448.0 [107,000–290,280; n=3] | 141,440.0 [134,581–258,862; n=3] | 751,071.1 [639,516–892,900; n=3] | 605 |
| 4 CPU / 4 GiB | PHP | 43,728.0 [36,816–46,512; n=3] | 101,517.6 [98,518–102,802; n=3] | 108,237.8 [105,243–109,093; n=3] | 251 |
| 4 CPU / 8 GiB | RabbitMQ | 77,596.2 | 65,965.4 | 156,266.4 | 228 |
| 4 CPU / 8 GiB | Rust | 267,845.4 [259,200–274,197; n=3] | 187,052.8 [183,748–219,369; n=3] | 350,963.8 [292,641–467,060; n=3] | 1067 |
| 4 CPU / 8 GiB | Bun | 82,720.0 [78,656–110,220; n=3] | 224,320.0 [217,841–266,700; n=3] | 612,384.0 [597,396–937,944; n=3] | 614 |
| 4 CPU / 8 GiB | PHP | 46,112.0 [43,984–46,912; n=3] | 101,945.9 [101,516–102,374; n=3] | 106,953.9 [106,098–107,809; n=3] | 258 |

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
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 43,341 [43,074–43,608; n=2] | 156,010 [153,067–158,953; n=2] | 171,865 [164,168–179,562; n=2] | 64,815 [64,538–65,091; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 104,101 [103,745–104,457; n=2] | 254,488 [249,597–259,380; n=2] | 583,696 [572,971–594,421; n=2] | 183,128 [180,471–185,785; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 47,547 [47,106–47,988; n=2] | 175,203 [174,416–175,989; n=2] | 418,012 [414,838–421,187; n=2] | 117,680 [117,668–117,693; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 42,298 [41,914–42,682; n=2] | 118,014 [117,043–118,985; n=2] | 185,609 [181,063–190,155; n=2] | 68,232 [67,833–68,630; n=2] |
| 64 KiB messages | Documents and images: 2 producers of durable 64 KiB bodies, scored in MB/s. | delivered MB/s | 778.0 MB/s [776.9–779.1; n=2] | 1,224.6 MB/s [1,210.3–1,238.9; n=2] | 797.6 MB/s [785.3–810.0; n=2] | 272.5 MB/s [255.0–290.0; n=2] |
| 1 MiB messages | Large payloads: 1 producer of durable 1 MiB bodies, 4 confirms in flight, scored in MB/s. | delivered MB/s | 1,345.8 MB/s [1,336.2–1,355.4; n=2] | 1,182.7 MB/s [1,114.2–1,251.1; n=2] | 810.5 MB/s [719.8–901.3; n=2] | 371.7 MB/s [351.6–391.8; n=2] |
| 64 KiB messages, quorum queue | The 64 KiB cell on a quorum queue, which RabbitMQ also fsyncs before the confirm. | delivered MB/s | – | 763.6 MB/s [738.1–789.1; n=2] | 624.7 MB/s [609.2–640.1; n=2] | 60.2 MB/s [55.8–64.7; n=2] |
| 1 MiB messages, quorum queue | The 1 MiB cell on a quorum queue, which RabbitMQ also fsyncs before the confirm. | delivered MB/s | – | 955.2 MB/s [936.7–973.8; n=2] | 804.1 MB/s [795.4–812.8; n=2] | 163.4 MB/s [155.4–171.5; n=2] |
| Latency at 1,000 msg/s | A steady API workload: 1,000 durable 1 KiB msg/s with confirms. Scored on end-to-end latency. | p99 latency at the offered rate | 1.73 ms [1.30–2.16; n=2] | 2.97 ms [2.74–3.20; n=2] | 1.70 ms [1.52–1.88; n=2] | 17.59 ms [10.68–24.49; n=2] |
| Latency at 10,000 msg/s | A busy API workload: 2 producers at 5,000 durable 1 KiB msg/s each. | p99 latency at the offered rate | 12.80 ms [12.07–13.54; n=2] | 0.94 ms [0.66–1.21; n=2] | 1.06 ms [0.70–1.42; n=2] | 24.79 ms [21.61–27.96; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 16.53 ms [15.29–17.77; n=2] | unscored (1/2 complete; 0 failed, 0 incomplete, 1 missed rate) | 2.81 ms [1.99–3.64; n=2] | 151.18 ms [39.61–262.75; n=2] |
| Slow workers, prefetch 1 | 20 workers that each spend 2 ms per job with prefetch 1, fed 5,000 msg/s. | p99 latency at the offered rate | 10.72 ms [10.44–11.01; n=2] | 0.77 ms [0.29–1.25; n=2] | 8.97 ms [7.83–10.11; n=2] | unscored (1/2 complete; 0 failed, 0 incomplete, 1 missed rate) |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 160,433 [156,533–164,333; n=2] | 127,971 [120,666–135,276; n=2] | 39,374 [38,467–40,280; n=2] |
| Stream queue | An append-only log read by 2 consumers from the start, over AMQP 0-9-1. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 120,232 [105,055–135,408; n=2] | 111,951 [110,310–113,593; n=2] | 74,058 [72,744–75,372; n=2] |
| Priority queue | x-max-priority=10, every message at priority 5, 2 producers and 2 consumers. | delivered msg/s | 32,635 [32,588–32,682; n=2] | 171,586 [170,800–172,371; n=2] | 163,209 [156,246–170,171; n=2] | 45,125 [44,829–45,420; n=2] |
| Transactions | tx.select publishers committing every 10 messages, transactional consumers acking every 10. | delivered msg/s | 21,134 [20,235–22,033; n=2] | 40,712 [35,609–45,815; n=2] | 39,398 [36,444–42,351; n=2] | 24,187 [20,239–28,134; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 34,843 [34,829–34,858; n=2] | 119,440 [114,841–124,039; n=2] | 143,677 [128,424–158,930; n=2] | 74,043 [73,453–74,632; n=2] |
| Backlog fill and drain | A consumer outage: 300,000 durable 1 KiB messages pile up, then 4 consumers drain them. | fill / drain msg/s | 74,235 / 50,007 [50,002–50,012; n=2] | 180,182 / 149,792 [149,597–149,988; n=2] | 148,738 / 149,576 [149,222–149,932; n=2] | 96,411 / 149,586 [149,396–149,778; n=2] |

Peak container memory, 1 CPU / 1 GiB (MiB, page cache included):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 194 | 184 | 186 | 108 |
| Fire-and-forget telemetry | 254 | 171 | 92 | 22 |
| Broadcast to 20 services | 186 | 1,024 | 152 | 366 |
| 64 queues, own publisher each | 220 | 1,024 | 182 | 246 |
| 64 KiB messages | 368 | 180 | 166 | 54 |
| 1 MiB messages | 335 | 200 | 156 | 152 |
| 64 KiB messages, quorum queue | – | 178 | 198 | 1,024 |
| 1 MiB messages, quorum queue | – | 198 | 178 | 258 |
| Latency at 1,000 msg/s | 152 | 67 | 91 | 27 |
| Latency at 10,000 msg/s | 153 | 166 | 110 | 30 |
| 500 queues, 1,000 connections | 422 | 1,024 | 148 | 124 |
| Slow workers, prefetch 1 | 176 | 118 | 110 | 46 |
| Quorum queue | 971 | 173 | 124 | 1,024 |
| Stream queue | 1,024 | 1,024 | 1,024 | 1,024 |
| Priority queue | 152 | 168 | 146 | 52 |
| Transactions | 151 | 165 | 108 | 32 |
| Heavy mixed load | 234 | 1,024 | 301 | 1,022 |
| Backlog fill and drain | 534 | 852 | 820 | 716 |

Disk writes and fsyncs over the measured window, 1 CPU / 1 GiB:

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | – | 189.6 MB/s, 1,119 fsync/s at 0.37 ms | 222.9 MB/s, 1,880 fsync/s at 0.24 ms | 79.8 MB/s, 324 fsync/s at 0.53 ms |
| Fire-and-forget telemetry | – | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s |
| Broadcast to 20 services | – | 228.0 MB/s, 1,850 fsync/s at 1.40 ms | 32.7 MB/s, 497 fsync/s at 0.33 ms | 78.9 MB/s, 60 fsync/s at 1.18 ms |
| 64 queues, own publisher each | – | 187.6 MB/s, 5,933 fsync/s at 3.12 ms | 145.3 MB/s, 2,554 fsync/s at 0.15 ms | 46.1 MB/s, 53 fsync/s at 0.99 ms |
| 64 KiB messages | – | 1,229.2 MB/s, 1,116 fsync/s at 0.45 ms | 797.6 MB/s, 1,864 fsync/s at 0.44 ms | 284.9 MB/s, 2,091 fsync/s at 0.32 ms |
| 1 MiB messages | – | 1,134.8 MB/s, 645 fsync/s at 1.07 ms | 814.3 MB/s, 706 fsync/s at 1.07 ms | 362.4 MB/s, 682 fsync/s at 0.98 ms |
| 64 KiB messages, quorum queue | – | 788.2 MB/s, 707 fsync/s at 0.90 ms | 639.0 MB/s, 1,283 fsync/s at 0.59 ms | 138.1 MB/s, 438 fsync/s at 0.60 ms |
| 1 MiB messages, quorum queue | – | 966.2 MB/s, 658 fsync/s at 1.07 ms | 898.5 MB/s, 688 fsync/s at 0.95 ms | 369.1 MB/s, 295 fsync/s at 0.64 ms |
| Latency at 1,000 msg/s | – | 3.2 MB/s, 100 fsync/s at 0.75 ms | 5.5 MB/s, 1,024 fsync/s at 0.21 ms | 8.7 MB/s, 1,811 fsync/s at 0.31 ms |
| Latency at 10,000 msg/s | – | 16.1 MB/s, 99 fsync/s at 0.30 ms | 42.8 MB/s, 7,418 fsync/s at 0.08 ms | 23.3 MB/s, 2,869 fsync/s at 0.27 ms |
| 500 queues, 1,000 connections | – | 71.0 MB/s, 8,264 fsync/s at 16.06 ms | 12.7 MB/s, 2,227 fsync/s at 0.13 ms | 6.3 MB/s, 727 fsync/s at 0.65 ms |
| Slow workers, prefetch 1 | – | 7.3 MB/s, 99 fsync/s at 0.23 ms | 23.2 MB/s, 4,771 fsync/s at 0.07 ms | 14.9 MB/s, 2,599 fsync/s at 0.39 ms |
| Quorum queue | – | 194.0 MB/s, 1,134 fsync/s at 0.36 ms | 171.9 MB/s, 1,564 fsync/s at 0.29 ms | 123.3 MB/s, 166 fsync/s at 0.45 ms |
| Stream queue | – | 250.3 MB/s | 184.4 MB/s, 0 fsync/s | 46.0 MB/s, 0 fsync/s |
| Priority queue | – | 211.3 MB/s, 1,332 fsync/s at 0.18 ms | 212.9 MB/s, 2,240 fsync/s at 0.19 ms | 57.0 MB/s, 462 fsync/s at 0.42 ms |
| Transactions | – | 76.7 MB/s, 3,877 fsync/s at 0.24 ms | 54.7 MB/s, 2,717 fsync/s at 0.29 ms | 17.7 MB/s, 616 fsync/s at 1.39 ms |
| Heavy mixed load | – | 219.2 MB/s, 1,444 fsync/s at 1.78 ms | 259.5 MB/s, 1,375 fsync/s at 0.34 ms | 87.0 MB/s, 19 fsync/s at 2.63 ms |
| Backlog fill and drain | – | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 3 fsync/s at 0.55 ms |

### Scenarios, 4 CPU / 4 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 78,439 [76,724–80,154; n=2] | 298,718 [298,216–299,220; n=2] | 205,800 [200,988–210,612; n=2] | 69,460 [69,454–69,465; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 326,615 [325,030–328,199; n=2] | 279,229 [275,536–282,922; n=2] | 620,869 [620,373–621,365; n=2] | 190,221 [189,356–191,086; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 161,952 [161,576–162,329; n=2] | 309,256 [303,328–315,184; n=2] | 496,648 [494,457–498,839; n=2] | 121,924 [120,993–122,856; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 139,264 [137,458–141,070; n=2] | 237,621 [226,296–248,947; n=2] | 283,660 [276,264–291,056; n=2] | 63,202 [62,712–63,692; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 3.29 ms [3.01–3.56; n=2] | 1.62 ms [1.06–2.18; n=2] | 1.76 ms [1.71–1.81; n=2] | 61.44 ms [36.67–86.21; n=2] |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | 85,414 [85,270–85,557; n=2] | 265,104 [237,376–292,832; n=2] | 127,614 [116,986–138,241; n=2] | 37,653 [37,375–37,930; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 122,603 [122,138–123,069; n=2] | 284,213 [267,455–300,970; n=2] | 220,090 [188,775–251,405; n=2] | 80,807 [80,419–81,194; n=2] |

Peak container memory, 4 CPU / 4 GiB (MiB, page cache included):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 172 | 182 | 254 | 80 |
| Fire-and-forget telemetry | 238 | 222 | 194 | 22 |
| Broadcast to 20 services | 214 | 2,709 | 572 | 350 |
| 64 queues, own publisher each | 299 | 3,458 | 614 | 238 |
| 500 queues, 1,000 connections | 480 | 2,544 | 599 | 126 |
| Quorum queue | 770 | 196 | 241 | 1,192 |
| Heavy mixed load | 410 | 2,778 | 756 | 1,216 |

Disk writes and fsyncs over the measured window, 4 CPU / 4 GiB:

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | – | 362.5 MB/s, 2,141 fsync/s at 0.26 ms | 268.1 MB/s, 2,618 fsync/s at 0.16 ms | 85.4 MB/s, 348 fsync/s at 0.41 ms |
| Fire-and-forget telemetry | – | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s |
| Broadcast to 20 services | – | 309.4 MB/s, 3,151 fsync/s at 1.54 ms | 99.5 MB/s, 3,017 fsync/s at 0.26 ms | 82.3 MB/s, 63 fsync/s at 1.12 ms |
| 64 queues, own publisher each | – | 399.0 MB/s, 12,203 fsync/s at 2.00 ms | 250.6 MB/s, 11,622 fsync/s at 0.20 ms | 42.6 MB/s, 49 fsync/s at 1.10 ms |
| 500 queues, 1,000 connections | – | 82.3 MB/s, 9,653 fsync/s at 2.44 ms | 21.7 MB/s, 4,438 fsync/s at 0.18 ms | 8.2 MB/s, 1,146 fsync/s at 0.31 ms |
| Quorum queue | – | 322.8 MB/s, 1,906 fsync/s at 0.31 ms | 170.8 MB/s, 1,680 fsync/s at 0.29 ms | 117.5 MB/s, 158 fsync/s at 0.54 ms |
| Heavy mixed load | – | 392.9 MB/s, 2,627 fsync/s at 2.47 ms | 355.1 MB/s, 2,807 fsync/s at 1.01 ms | 95.7 MB/s, 21 fsync/s at 2.40 ms |

### Connections

| Broker | Size | Held | Memory at hold |
| --- | --- | ---: | ---: |
| RabbitMQ | 1 CPU / 512m | 3,500 | 454.7 MiB |
| RabbitMQ | 2 CPU / 2g | 18,750 | 1.93 GiB |
| RabbitMQ | 4 CPU / 4g | 41,622 | 3.579 GiB |
| RabbitMQ | 4 CPU / 8g | 62,065 | 5.55 GiB |
| Rust | 1 CPU / 512m | 6,860 | 468.5 MiB |
| Rust | 2 CPU / 2g | 28,655 | 1.816 GiB |
| Rust | 4 CPU / 4g | 57,412 | 3.663 GiB |
| Rust | 4 CPU / 8g | 95,000 | 5.977 GiB |
| Bun | 1 CPU / 512m | 42,187 | 381.6 MiB |
| Bun | 2 CPU / 2g | 121,593 | 1.459 GiB |
| Bun | 4 CPU / 4g | 224,979 | 3.357 GiB |
| Bun | 4 CPU / 8g | 260,000 | 3.637 GiB |
| PHP | 1 CPU / 512m | 41,961 | 318.1 MiB |
| PHP | 2 CPU / 2g | 40,000 | 302.6 MiB |
| PHP | 4 CPU / 4g | 40,000 | 303 MiB |
| PHP | 4 CPU / 8g | 40,000 | 300.6 MiB |
