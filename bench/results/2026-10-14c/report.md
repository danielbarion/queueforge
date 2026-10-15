### Paced

#### 128 confirms in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 9,310.34 | 32000 | 4.41 | 7,022.51 | 32000 |
| Rust | 19,026.76 | 48000 | 2.07 | 23,775.86 | 96000 |
| Bun | 19,919.13 | 48000 | 0.81 | 23,981.23 | 96000 |
| PHP | 6,695.41 | 16000 | 9.90 | 10,436.78 | 32000 |

#### One confirm in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 1,604.59 | 2000 | 0.45 | 2,202.27 | 6400 |
| Rust | 1,518.57 | 1000 | 0.33 | 3,204.46 | 800 |
| Bun | 2,714.19 | 8000 | 0.21 | 3,432.00 | 12800 |
| PHP | 1,432.29 | 2000 | 0.36 | 2,142.75 | 6400 |

### Load

Each cell is the lower of confirm/s and consume/s, the median over repeats with
its range; a cell without a range was measured once.

| Container | Broker | single msg/s | shared msg/s | spread msg/s | spread MiB |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB | RabbitMQ | 55,632.2 | 40,959.9 | 41,172.8 | 180 |
| 1 CPU / 512 MiB | Rust | 151,312.0 | 62,989.5 | 151,524.4 | 365 |
| 1 CPU / 512 MiB | Bun | 167,032.0 [15,960–254,852; n=3] | 195,395.0 [159,992–217,452; n=3] | 196,112.0 [172,484–272,616; n=3] | 208 |
| 1 CPU / 512 MiB | PHP | 11,840.0 | 81,814.0 | 75,295.9 | 184 |
| 1 CPU / 1 GiB | RabbitMQ | 53,795.9 | 38,629.5 | 40,813.8 | 181 |
| 1 CPU / 1 GiB | Rust | 163,130.8 | 61,090.6 | 143,230.4 | 362 |
| 1 CPU / 1 GiB | Bun | 238,252.0 [102,352–259,092; n=3] | 217,496.0 [115,233–220,457; n=3] | 226,690.6 [148,638–238,996; n=3] | 204 |
| 1 CPU / 1 GiB | PHP | 11,808.0 | 76,244.0 | 72,301.5 | 177 |
| 2 CPU / 2 GiB | RabbitMQ | 67,769.0 | 65,547.4 | 81,053.1 | 199 |
| 2 CPU / 2 GiB | Rust | 197,997.5 | 85,598.5 | 250,649.5 | 538 |
| 2 CPU / 2 GiB | Bun | 234,636.0 [216,752–235,808; n=3] | 253,836.0 [121,816–260,416; n=3] | 346,168.0 [338,740–430,928; n=3] | 307 |
| 2 CPU / 2 GiB | PHP | 11,888.0 | 76,672.0 | 78,134.2 | 190 |
| 4 CPU / 4 GiB | RabbitMQ | 76,623.0 | 67,918.0 | 159,492.4 | 245 |
| 4 CPU / 4 GiB | Rust | 203,649.4 | 124,121.1 | 384,290.6 | 701 |
| 4 CPU / 4 GiB | Bun | 231,272.0 [136,355–321,252; n=3] | 119,760.0 [48,832–277,034; n=3] | 823,600.0 [665,044–860,716; n=3] | 578 |
| 4 CPU / 4 GiB | PHP | 11,751.1 | 76,254.0 | 82,386.8 | 205 |
| 4 CPU / 8 GiB | RabbitMQ | 77,596.2 | 65,965.4 | 156,266.4 | 228 |
| 4 CPU / 8 GiB | Rust | 199,296.0 | 109,002.2 | 361,130.1 | 722 |
| 4 CPU / 8 GiB | Bun | 222,980.0 [198,592–225,116; n=3] | 273,230.5 [267,742–280,580; n=3] | 636,900.0 [459,388–786,528; n=3] | 491 |
| 4 CPU / 8 GiB | PHP | 11,920.0 | 81,813.9 | 77,807.4 | 196 |

### Disk ceiling

dd write+fdatasync in a 1 CPU / 1 GiB container on an anonymous volume.

| Written per sync | MB/s (median) | Range |
| --- | ---: | ---: |
| 4 KiB | 16.0 | 6.8–18.8 |
| 64 KiB | 280.2 | 98.0–293.7 |
| 256 KiB | 821.8 | 527.4–893.6 |
| 1 MiB | 1,598.1 | 1,163.4–1,941.7 |
| 4 MiB | 2,599.1 | 1,036.8–2,620.5 |
| 2 GiB, one sync | 1,216.0 | 1,109.0–1,943.3 |

### Scenarios, 1 CPU / 1 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 43,341 [43,074–43,608; n=2] | 130,415 [129,918–130,911; n=2] | 164,266 [156,826–171,706; n=2] | 39,139 [38,774–39,505; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 104,101 [103,745–104,457; n=2] | 94,260 [94,143–94,378; n=2] | 554,908 [551,737–558,078; n=2] | 90,752 [89,844–91,659; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 47,547 [47,106–47,988; n=2] | 135,620 [134,244–136,995; n=2] | 430,044 [429,964–430,124; n=2] | 84,598 [81,360–87,836; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 42,298 [41,914–42,682; n=2] | 91,002 [87,815–94,189; n=2] | 179,135 [177,287–180,984; n=2] | 52,064 [51,253–52,874; n=2] |
| 64 KiB messages | Documents and images: 2 producers of durable 64 KiB bodies, scored in MB/s. | delivered MB/s | 778.0 MB/s [776.9–779.1; n=2] | 252.3 MB/s [252.2–252.5; n=2] | 856.3 MB/s [848.4–864.3; n=2] | 230.6 MB/s [215.2–246.1; n=2] |
| 1 MiB messages | Large payloads: 1 producer of durable 1 MiB bodies, 4 confirms in flight, scored in MB/s. | delivered MB/s | 1,345.8 MB/s [1,336.2–1,355.4; n=2] | 573.9 MB/s [554.9–592.9; n=2] | 1,064.7 MB/s [1,057.9–1,071.4; n=2] | 605.0 MB/s [590.1–619.8; n=2] |
| 64 KiB messages, quorum queue | The 64 KiB cell on a quorum queue, which RabbitMQ also fsyncs before the confirm. | delivered MB/s | – | – | 817.9 MB/s [786.5–849.2; n=2] | – |
| 1 MiB messages, quorum queue | The 1 MiB cell on a quorum queue, which RabbitMQ also fsyncs before the confirm. | delivered MB/s | – | – | 640.7 MB/s [583.0–698.4; n=2] | – |
| Latency at 1,000 msg/s | A steady API workload: 1,000 durable 1 KiB msg/s with confirms. Scored on end-to-end latency. | p99 latency at the offered rate | 1.73 ms [1.30–2.16; n=2] | 1.08 ms [0.79–1.36; n=2] | 2.12 ms [1.83–2.41; n=2] | 20.69 ms [15.57–25.80; n=2] |
| Latency at 10,000 msg/s | A busy API workload: 2 producers at 5,000 durable 1 KiB msg/s each. | p99 latency at the offered rate | 12.80 ms [12.07–13.54; n=2] | 1.07 ms [1.00–1.14; n=2] | 1.53 ms [0.69–2.38; n=2] | 53.74 ms [40.93–66.55; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 16.53 ms [15.29–17.77; n=2] | 6.45 ms [2.14–10.75; n=2] | 3.16 ms [1.93–4.39; n=2] | 24.16 ms [23.66–24.67; n=2] |
| Slow workers, prefetch 1 | 20 workers that each spend 2 ms per job with prefetch 1, fed 5,000 msg/s. | p99 latency at the offered rate | 10.72 ms [10.44–11.01; n=2] | 2.25 ms [1.01–3.48; n=2] | 4.96 ms [0.75–9.16; n=2] | 260.07 ms [257.89–262.25; n=2] |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 122,425 [118,163–126,687; n=2] | 8,414 [8,304–8,524; n=2] |
| Stream queue | An append-only log read by 2 consumers from the start, over AMQP 0-9-1. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 101,429 [98,823–104,035; n=2] | 109,585 [107,719–111,450; n=2] | 33,846 [33,535–34,156; n=2] |
| Priority queue | x-max-priority=10, every message at priority 5, 2 producers and 2 consumers. | delivered msg/s | 32,635 [32,588–32,682; n=2] | 112,740 [107,073–118,408; n=2] | 162,644 [161,273–164,016; n=2] | 17,859 [17,847–17,871; n=2] |
| Transactions | tx.select publishers committing every 10 messages, transactional consumers acking every 10. | delivered msg/s | 21,134 [20,235–22,033; n=2] | 35,336 [32,090–38,583; n=2] | 47,099 [33,001–61,198; n=2] | 26,927 [26,848–27,006; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 34,843 [34,829–34,858; n=2] | 95,672 [93,922–97,422; n=2] | 139,304 [123,863–154,745; n=2] | 59,746 [59,256–60,236; n=2] |
| Backlog fill and drain | A consumer outage: 300,000 durable 1 KiB messages pile up, then 4 consumers drain them. | fill / drain msg/s | 74,235 / 50,007 [50,002–50,012; n=2] | 146,492 / 150,000 [150,000–150,000; n=2] | 154,722 / 149,808 [149,616–150,000; n=2] | 66,529 / 99,960 [99,931–99,989; n=2] |

Peak container memory, 1 CPU / 1 GiB (MiB, page cache included):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 194 | 93 | 188 | 108 |
| Fire-and-forget telemetry | 254 | 186 | 88 | 25 |
| Broadcast to 20 services | 186 | 1,024 | 152 | 350 |
| 64 queues, own publisher each | 220 | 1,024 | 173 | 243 |
| 64 KiB messages | 368 | 82 | 155 | 54 |
| 1 MiB messages | 335 | 82 | 166 | 157 |
| 64 KiB messages, quorum queue | – | – | 210 | – |
| 1 MiB messages, quorum queue | – | – | 177 | – |
| Latency at 1,000 msg/s | 152 | 42 | 96 | 27 |
| Latency at 10,000 msg/s | 153 | 74 | 107 | 34 |
| 500 queues, 1,000 connections | 422 | 1,024 | 159 | 66 |
| Slow workers, prefetch 1 | 176 | 76 | 106 | 38 |
| Quorum queue | 971 | 1,024 | 124 | 304 |
| Stream queue | 1,024 | 1,024 | 1,024 | 758 |
| Priority queue | 152 | 77 | 144 | 50 |
| Transactions | 151 | 75 | 107 | 34 |
| Heavy mixed load | 234 | 1,024 | 316 | 932 |
| Backlog fill and drain | 534 | 813 | 824 | 1,016 |

Disk writes and fsyncs over the measured window, 1 CPU / 1 GiB:

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | – | – | 212.4 MB/s, 1,728 fsync/s at 0.31 ms | – |
| Fire-and-forget telemetry | – | – | 0.0 MB/s, 0 fsync/s | – |
| Broadcast to 20 services | – | – | 33.7 MB/s, 519 fsync/s at 0.29 ms | – |
| 64 queues, own publisher each | – | – | 138.7 MB/s, 2,339 fsync/s at 0.15 ms | – |
| 64 KiB messages | – | – | 863.5 MB/s, 1,944 fsync/s at 0.43 ms | – |
| 1 MiB messages | – | – | 1,058.7 MB/s, 857 fsync/s at 0.83 ms | – |
| 64 KiB messages, quorum queue | – | – | 826.0 MB/s, 1,628 fsync/s at 0.46 ms | – |
| 1 MiB messages, quorum queue | – | – | 734.1 MB/s, 525 fsync/s at 1.46 ms | – |
| Latency at 1,000 msg/s | – | – | 5.3 MB/s, 998 fsync/s at 0.26 ms | – |
| Latency at 10,000 msg/s | – | – | 32.1 MB/s, 5,152 fsync/s at 0.12 ms | – |
| 500 queues, 1,000 connections | – | – | 13.2 MB/s, 2,344 fsync/s at 0.12 ms | – |
| Slow workers, prefetch 1 | – | – | 23.2 MB/s, 4,757 fsync/s at 0.07 ms | – |
| Quorum queue | – | – | 164.4 MB/s, 1,498 fsync/s at 0.27 ms | – |
| Stream queue | – | – | 179.8 MB/s, 0 fsync/s | – |
| Priority queue | – | – | 212.2 MB/s, 2,249 fsync/s at 0.18 ms | – |
| Transactions | – | – | 74.5 MB/s, 3,371 fsync/s at 0.20 ms | – |
| Heavy mixed load | – | – | 249.3 MB/s, 1,344 fsync/s at 0.36 ms | – |
| Backlog fill and drain | – | – | 0.0 MB/s, 0 fsync/s | – |

### Scenarios, 4 CPU / 4 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 78,439 [76,724–80,154; n=2] | 197,010 [190,760–203,260; n=2] | 203,577 [203,118–204,036; n=2] | 36,624 [35,842–37,406; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 326,615 [325,030–328,199; n=2] | 175,799 [175,792–175,806; n=2] | 487,371 [486,801–487,942; n=2] | 83,247 [82,634–83,860; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 161,952 [161,576–162,329; n=2] | 183,530 [182,793–184,268; n=2] | 532,422 [528,445–536,399; n=2] | 84,941 [83,473–86,409; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 139,264 [137,458–141,070; n=2] | 123,730 [123,030–124,430; n=2] | 290,351 [283,453–297,249; n=2] | 48,245 [48,041–48,449; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 3.29 ms [3.01–3.56; n=2] | 2.56 ms [1.63–3.50; n=2] | 1.68 ms [1.25–2.10; n=2] | 51.02 ms [40.79–61.26; n=2] |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | 85,414 [85,270–85,557; n=2] | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 53,629 [46,173–61,085; n=2] | 7,749 [7,569–7,930; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 122,603 [122,138–123,069; n=2] | 252,798 [249,582–256,015; n=2] | 237,882 [223,086–252,677; n=2] | 58,668 [55,476–61,860; n=2] |

Peak container memory, 4 CPU / 4 GiB (MiB, page cache included):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 172 | 79 | 252 | 79 |
| Fire-and-forget telemetry | 238 | 230 | 187 | 22 |
| Broadcast to 20 services | 214 | 1,336 | 542 | 366 |
| 64 queues, own publisher each | 299 | 1,892 | 604 | 244 |
| 500 queues, 1,000 connections | 480 | 2,224 | 506 | 67 |
| Quorum queue | 770 | 4,096 | 288 | 288 |
| Heavy mixed load | 410 | 1,124 | 727 | 1,106 |

Disk writes and fsyncs over the measured window, 4 CPU / 4 GiB:

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | – | – | 264.3 MB/s, 2,575 fsync/s at 0.16 ms | – |
| Fire-and-forget telemetry | – | – | 0.0 MB/s, 0 fsync/s | – |
| Broadcast to 20 services | – | – | 108.0 MB/s, 3,491 fsync/s at 0.23 ms | – |
| 64 queues, own publisher each | – | – | 256.4 MB/s, 10,990 fsync/s at 0.21 ms | – |
| 500 queues, 1,000 connections | – | – | 20.8 MB/s, 4,205 fsync/s at 0.26 ms | – |
| Quorum queue | – | – | 144.6 MB/s, 65 fsync/s at 6.75 ms | – |
| Heavy mixed load | – | – | 356.4 MB/s, 2,528 fsync/s at 1.07 ms | – |

### Connections

| Broker | Size | Held | Memory at hold |
| --- | --- | ---: | ---: |
| RabbitMQ | 1 CPU / 512m | 3,500 | 454.7 MiB |
| RabbitMQ | 2 CPU / 2g | 18,750 | 1.93 GiB |
| RabbitMQ | 4 CPU / 4g | 41,622 | 3.579 GiB |
| RabbitMQ | 4 CPU / 8g | 62,065 | 5.55 GiB |
| Rust | 1 CPU / 512m | 8,980 | 503.4 MiB |
| Rust | 2 CPU / 2g | 34,600 | 1.876 GiB |
| Rust | 4 CPU / 4g | 67,086 | 3.634 GiB |
| Rust | 4 CPU / 8g | 95,000 | 5.126 GiB |
| Bun | 1 CPU / 512m | 42,187 | 370.3 MiB |
| Bun | 2 CPU / 2g | 116,434 | 1.266 GiB |
| Bun | 4 CPU / 4g | 249,768 | 2.932 GiB |
| Bun | 4 CPU / 8g | 160,000 | 1.959 GiB |
| PHP | 1 CPU / 512m | 28,000 | 210.9 MiB |
| PHP | 2 CPU / 2g | 40,000 | 296.4 MiB |
| PHP | 4 CPU / 4g | 40,000 | 298.6 MiB |
| PHP | 4 CPU / 8g | 20,000 | 159.5 MiB |
