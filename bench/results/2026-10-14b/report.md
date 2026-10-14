### Paced

#### 128 confirms in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 9,310.34 | 32000 | 4.41 | 7,022.51 | 32000 |
| Rust | 19,026.76 | 48000 | 2.07 | 23,775.86 | 96000 |
| Bun | 20,558.58 | 64000 | 0.92 | 23,839.33 | 96000 |
| PHP | 6,695.41 | 16000 | 9.90 | 10,436.78 | 32000 |

#### One confirm in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 1,604.59 | 2000 | 0.45 | 2,202.27 | 6400 |
| Rust | 1,518.57 | 1000 | 0.33 | 3,204.46 | 800 |
| Bun | 2,731.65 | 8000 | 0.21 | 3,450.68 | 12800 |
| PHP | 1,432.29 | 2000 | 0.36 | 2,142.75 | 6400 |

### Load

Each cell is the lower of confirm/s and consume/s, the median over repeats with
its range; a cell without a range was measured once.

| Container | Broker | single msg/s | shared msg/s | spread msg/s | spread MiB |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB | RabbitMQ | 55,632.2 | 40,959.9 | 41,172.8 | 180 |
| 1 CPU / 512 MiB | Rust | 151,312.0 | 62,989.5 | 151,524.4 | 365 |
| 1 CPU / 512 MiB | Bun | 197,904.0 [129,827–218,444; n=3] | 177,544.0 [93,013–197,092; n=3] | 237,708.0 [237,339–241,984; n=3] | 160 |
| 1 CPU / 512 MiB | PHP | 11,840.0 | 81,814.0 | 75,295.9 | 184 |
| 1 CPU / 1 GiB | RabbitMQ | 53,795.9 | 38,629.5 | 40,813.8 | 181 |
| 1 CPU / 1 GiB | Rust | 163,130.8 | 61,090.6 | 143,230.4 | 362 |
| 1 CPU / 1 GiB | Bun | 218,460.0 [138,636–236,996; n=3] | 150,736.4 [140,551–202,552; n=3] | 239,680.0 [191,622–251,536; n=3] | 164 |
| 1 CPU / 1 GiB | PHP | 11,808.0 | 76,244.0 | 72,301.5 | 177 |
| 2 CPU / 2 GiB | RabbitMQ | 67,769.0 | 65,547.4 | 81,053.1 | 199 |
| 2 CPU / 2 GiB | Rust | 197,997.5 | 85,598.5 | 250,649.5 | 538 |
| 2 CPU / 2 GiB | Bun | 275,196.0 [147,124–277,536; n=3] | 217,460.0 [180,354–220,968; n=3] | 381,604.0 [273,276–441,572; n=3] | 248 |
| 2 CPU / 2 GiB | PHP | 11,888.0 | 76,672.0 | 78,134.2 | 190 |
| 4 CPU / 4 GiB | RabbitMQ | 76,623.0 | 67,918.0 | 159,492.4 | 245 |
| 4 CPU / 4 GiB | Rust | 203,649.4 | 124,121.1 | 384,290.6 | 701 |
| 4 CPU / 4 GiB | Bun | 180,100.0 [43,152–294,384; n=3] | 210,415.6 [95,260–231,024; n=3] | 522,317.8 [452,150–784,248; n=3] | 371 |
| 4 CPU / 4 GiB | PHP | 11,751.1 | 76,254.0 | 82,386.8 | 205 |
| 4 CPU / 8 GiB | RabbitMQ | 77,596.2 | 65,965.4 | 156,266.4 | 228 |
| 4 CPU / 8 GiB | Rust | 199,296.0 | 109,002.2 | 361,130.1 | 722 |
| 4 CPU / 8 GiB | Bun | 262,524.0 [103,676–290,252; n=3] | 216,304.0 [149,982–228,872; n=3] | 475,899.0 [286,873–777,752; n=3] | 386 |
| 4 CPU / 8 GiB | PHP | 11,920.0 | 81,813.9 | 77,807.4 | 196 |

### Disk ceiling

dd write+fdatasync in a 1 CPU / 1 GiB container on an anonymous volume.

| Written per sync | MB/s (median) | Range |
| --- | ---: | ---: |
| 4 KiB | 18.6 | 17.5–18.8 |
| 64 KiB | 275.8 | 228.3–291.6 |
| 256 KiB | 746.7 | 654.6–829.8 |
| 1 MiB | 1,742.8 | 1,422.5–1,836.6 |
| 4 MiB | 2,272.5 | 1,814.7–2,311.9 |
| 2 GiB, one sync | 2,397.8 | 2,062.4–3,101.1 |

### Scenarios, 1 CPU / 1 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 43,341 [43,074–43,608; n=2] | 130,415 [129,918–130,911; n=2] | 142,973 [140,816–145,130; n=2] | 39,139 [38,774–39,505; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 104,101 [103,745–104,457; n=2] | 94,260 [94,143–94,378; n=2] | 544,774 [544,304–545,244; n=2] | 90,752 [89,844–91,659; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 47,547 [47,106–47,988; n=2] | 135,620 [134,244–136,995; n=2] | 399,446 [397,708–401,184; n=2] | 84,598 [81,360–87,836; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 42,298 [41,914–42,682; n=2] | 91,002 [87,815–94,189; n=2] | 155,177 [152,510–157,844; n=2] | 52,064 [51,253–52,874; n=2] |
| 64 KiB messages | Documents and images: 2 producers of durable 64 KiB bodies, scored in MB/s. | delivered MB/s | 778.0 MB/s [776.9–779.1; n=2] | 252.3 MB/s [252.2–252.5; n=2] | 526.3 MB/s [513.5–539.1; n=2] | 230.6 MB/s [215.2–246.1; n=2] |
| 1 MiB messages | Large payloads: 1 producer of durable 1 MiB bodies, 4 confirms in flight, scored in MB/s. | delivered MB/s | 1,345.8 MB/s [1,336.2–1,355.4; n=2] | 573.9 MB/s [554.9–592.9; n=2] | 547.5 MB/s [546.2–548.9; n=2] | 605.0 MB/s [590.1–619.8; n=2] |
| Latency at 1,000 msg/s | A steady API workload: 1,000 durable 1 KiB msg/s with confirms. Scored on end-to-end latency. | p99 latency at the offered rate | 1.73 ms [1.30–2.16; n=2] | 1.08 ms [0.79–1.36; n=2] | 2.00 ms [1.74–2.25; n=2] | 20.69 ms [15.57–25.80; n=2] |
| Latency at 10,000 msg/s | A busy API workload: 2 producers at 5,000 durable 1 KiB msg/s each. | p99 latency at the offered rate | 12.80 ms [12.07–13.54; n=2] | 1.07 ms [1.00–1.14; n=2] | 1.99 ms [1.10–2.87; n=2] | 53.74 ms [40.93–66.55; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 16.53 ms [15.29–17.77; n=2] | 6.45 ms [2.14–10.75; n=2] | 3.68 ms [1.40–5.97; n=2] | 24.16 ms [23.66–24.67; n=2] |
| Slow workers, prefetch 1 | 20 workers that each spend 2 ms per job with prefetch 1, fed 5,000 msg/s. | p99 latency at the offered rate | 10.72 ms [10.44–11.01; n=2] | 2.25 ms [1.01–3.48; n=2] | 12.25 ms [9.68–14.83; n=2] | 260.07 ms [257.89–262.25; n=2] |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 101,988 [101,532–102,444; n=2] | 8,414 [8,304–8,524; n=2] |
| Stream queue | An append-only log read by 2 consumers from the start, over AMQP 0-9-1. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 101,429 [98,823–104,035; n=2] | 110,721 [109,262–112,179; n=2] | 33,846 [33,535–34,156; n=2] |
| Priority queue | x-max-priority=10, every message at priority 5, 2 producers and 2 consumers. | delivered msg/s | 32,635 [32,588–32,682; n=2] | 112,740 [107,073–118,408; n=2] | 136,410 [134,788–138,032; n=2] | 17,859 [17,847–17,871; n=2] |
| Transactions | tx.select publishers committing every 10 messages, transactional consumers acking every 10. | delivered msg/s | 21,134 [20,235–22,033; n=2] | 35,336 [32,090–38,583; n=2] | 57,323 [53,084–61,562; n=2] | 26,927 [26,848–27,006; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 34,843 [34,829–34,858; n=2] | 95,672 [93,922–97,422; n=2] | 144,316 [141,412–147,220; n=2] | 59,746 [59,256–60,236; n=2] |
| Backlog fill and drain | A consumer outage: 300,000 durable 1 KiB messages pile up, then 4 consumers drain them. | fill / drain msg/s | 74,235 / 50,007 [50,002–50,012; n=2] | 146,492 / 150,000 [150,000–150,000; n=2] | 161,239 / 149,526 [149,386–149,664; n=2] | 66,529 / 99,960 [99,931–99,989; n=2] |

Peak container memory, 1 CPU / 1 GiB (MiB, page cache included):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 194 | 93 | 150 | 108 |
| Fire-and-forget telemetry | 254 | 186 | 86 | 25 |
| Broadcast to 20 services | 186 | 1,024 | 114 | 350 |
| 64 queues, own publisher each | 220 | 1,024 | 137 | 243 |
| 64 KiB messages | 368 | 82 | 118 | 54 |
| 1 MiB messages | 335 | 82 | 114 | 157 |
| Latency at 1,000 msg/s | 152 | 42 | 77 | 27 |
| Latency at 10,000 msg/s | 153 | 74 | 80 | 34 |
| 500 queues, 1,000 connections | 422 | 1,024 | 118 | 66 |
| Slow workers, prefetch 1 | 176 | 76 | 86 | 38 |
| Quorum queue | 971 | 1,024 | 130 | 304 |
| Stream queue | 1,024 | 1,024 | 1,024 | 758 |
| Priority queue | 152 | 77 | 104 | 50 |
| Transactions | 151 | 75 | 72 | 34 |
| Heavy mixed load | 234 | 1,024 | 286 | 932 |
| Backlog fill and drain | 534 | 813 | 789 | 1,016 |

Disk writes and fsyncs over the measured window, 1 CPU / 1 GiB:

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | – | – | – | – |
| Fire-and-forget telemetry | – | – | – | – |
| Broadcast to 20 services | – | – | – | – |
| 64 queues, own publisher each | – | – | – | – |
| 64 KiB messages | – | – | – | – |
| 1 MiB messages | – | – | – | – |
| Latency at 1,000 msg/s | – | – | – | – |
| Latency at 10,000 msg/s | – | – | – | – |
| 500 queues, 1,000 connections | – | – | – | – |
| Slow workers, prefetch 1 | – | – | – | – |
| Quorum queue | – | – | – | – |
| Stream queue | – | – | – | – |
| Priority queue | – | – | – | – |
| Transactions | – | – | – | – |
| Heavy mixed load | – | – | – | – |
| Backlog fill and drain | – | – | – | – |

### Scenarios, 4 CPU / 4 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 78,439 [76,724–80,154; n=2] | 197,010 [190,760–203,260; n=2] | 165,339 [164,285–166,393; n=2] | 36,624 [35,842–37,406; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 326,615 [325,030–328,199; n=2] | 175,799 [175,792–175,806; n=2] | 479,113 [476,697–481,528; n=2] | 83,247 [82,634–83,860; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 161,952 [161,576–162,329; n=2] | 183,530 [182,793–184,268; n=2] | 490,062 [486,579–493,544; n=2] | 84,941 [83,473–86,409; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 139,264 [137,458–141,070; n=2] | 123,730 [123,030–124,430; n=2] | 295,157 [292,080–298,234; n=2] | 48,245 [48,041–48,449; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 3.29 ms [3.01–3.56; n=2] | 2.56 ms [1.63–3.50; n=2] | 1.94 ms [0.72–3.17; n=2] | 51.02 ms [40.79–61.26; n=2] |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | 85,414 [85,270–85,557; n=2] | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 83,474 [82,416–84,531; n=2] | 7,749 [7,569–7,930; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 122,603 [122,138–123,069; n=2] | 252,798 [249,582–256,015; n=2] | 328,396 [325,911–330,880; n=2] | 58,668 [55,476–61,860; n=2] |

Peak container memory, 4 CPU / 4 GiB (MiB, page cache included):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 172 | 79 | 208 | 79 |
| Fire-and-forget telemetry | 238 | 230 | 190 | 22 |
| Broadcast to 20 services | 214 | 1,336 | 422 | 366 |
| 64 queues, own publisher each | 299 | 1,892 | 472 | 244 |
| 500 queues, 1,000 connections | 480 | 2,224 | 451 | 67 |
| Quorum queue | 770 | 4,096 | 248 | 288 |
| Heavy mixed load | 410 | 1,124 | 592 | 1,106 |

Disk writes and fsyncs over the measured window, 4 CPU / 4 GiB:

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | – | – | – | – |
| Fire-and-forget telemetry | – | – | – | – |
| Broadcast to 20 services | – | – | – | – |
| 64 queues, own publisher each | – | – | – | – |
| 500 queues, 1,000 connections | – | – | – | – |
| Quorum queue | – | – | – | – |
| Heavy mixed load | – | – | – | – |

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
| Bun | 1 CPU / 512m | 42,187 | 372.7 MiB |
| Bun | 2 CPU / 2g | 120,402 | 1.39 GiB |
| Bun | 4 CPU / 4g | 247,534 | 2.851 GiB |
| Bun | 4 CPU / 8g | 260,000 | 3.072 GiB |
| PHP | 1 CPU / 512m | 28,000 | 210.9 MiB |
| PHP | 2 CPU / 2g | 40,000 | 296.4 MiB |
| PHP | 4 CPU / 4g | 40,000 | 298.6 MiB |
| PHP | 4 CPU / 8g | 20,000 | 159.5 MiB |
