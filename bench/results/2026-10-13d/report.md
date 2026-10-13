### Paced

#### 128 confirms in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 9,310.34 | 32000 | 4.41 | 7,022.51 | 32000 |
| Rust | 19,026.76 | 48000 | 2.07 | 23,775.86 | 96000 |
| Bun | 20,427.58 | 64000 | 0.95 | 23,864.52 | 96000 |
| PHP | 6,695.41 | 16000 | 9.90 | 10,436.78 | 32000 |

#### One confirm in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 1,604.59 | 2000 | 0.45 | 2,202.27 | 6400 |
| Rust | 1,518.57 | 1000 | 0.33 | 3,204.46 | 800 |
| Bun | 2,564.26 | 4000 | 0.22 | 3,522.12 | 12800 |
| PHP | 1,432.29 | 2000 | 0.36 | 2,142.75 | 6400 |

### Load

| Container | Broker | single confirm/s | single consume/s | shared confirm/s | shared consume/s | spread confirm/s | spread consume/s | spread MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB | RabbitMQ | 55,637.0 | 55,632.2 | 40,960.0 | 40,959.9 | 41,172.8 | 41,239.8 | 180 |
| 1 CPU / 512 MiB | Rust | 151,312.0 | 151,317.4 | 100,552.1 | 62,989.5 | 151,524.4 | 151,546.2 | 365 |
| 1 CPU / 512 MiB | Bun | 135,892.0 | 135,884.0 | 84,781.6 | 84,781.6 | 204,240.0 | 203,960.0 | 188 |
| 1 CPU / 512 MiB | PHP | 11,840.0 | 11,840.0 | 81,814.0 | 81,814.0 | 75,295.9 | 75,506.8 | 184 |
| 1 CPU / 1 GiB | RabbitMQ | 53,795.9 | 53,804.6 | 38,629.5 | 38,811.0 | 40,813.8 | 40,854.9 | 181 |
| 1 CPU / 1 GiB | Rust | 163,130.8 | 163,130.9 | 105,525.9 | 61,090.6 | 143,286.2 | 143,230.4 | 362 |
| 1 CPU / 1 GiB | Bun | 158,816.0 | 158,832.0 | 164,416.0 | 164,824.0 | 145,970.5 | 145,924.2 | 152 |
| 1 CPU / 1 GiB | PHP | 11,808.0 | 11,808.0 | 76,244.0 | 76,244.0 | 72,301.5 | 72,413.1 | 177 |
| 2 CPU / 2 GiB | RabbitMQ | 67,773.8 | 67,769.0 | 65,547.4 | 65,825.5 | 81,053.1 | 81,258.8 | 199 |
| 2 CPU / 2 GiB | Rust | 198,000.0 | 197,997.5 | 183,290.0 | 85,598.5 | 250,681.1 | 250,649.5 | 538 |
| 2 CPU / 2 GiB | Bun | 155,492.0 | 155,504.0 | 180,832.0 | 180,640.0 | 205,597.8 | 205,909.8 | 228 |
| 2 CPU / 2 GiB | PHP | 11,888.0 | 11,888.0 | 76,672.0 | 76,672.0 | 78,290.0 | 78,134.2 | 190 |
| 4 CPU / 4 GiB | RabbitMQ | 76,623.0 | 76,630.5 | 67,982.0 | 67,918.0 | 159,719.5 | 159,492.4 | 245 |
| 4 CPU / 4 GiB | Rust | 203,650.8 | 203,649.4 | 245,078.6 | 124,121.1 | 384,363.2 | 384,290.6 | 701 |
| 4 CPU / 4 GiB | Bun | 167,128.0 | 167,128.0 | 104,239.2 | 104,190.8 | 464,944.0 | 465,100.0 | 465 |
| 4 CPU / 4 GiB | PHP | 11,760.0 | 11,751.1 | 76,672.1 | 76,254.0 | 82,567.5 | 82,386.8 | 205 |
| 4 CPU / 8 GiB | RabbitMQ | 77,608.0 | 77,596.2 | 66,674.9 | 65,965.4 | 156,266.4 | 156,328.1 | 228 |
| 4 CPU / 8 GiB | Rust | 199,301.2 | 199,296.0 | 216,175.0 | 109,002.2 | 361,168.5 | 361,130.1 | 722 |
| 4 CPU / 8 GiB | Bun | 227,228.0 | 227,228.0 | 57,555.2 | 57,403.2 | 502,483.6 | 502,659.6 | 469 |
| 4 CPU / 8 GiB | PHP | 11,920.0 | 11,920.0 | 81,813.9 | 81,813.9 | 77,862.8 | 77,807.4 | 196 |

### Scenarios, 1 CPU / 1 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 43,341 [43,074–43,608; n=2] | 130,415 [129,918–130,911; n=2] | 119,728 [115,436–124,020; n=2] | 39,139 [38,774–39,505; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 104,101 [103,745–104,457; n=2] | 94,260 [94,143–94,378; n=2] | 444,446 [431,478–457,415; n=2] | 90,752 [89,844–91,659; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 47,547 [47,106–47,988; n=2] | 135,620 [134,244–136,995; n=2] | 249,193 [239,819–258,566; n=2] | 84,598 [81,360–87,836; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 42,298 [41,914–42,682; n=2] | 91,002 [87,815–94,189; n=2] | 139,685 [139,324–140,046; n=2] | 52,064 [51,253–52,874; n=2] |
| 64 KiB messages | Documents and images: 2 producers of durable 64 KiB bodies, scored in MB/s. | delivered MB/s | 778.0 MB/s [776.9–779.1; n=2] | 252.3 MB/s [252.2–252.5; n=2] | 531.9 MB/s [528.8–535.0; n=2] | 230.6 MB/s [215.2–246.1; n=2] |
| 1 MiB messages | Large payloads: 1 producer of durable 1 MiB bodies, 4 confirms in flight, scored in MB/s. | delivered MB/s | 1,345.8 MB/s [1,336.2–1,355.4; n=2] | 573.9 MB/s [554.9–592.9; n=2] | 532.1 MB/s [526.8–537.5; n=2] | 605.0 MB/s [590.1–619.8; n=2] |
| Latency at 1,000 msg/s | A steady API workload: 1,000 durable 1 KiB msg/s with confirms. Scored on end-to-end latency. | p99 latency at the offered rate | 1.73 ms [1.30–2.16; n=2] | 1.08 ms [0.79–1.36; n=2] | 1.48 ms [1.38–1.57; n=2] | 20.69 ms [15.57–25.80; n=2] |
| Latency at 10,000 msg/s | A busy API workload: 2 producers at 5,000 durable 1 KiB msg/s each. | p99 latency at the offered rate | 12.80 ms [12.07–13.54; n=2] | 1.07 ms [1.00–1.14; n=2] | 2.19 ms [1.05–3.32; n=2] | 53.74 ms [40.93–66.55; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 16.53 ms [15.29–17.77; n=2] | 6.45 ms [2.14–10.75; n=2] | 4.04 ms [2.95–5.12; n=2] | 24.16 ms [23.66–24.67; n=2] |
| Slow workers, prefetch 1 | 20 workers that each spend 2 ms per job with prefetch 1, fed 5,000 msg/s. | p99 latency at the offered rate | 10.72 ms [10.44–11.01; n=2] | 2.25 ms [1.01–3.48; n=2] | 5.60 ms [0.48–10.71; n=2] | 260.07 ms [257.89–262.25; n=2] |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 90,933 [90,104–91,763; n=2] | 8,414 [8,304–8,524; n=2] |
| Stream queue | An append-only log read by 2 consumers from the start, over AMQP 0-9-1. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 101,429 [98,823–104,035; n=2] | 82,933 [76,650–89,217; n=2] | 33,846 [33,535–34,156; n=2] |
| Priority queue | x-max-priority=10, every message at priority 5, 2 producers and 2 consumers. | delivered msg/s | 32,635 [32,588–32,682; n=2] | 112,740 [107,073–118,408; n=2] | 120,864 [113,852–127,875; n=2] | 17,859 [17,847–17,871; n=2] |
| Transactions | tx.select publishers committing every 10 messages, transactional consumers acking every 10. | delivered msg/s | 21,134 [20,235–22,033; n=2] | 35,336 [32,090–38,583; n=2] | 46,714 [35,252–58,177; n=2] | 26,927 [26,848–27,006; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 34,843 [34,829–34,858; n=2] | 95,672 [93,922–97,422; n=2] | 107,113 [92,322–121,903; n=2] | 59,746 [59,256–60,236; n=2] |
| Backlog fill and drain | A consumer outage: 300,000 durable 1 KiB messages pile up, then 4 consumers drain them. | fill / drain msg/s | 74,235 / 50,007 [50,002–50,012; n=2] | 146,492 / 150,000 [150,000–150,000; n=2] | 149,960 / 149,811 [149,700–149,922; n=2] | 66,529 / 99,960 [99,931–99,989; n=2] |

Peak container memory, 1 CPU / 1 GiB (MiB, page cache included):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 194 | 93 | 149 | 108 |
| Fire-and-forget telemetry | 254 | 186 | 91 | 25 |
| Broadcast to 20 services | 186 | 1,024 | 120 | 350 |
| 64 queues, own publisher each | 220 | 1,024 | 134 | 243 |
| 64 KiB messages | 368 | 82 | 113 | 54 |
| 1 MiB messages | 335 | 82 | 120 | 157 |
| Latency at 1,000 msg/s | 152 | 42 | 78 | 27 |
| Latency at 10,000 msg/s | 153 | 74 | 83 | 34 |
| 500 queues, 1,000 connections | 422 | 1,024 | 118 | 66 |
| Slow workers, prefetch 1 | 176 | 76 | 88 | 38 |
| Quorum queue | 971 | 1,024 | 86 | 304 |
| Stream queue | 1,024 | 1,024 | 1,024 | 758 |
| Priority queue | 152 | 77 | 106 | 50 |
| Transactions | 151 | 75 | 74 | 34 |
| Heavy mixed load | 234 | 1,024 | 324 | 932 |
| Backlog fill and drain | 534 | 813 | 818 | 1,016 |

### Scenarios, 4 CPU / 4 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 78,439 [76,724–80,154; n=2] | 197,010 [190,760–203,260; n=2] | 142,333 [140,030–144,635; n=2] | 36,624 [35,842–37,406; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 326,615 [325,030–328,199; n=2] | 175,799 [175,792–175,806; n=2] | 375,146 [366,998–383,295; n=2] | 83,247 [82,634–83,860; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 161,952 [161,576–162,329; n=2] | 183,530 [182,793–184,268; n=2] | 163,973 [162,252–165,695; n=2] | 84,941 [83,473–86,409; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 139,264 [137,458–141,070; n=2] | 123,730 [123,030–124,430; n=2] | 259,490 [230,072–288,908; n=2] | 48,245 [48,041–48,449; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 3.29 ms [3.01–3.56; n=2] | 2.56 ms [1.63–3.50; n=2] | 3.14 ms [1.77–4.52; n=2] | 51.02 ms [40.79–61.26; n=2] |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | 85,414 [85,270–85,557; n=2] | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 56,281 [53,615–58,948; n=2] | 7,749 [7,569–7,930; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 122,603 [122,138–123,069; n=2] | 252,798 [249,582–256,015; n=2] | 243,555 [165,130–321,981; n=2] | 58,668 [55,476–61,860; n=2] |

Peak container memory, 4 CPU / 4 GiB (MiB, page cache included):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 172 | 79 | 314 | 79 |
| Fire-and-forget telemetry | 238 | 230 | 298 | 22 |
| Broadcast to 20 services | 214 | 1,336 | 416 | 366 |
| 64 queues, own publisher each | 299 | 1,892 | 466 | 244 |
| 500 queues, 1,000 connections | 480 | 2,224 | 466 | 67 |
| Quorum queue | 770 | 4,096 | 344 | 288 |
| Heavy mixed load | 410 | 1,124 | 610 | 1,106 |

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
| Bun | 1 CPU / 512m | 42,187 | 371 MiB |
| Bun | 2 CPU / 2g | 122,802 | 1.392 GiB |
| Bun | 4 CPU / 4g | 259,195 | 2.957 GiB |
| Bun | 4 CPU / 8g | 260,000 | 2.967 GiB |
| PHP | 1 CPU / 512m | 28,000 | 210.9 MiB |
| PHP | 2 CPU / 2g | 40,000 | 296.4 MiB |
| PHP | 4 CPU / 4g | 40,000 | 298.6 MiB |
| PHP | 4 CPU / 8g | 20,000 | 159.5 MiB |
