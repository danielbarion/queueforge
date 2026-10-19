### Paced

#### 128 confirms in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 9,310.34 | 32000 | 4.41 | 7,022.51 | 32000 |
| Rust | 20,941.15 | 64000 | 1.76 | 24,802.63 | 96000 |
| Bun | 18,818.83 | 48000 | 0.86 | 21,307.26 | 96000 |
| PHP | 14,228.46 | 32000 | 3.52 | 17,815.36 | 48000 |

#### One confirm in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 1,604.59 | 2000 | 0.45 | 2,202.27 | 6400 |
| Rust | 2,677.64 | 8000 | 0.22 | 3,411.51 | 12800 |
| Bun | 2,494.95 | 4000 | 0.24 | 3,470.85 | 12800 |
| PHP | 2,018.96 | 4000 | 0.31 | 2,374.51 | 6400 |

### Load

Each cell is the lower of confirm/s and consume/s, the median over repeats with
its range; a cell without a range was measured once.

| Container | Broker | single msg/s | shared msg/s | spread msg/s | spread MiB |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB | RabbitMQ | 55,632.2 | 40,959.9 | 41,172.8 | 180 |
| 1 CPU / 512 MiB | Rust | 230,074.8 [221,915–230,901; n=3] | 162,809.1 [107,115–164,003; n=3] | 176,176.2 [165,893–214,522; n=3] | 507 |
| 1 CPU / 512 MiB | Bun | 220,528.0 [151,148–275,636; n=3] | 245,328.0 [161,556–259,720; n=3] | 292,303.8 [65,209–378,136; n=3] | 270 |
| 1 CPU / 512 MiB | PHP | 49,184.0 [49,024–49,344; n=2] | 101,088.0 [98,520–102,374; n=3] | 107,810.0 [106,526–107,810; n=3] | 253 |
| 1 CPU / 1 GiB | RabbitMQ | 53,795.9 | 38,629.5 | 40,813.8 | 181 |
| 1 CPU / 1 GiB | Rust | 224,980.1 [222,576–231,067; n=3] | 145,941.9 [145,262–148,363; n=3] | 216,574.8 [216,324–219,664; n=3] | 537 |
| 1 CPU / 1 GiB | Bun | 287,496.0 [98,628–306,132; n=3] | 253,180.0 [252,128–258,840; n=3] | 188,876.0 [163,488–384,400; n=3] | 274 |
| 1 CPU / 1 GiB | PHP | 49,344.0 [49,328–49,728; n=3] | 100,231.9 [95,948–101,516; n=3] | 106,525.6 [105,671–107,382; n=3] | 260 |
| 2 CPU / 2 GiB | RabbitMQ | 67,769.0 | 65,547.4 | 81,053.1 | 199 |
| 2 CPU / 2 GiB | Rust | 254,633.1 [246,567–262,693; n=3] | 234,388.9 [226,054–238,016; n=3] | 357,188.5 [249,003–375,002; n=3] | 773 |
| 2 CPU / 2 GiB | Bun | 351,228.0 [223,504–393,840; n=3] | 181,390.4 [170,700–308,429; n=3] | 611,828.0 [180,840–675,676; n=3] | 494 |
| 2 CPU / 2 GiB | PHP | 49,488.0 [48,912–49,936; n=3] | 103,229.9 [101,946–104,088; n=3] | 108,238.0 [106,098–108,665; n=3] | 250 |
| 4 CPU / 4 GiB | RabbitMQ | 76,623.0 | 67,918.0 | 159,492.4 | 245 |
| 4 CPU / 4 GiB | Rust | 268,650.8 [256,725–274,821; n=3] | 181,545.5 [180,909–182,335; n=3] | 371,061.2 [298,705–424,776; n=3] | 793 |
| 4 CPU / 4 GiB | Bun | 345,032.0 [136,072–391,740; n=3] | 272,739.0 [230,111–282,298; n=3] | 665,377.6 [606,932–1,444,224; n=3] | 925 |
| 4 CPU / 4 GiB | PHP | 49,280.0 [48,608–49,432; n=3] | 102,373.9 [101,946–102,802; n=3] | 106,954.8 [106,526–107,382; n=3] | 260 |
| 4 CPU / 8 GiB | RabbitMQ | 77,596.2 | 65,965.4 | 156,266.4 | 228 |
| 4 CPU / 8 GiB | Rust | 255,605.0 [203,312–269,915; n=3] | 182,273.9 [125,230–183,264; n=3] | 460,905.6 [46,705–468,635; n=3] | 1086 |
| 4 CPU / 8 GiB | Bun | 206,040.0 [99,000–236,908; n=3] | 113,912.6 [97,216–130,271; n=3] | 1,059,352.0 [789,040–1,125,704; n=3] | 966 |
| 4 CPU / 8 GiB | PHP | 48,976.0 [48,432–49,392; n=3] | 103,230.0 [102,374–104,088; n=3] | 107,810.0 [105,671–108,131; n=3] | 253 |

### Disk ceiling

dd write+fdatasync in a 1 CPU / 1 GiB container on an anonymous volume.

| Written per sync | MB/s (median) | Range |
| --- | ---: | ---: |
| 4 KiB | 13.9 | 12.1–15.5 |
| 64 KiB | 215.5 | 208.5–216.7 |
| 256 KiB | 734.2 | 634.5–804.5 |
| 1 MiB | 1,386.6 | 1,269.1–1,475.3 |
| 4 MiB | 2,334.4 | 1,592.0–2,625.5 |
| 2 GiB, one sync | 1,928.6 | 1,023.1–3,484.5 |

### Scenarios, 1 CPU / 1 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 43,341 [43,074–43,608; n=2] | 165,332 [164,368–166,295; n=2] | 235,668 [226,605–244,731; n=2] | 72,602 [72,563–72,642; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 104,101 [103,745–104,457; n=2] | 254,497 [250,207–258,788; n=2] | 681,658 [661,416–701,901; n=2] | 187,007 [185,014–189,000; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 47,547 [47,106–47,988; n=2] | 172,251 [170,770–173,731; n=2] | 468,884 [467,399–470,368; n=2] | 128,599 [117,709–139,488; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 42,298 [41,914–42,682; n=2] | 117,520 [116,282–118,759; n=2] | 223,853 [215,223–232,483; n=2] | 68,675 [68,272–69,078; n=2] |
| 64 KiB messages | Documents and images: 2 producers of durable 64 KiB bodies, scored in MB/s. | delivered MB/s | 778.0 MB/s [776.9–779.1; n=2] | 1,226.8 MB/s [1,222.7–1,231.0; n=2] | 970.9 MB/s [970.4–971.4; n=2] | 276.5 MB/s [260.7–292.4; n=2] |
| 1 MiB messages | Large payloads: 1 producer of durable 1 MiB bodies, 4 confirms in flight, scored in MB/s. | delivered MB/s | 1,345.8 MB/s [1,336.2–1,355.4; n=2] | 1,316.3 MB/s [1,315.5–1,317.1; n=2] | 1,564.6 MB/s [1,497.1–1,632.1; n=2] | 676.5 MB/s [579.1–774.0; n=2] |
| 64 KiB messages, quorum queue | The 64 KiB cell on a quorum queue, which RabbitMQ also fsyncs before the confirm. | delivered MB/s | – | 1,000.2 MB/s [786.1–1,214.4; n=2] | 903.3 MB/s [830.3–976.4; n=2] | 57.8 MB/s [50.0–65.6; n=2] |
| 1 MiB messages, quorum queue | The 1 MiB cell on a quorum queue, which RabbitMQ also fsyncs before the confirm. | delivered MB/s | – | 983.5 MB/s [836.6–1,130.4; n=2] | 956.0 MB/s [955.5–956.4; n=2] | 154.8 MB/s [137.9–171.7; n=2] |
| Latency at 1,000 msg/s | A steady API workload: 1,000 durable 1 KiB msg/s with confirms. Scored on end-to-end latency. | p99 latency at the offered rate | 1.73 ms [1.30–2.16; n=2] | 1.70 ms [1.28–2.12; n=2] | 2.00 ms [1.97–2.03; n=2] | 34.48 ms [34.45–34.51; n=2] |
| Latency at 10,000 msg/s | A busy API workload: 2 producers at 5,000 durable 1 KiB msg/s each. | p99 latency at the offered rate | 12.80 ms [12.07–13.54; n=2] | 0.84 ms [0.48–1.21; n=2] | 2.42 ms [0.86–3.97; n=2] | 24.50 ms [14.58–34.42; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 16.53 ms [15.29–17.77; n=2] | 6.13 ms [6.12–6.14; n=2] | 3.70 ms [2.73–4.67; n=2] | 29.91 ms [29.38–30.43; n=2] |
| Slow workers, prefetch 1 | 20 workers that each spend 2 ms per job with prefetch 1, fed 5,000 msg/s. | p99 latency at the offered rate | 10.72 ms [10.44–11.01; n=2] | 3.58 ms [0.11–7.05; n=2] | 6.38 ms [0.54–12.23; n=2] | 40.79 ms [35.68–45.90; n=2] |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 152,497 [147,379–157,616; n=2] | 211,538 [187,452–235,624; n=2] | 40,053 [38,838–41,269; n=2] |
| Stream queue | An append-only log read by 2 consumers from the start, over AMQP 0-9-1. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 123,605 [105,450–141,760; n=2] | 130,389 [127,777–133,002; n=2] | 75,925 [75,283–76,568; n=2] |
| Priority queue | x-max-priority=10, every message at priority 5, 2 producers and 2 consumers. | delivered msg/s | 32,635 [32,588–32,682; n=2] | 165,876 [165,201–166,550; n=2] | 203,543 [184,387–222,698; n=2] | 48,509 [48,440–48,578; n=2] |
| Transactions | tx.select publishers committing every 10 messages, transactional consumers acking every 10. | delivered msg/s | 21,134 [20,235–22,033; n=2] | 45,175 [44,825–45,524; n=2] | 51,566 [36,496–66,637; n=2] | 72,716 [71,896–73,536; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 34,843 [34,829–34,858; n=2] | 121,258 [116,696–125,820; n=2] | 197,932 [183,324–212,540; n=2] | 81,786 [81,575–81,998; n=2] |
| Backlog fill and drain | A consumer outage: 300,000 durable 1 KiB messages pile up, then 4 consumers drain them. | fill / drain msg/s | 74,235 / 50,007 [50,002–50,012; n=2] | 207,991 / 223,474 [149,920–297,029; n=2] | 188,192 / 149,657 [149,620–149,694; n=2] | 111,662 / 149,719 [149,692–149,746; n=2] |

Peak container memory, 1 CPU / 1 GiB (MiB, page cache included):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 194 | 170 | 278 | 168 |
| Fire-and-forget telemetry | 254 | 167 | 92 | 27 |
| Broadcast to 20 services | 186 | 1,024 | 194 | 368 |
| 64 queues, own publisher each | 220 | 1,024 | 265 | 245 |
| 64 KiB messages | 368 | 180 | 230 | 57 |
| 1 MiB messages | 335 | 202 | 230 | 255 |
| 64 KiB messages, quorum queue | – | 176 | 284 | 1,024 |
| 1 MiB messages, quorum queue | – | 200 | 403 | 292 |
| Latency at 1,000 msg/s | 152 | 64 | 92 | 30 |
| Latency at 10,000 msg/s | 153 | 164 | 134 | 36 |
| 500 queues, 1,000 connections | 422 | 1,024 | 158 | 78 |
| Slow workers, prefetch 1 | 176 | 120 | 114 | 37 |
| Quorum queue | 971 | 171 | 216 | 1,024 |
| Stream queue | 1,024 | 1,024 | 1,024 | 1,024 |
| Priority queue | 152 | 167 | 232 | 80 |
| Transactions | 151 | 168 | 200 | 280 |
| Heavy mixed load | 234 | 1,024 | 404 | 1,022 |
| Backlog fill and drain | 534 | 850 | 817 | 720 |

Disk writes and fsyncs over the measured window, 1 CPU / 1 GiB:

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | – | 200.3 MB/s, 1,159 fsync/s at 0.35 ms | 303.6 MB/s, 2,215 fsync/s at 0.27 ms | 88.1 MB/s, 180 fsync/s at 0.57 ms |
| Fire-and-forget telemetry | – | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s |
| Broadcast to 20 services | – | 227.4 MB/s, 1,800 fsync/s at 1.31 ms | 36.8 MB/s, 581 fsync/s at 0.28 ms | 86.0 MB/s, 63 fsync/s at 1.10 ms |
| 64 queues, own publisher each | – | 186.4 MB/s, 5,904 fsync/s at 3.13 ms | 172.8 MB/s, 2,857 fsync/s at 0.15 ms | 46.2 MB/s, 53 fsync/s at 0.94 ms |
| 64 KiB messages | – | 1,227.5 MB/s, 1,108 fsync/s at 0.45 ms | 978.1 MB/s, 2,366 fsync/s at 0.34 ms | 288.7 MB/s, 2,118 fsync/s at 0.33 ms |
| 1 MiB messages | – | 1,314.8 MB/s, 790 fsync/s at 0.82 ms | 1,562.2 MB/s, 1,444 fsync/s at 0.54 ms | 668.8 MB/s, 633 fsync/s at 0.83 ms |
| 64 KiB messages, quorum queue | – | 990.9 MB/s, 885 fsync/s at 0.75 ms | 918.2 MB/s, 1,794 fsync/s at 0.45 ms | 122.3 MB/s, 388 fsync/s at 1.08 ms |
| 1 MiB messages, quorum queue | – | 1,018.4 MB/s, 600 fsync/s at 1.23 ms | 933.1 MB/s, 812 fsync/s at 0.99 ms | 350.0 MB/s, 280 fsync/s at 0.78 ms |
| Latency at 1,000 msg/s | – | 3.2 MB/s, 99 fsync/s at 0.70 ms | 5.5 MB/s, 1,017 fsync/s at 0.23 ms | 5.0 MB/s, 939 fsync/s at 0.45 ms |
| Latency at 10,000 msg/s | – | 16.1 MB/s, 99 fsync/s at 0.31 ms | 39.0 MB/s, 6,359 fsync/s at 0.09 ms | 23.6 MB/s, 2,821 fsync/s at 0.23 ms |
| 500 queues, 1,000 connections | – | 64.1 MB/s, 7,432 fsync/s at 23.41 ms | 13.5 MB/s, 2,432 fsync/s at 0.14 ms | 7.0 MB/s, 894 fsync/s at 0.26 ms |
| Slow workers, prefetch 1 | – | 7.3 MB/s, 99 fsync/s at 0.24 ms | 22.1 MB/s, 4,511 fsync/s at 0.08 ms | 18.1 MB/s, 3,584 fsync/s at 0.22 ms |
| Quorum queue | – | 181.6 MB/s, 1,089 fsync/s at 0.40 ms | 279.8 MB/s, 2,119 fsync/s at 0.29 ms | 127.2 MB/s, 171 fsync/s at 0.45 ms |
| Stream queue | – | 257.4 MB/s | 243.6 MB/s, 0 fsync/s | 47.0 MB/s, 0 fsync/s |
| Priority queue | – | 201.1 MB/s, 1,268 fsync/s at 0.20 ms | 268.2 MB/s, 2,960 fsync/s at 0.20 ms | 59.3 MB/s, 241 fsync/s at 0.46 ms |
| Transactions | – | 89.5 MB/s, 4,546 fsync/s at 0.17 ms | 78.7 MB/s, 3,459 fsync/s at 0.19 ms | 88.3 MB/s, 87 fsync/s at 0.93 ms |
| Heavy mixed load | – | 219.2 MB/s, 1,432 fsync/s at 1.81 ms | 253.6 MB/s, 1,529 fsync/s at 0.42 ms | 96.7 MB/s, 21 fsync/s at 2.59 ms |
| Backlog fill and drain | – | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s |

### Scenarios, 4 CPU / 4 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 78,439 [76,724–80,154; n=2] | 302,894 [300,818–304,970; n=2] | 262,807 [258,768–266,845; n=2] | 72,423 [72,323–72,522; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 326,615 [325,030–328,199; n=2] | 273,397 [271,883–274,911; n=2] | 690,425 [676,056–704,793; n=2] | 187,739 [187,376–188,101; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 161,952 [161,576–162,329; n=2] | 302,416 [291,763–313,070; n=2] | 497,084 [485,651–508,516; n=2] | 127,110 [123,445–130,775; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 139,264 [137,458–141,070; n=2] | 236,977 [235,674–238,279; n=2] | 266,767 [260,957–272,577; n=2] | 68,372 [67,624–69,120; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 3.29 ms [3.01–3.56; n=2] | 0.87 ms [0.64–1.10; n=2] | 1.35 ms [1.18–1.52; n=2] | 15.14 ms [14.40–15.88; n=2] |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | 85,414 [85,270–85,557; n=2] | 292,528 [290,838–294,218; n=2] | 249,766 [249,448–250,083; n=2] | 41,627 [41,555–41,699; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 122,603 [122,138–123,069; n=2] | unscored (1/2 complete; 0 failed, 0 incomplete, 1 missed rate) | 328,948 [323,747–334,149; n=2] | 78,958 [76,347–81,569; n=2] |

Peak container memory, 4 CPU / 4 GiB (MiB, page cache included):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 172 | 186 | 363 | 145 |
| Fire-and-forget telemetry | 238 | 214 | 202 | 26 |
| Broadcast to 20 services | 214 | 2,714 | 711 | 355 |
| 64 queues, own publisher each | 299 | 3,554 | 908 | 245 |
| 500 queues, 1,000 connections | 480 | 2,542 | 598 | 66 |
| Quorum queue | 770 | 188 | 340 | 1,307 |
| Heavy mixed load | 410 | 3,040 | 1,062 | 1,205 |

Disk writes and fsyncs over the measured window, 4 CPU / 4 GiB:

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | – | 367.1 MB/s, 2,196 fsync/s at 0.23 ms | 342.4 MB/s, 3,334 fsync/s at 0.15 ms | 87.9 MB/s, 180 fsync/s at 0.57 ms |
| Fire-and-forget telemetry | – | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s |
| Broadcast to 20 services | – | 308.3 MB/s, 3,067 fsync/s at 1.62 ms | 99.2 MB/s, 2,986 fsync/s at 0.27 ms | 85.5 MB/s, 63 fsync/s at 1.08 ms |
| 64 queues, own publisher each | – | 392.7 MB/s, 12,097 fsync/s at 2.06 ms | 239.8 MB/s, 10,759 fsync/s at 0.23 ms | 46.1 MB/s, 53 fsync/s at 0.88 ms |
| 500 queues, 1,000 connections | – | 84.0 MB/s, 9,848 fsync/s at 0.72 ms | 17.6 MB/s, 3,469 fsync/s at 0.19 ms | 8.2 MB/s, 1,184 fsync/s at 0.23 ms |
| Quorum queue | – | 354.8 MB/s, 2,105 fsync/s at 0.25 ms | 337.0 MB/s, 2,975 fsync/s at 0.17 ms | 130.0 MB/s, 175 fsync/s at 0.42 ms |
| Heavy mixed load | – | 460.4 MB/s, 3,476 fsync/s at 0.72 ms | 427.0 MB/s, 6,109 fsync/s at 0.27 ms | 93.0 MB/s, 20 fsync/s at 4.17 ms |

### Connections

| Broker | Size | Held | Memory at hold |
| --- | --- | ---: | ---: |
| RabbitMQ | 1 CPU / 512m | 3,500 | 454.7 MiB |
| RabbitMQ | 2 CPU / 2g | 18,750 | 1.93 GiB |
| RabbitMQ | 4 CPU / 4g | 41,622 | 3.579 GiB |
| RabbitMQ | 4 CPU / 8g | 62,065 | 5.55 GiB |
| Rust | 1 CPU / 512m | 6,860 | 468.9 MiB |
| Rust | 2 CPU / 2g | 28,524 | 1.808 GiB |
| Rust | 4 CPU / 4g | 57,412 | 3.659 GiB |
| Rust | 4 CPU / 8g | 95,000 | 5.978 GiB |
| Bun | 1 CPU / 512m | 40,616 | 358.4 MiB |
| Bun | 2 CPU / 2g | 122,199 | 1.458 GiB |
| Bun | 4 CPU / 4g | 244,714 | 3.207 GiB |
| Bun | 4 CPU / 8g | 160,000 | 2.451 GiB |
| PHP | 1 CPU / 512m | 41,871 | 318.7 MiB |
| PHP | 2 CPU / 2g | 40,000 | 311.8 MiB |
| PHP | 4 CPU / 4g | 40,000 | 309.8 MiB |
| PHP | 4 CPU / 8g | 40,000 | 307.5 MiB |
