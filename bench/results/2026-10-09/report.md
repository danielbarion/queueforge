### Paced

#### 128 confirms in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 11,545.89 | 32000 | 3.84 | 11,855.48 | 32000 |
| Rust | 21,032.90 | 64000 | 1.75 | 23,993.55 | 96000 |
| Bun | 20,357.20 | 64000 | 0.61 | 24,306.63 | 96000 |
| PHP | 7,004.91 | 16000 | 9.84 | 11,124.00 | 32000 |

#### One confirm in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 1,827.86 | 4000 | 0.37 | 2,273.10 | 6400 |
| Rust | 2,086.78 | 8000 | 0.28 | 197.87 | 800 |
| Bun | 2,779.87 | 8000 | 0.19 | 3,678.05 | 12800 |
| PHP | 1,146.85 | 4000 | 0.42 | 2,188.24 | 6400 |

### Load

| Container | Broker | single confirm/s | single consume/s | shared confirm/s | shared consume/s | spread confirm/s | spread consume/s | spread MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB | RabbitMQ | 53,568.0 | 53,567.4 | 41,984.0 | 41,984.0 | 45,760.2 | 45,789.2 | 190 |
| 1 CPU / 512 MiB | Rust | 175,360.1 | 175,360.1 | 59,836.4 | 34,719.8 | 157,075.1 | 157,219.5 | 368 |
| 1 CPU / 512 MiB | Bun | 205,140.0 | 205,140.0 | 162,848.0 | 162,824.0 | 146,304.0 | 146,304.0 | 159 |
| 1 CPU / 512 MiB | PHP | 11,232.0 | 11,232.0 | 51,401.9 | 51,278.2 | 81,285.0 | 81,383.0 | 207 |
| 1 CPU / 1 GiB | RabbitMQ | 58,114.6 | 58,129.6 | 45,056.0 | 45,198.1 | 44,635.6 | 44,616.4 | 196 |
| 1 CPU / 1 GiB | Rust | 174,773.5 | 174,779.1 | 116,388.6 | 68,803.4 | 153,490.6 | 153,455.0 | 367 |
| 1 CPU / 1 GiB | Bun | 180,636.0 | 180,644.0 | 161,520.0 | 161,504.0 | 205,584.0 | 205,600.0 | 157 |
| 1 CPU / 1 GiB | PHP | 11,152.0 | 11,152.0 | 49,688.0 | 49,835.0 | 77,007.4 | 76,805.5 | 181 |
| 2 CPU / 2 GiB | RabbitMQ | 78,934.0 | 78,943.9 | 74,752.0 | 75,338.5 | 89,170.8 | 89,523.0 | 245 |
| 2 CPU / 2 GiB | Rust | 189,818.4 | 189,817.9 | 189,525.0 | 93,801.8 | 250,179.1 | 250,176.8 | 502 |
| 2 CPU / 2 GiB | Bun | 170,456.0 | 170,452.0 | 144,472.1 | 144,464.1 | 277,712.0 | 277,688.0 | 248 |
| 2 CPU / 2 GiB | PHP | 32,048.0 | 32,048.0 | c17_404_NOT_FOUND_-_no_queue__q0_ |  | 170,484.2 | 170,507.5 | 417 |
| 4 CPU / 4 GiB | RabbitMQ | 83,586.1 | 83,592.2 | 80,154.2 | 80,820.8 | 172,532.1 | 172,583.6 | 274 |
| 4 CPU / 4 GiB | Rust | 177,606.4 | 177,595.8 | 244,256.8 | 120,327.6 | 381,878.9 | 381,938.6 | 700 |
| 4 CPU / 4 GiB | Bun | 189,068.0 | 189,068.0 | 155,968.0 | 155,992.0 | 411,904.0 | 411,888.0 | 398 |
| 4 CPU / 4 GiB | PHP | 31,296.0 | 31,296.0 | 47,546.0 | 47,576.4 | 288,669.0 | 288,512.6 | 332 |
| 4 CPU / 8 GiB | RabbitMQ | 83,574.6 | 83,571.1 | 82,234.2 | 82,252.1 | 173,112.8 | 173,110.0 | 241 |
| 4 CPU / 8 GiB | Rust | 195,854.9 | 195,861.5 | 239,698.4 | 123,188.1 | 399,582.0 | 399,594.9 | 792 |
| 4 CPU / 8 GiB | Bun | 165,220.0 | 165,216.0 | 149,984.0 | 149,974.0 | 461,888.0 | 461,936.0 | 384 |
| 4 CPU / 8 GiB | PHP | c2_404_NOT_FOUND_-_no_queue__q0_ |  | 50,116.0 | 50,116.0 | 292,520.1 | 292,480.2 | 334 |

### Scenarios, 1 CPU / 1 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 41,976 | 124,898 | 92,563 | 35,518 |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 94,088 | 88,042 | 69,776 | 88,917 |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 42,192 | 117,624 | 214,913 | 75,801 |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 34,480 | 73,944 | 92,925 | 51,665 |
| 64 KiB messages | Documents and images: 2 producers of durable 64 KiB bodies, scored in MB/s. | delivered MB/s | 634.3 MB/s | 255.0 MB/s | 312.5 MB/s | 245.9 MB/s |
| 1 MiB messages | Large payloads: 1 producer of durable 1 MiB bodies, 4 confirms in flight, scored in MB/s. | delivered MB/s | 1,240.5 MB/s | 390.3 MB/s | 362.8 MB/s | failed (oom) |
| Latency at 1,000 msg/s | A steady API workload: 1,000 durable 1 KiB msg/s with confirms. Scored on end-to-end latency. | p99 latency at the offered rate | 4.23 ms | 1.46 ms | 6.50 ms | 13.45 ms |
| Latency at 10,000 msg/s | A busy API workload: 2 producers at 5,000 durable 1 KiB msg/s each. | p99 latency at the offered rate | 14.91 ms | 1.64 ms | 14.69 ms | 28.87 ms |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 13.35 ms | 7.43 ms | 38.42 ms | 33.97 ms |
| Slow workers, prefetch 1 | 20 workers that each spend 2 ms per job with prefetch 1, fed 5,000 msg/s. | p99 latency at the offered rate | 12.53 ms | 0.67 ms | 22.82 ms | 37.66 ms |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | 26,692 | 76,544 | 88,158 | 589 |
| Stream queue | An append-only log read by 2 consumers from the start, over AMQP 0-9-1. | delivered msg/s | 26,688 | 10,442 | 27,171 | 791 |
| Priority queue | x-max-priority=10, every message at priority 5, 2 producers and 2 consumers. | delivered msg/s | 31,130 | 113,478 | 79,705 | 16,991 |
| Transactions | tx.select publishers committing every 10 messages, transactional consumers acking every 10. | delivered msg/s | 19,233 | 199 | 17,704 | 21,550 |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 29,258 | 90,035 | 96,051 | 61,762 |
| Backlog fill and drain | A consumer outage: 300,000 durable 1 KiB messages pile up, then 4 consumers drain them. | fill / drain msg/s | 71,864 / 50,009 | 145,338 / 149,850 | 29,988 / 74,814 | 60,000 / 811 (incomplete: sent 300000 got 76200) |

Peak memory, 1 CPU / 1 GiB (anonymous / cgroup total MiB):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 125 / 161 | 15 / 83 | 798 / 840 | 20 / 88 |
| Fire-and-forget telemetry | 195 / 256 | 66 / 189 | 790 / 909 | 20 / 31 |
| Broadcast to 20 services | 151 / 193 | 18 / 1024 | 88 / 145 | 26 / 367 |
| 64 queues, own publisher each | 200 / 236 | 34 / 1024 | 96 / 126 | 28 / 255 |
| 64 KiB messages | 268 / 366 | 16 / 88 | 960 / 1024 | 18 / 61 |
| 1 MiB messages | 241 / 324 | 20 / 97 | 204 / 438 | 993 / 1022 |
| Latency at 1,000 msg/s | 127 / 159 | 14 / 48 | 54 / 84 | 18 / 35 |
| Latency at 10,000 msg/s | 127 / 159 | 12 / 83 | 68 / 91 | 16 / 35 |
| 500 queues, 1,000 connections | 392 / 441 | 111 / 1024 | 91 / 127 | 50 / 73 |
| Slow workers, prefetch 1 | 128 / 161 | 18 / 85 | 70 / 95 | 22 / 38 |
| Quorum queue | 573 / 954 | 794 / 1024 | 569 / 595 | 38 / 84 |
| Stream queue | 126 / 1024 | 366 / 634 | 241 / 1024 | 42 / 70 |
| Priority queue | 127 / 158 | 15 / 83 | 90 / 110 | 20 / 58 |
| Transactions | 126 / 157 | 14 / 23 | 295 / 1024 | 18 / 38 |
| Heavy mixed load | 214 / 248 | 68 / 1024 | 314 / 1024 | 53 / 1024 |
| Backlog fill and drain | 169 / 545 | 473 / 820 | 556 / 1024 | 763 / 1024 |

### Scenarios, 4 CPU / 4 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 68,452 | 181,079 | 101,532 | 10,050 |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 274,151 | 143,620 | 97,339 | 50,305 |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 144,495 | 189,063 | 28,641 | 7,269 |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 132,621 | 125,354 | 208,309 | 36,746 |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 2.17 ms | 0.80 ms | 258.10 ms | failed (client-exit-124) |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | 89,750 | 73,562 | 12,034 | failed (no-delivery) |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 123,802 | 240,587 | 234,593 | 2,802 |

Peak memory, 4 CPU / 4 GiB (anonymous / cgroup total MiB):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 146 / 180 | 18 / 85 | 2147 / 2286 | 57 / 106 |
| Fire-and-forget telemetry | 203 / 268 | 100 / 223 | 1015 / 1158 | 378 / 446 |
| Broadcast to 20 services | 176 / 216 | 21 / 1343 | 1002 / 2949 | 61 / 169 |
| 64 queues, own publisher each | 247 / 307 | 39 / 2149 | 345 / 404 | 65 / 279 |
| 500 queues, 1,000 connections | 413 / 461 | 144 / 2231 | 344 / 498 | 67 / 107 |
| Quorum queue | 337 / 810 | 3082 / 4096 | 2209 / 2281 | 2340 / 2405 |
| Heavy mixed load | 323 / 506 | 143 / 1139 | 2098 / 3951 | 105 / 237 |

### Connections

| Broker | Size | Held | Memory at hold |
| --- | --- | ---: | ---: |
| RabbitMQ | 1 CPU / 512m | 3,500 | 440.5MiB |
| RabbitMQ | 2 CPU / 2g | 20,000 | 1.862GiB |
| RabbitMQ | 4 CPU / 4g | 41,083 | 3.801GiB |
| RabbitMQ | 4 CPU / 8g | 64,370 | 5.676GiB |
| Rust | 1 CPU / 512m | 8,980 | 502.2MiB |
| Rust | 2 CPU / 2g | 34,600 | 1.873GiB |
| Rust | 4 CPU / 4g | 67,208 | 3.639GiB |
| Rust | 4 CPU / 8g | 95,000 | 5.121GiB |
| Bun | 1 CPU / 512m | 42,965 | 426.7MiB |
| Bun | 2 CPU / 2g | 117,420 | 1.506GiB |
| Bun | 4 CPU / 4g | 247,316 | 3.03GiB |
| Bun | 4 CPU / 8g | 260,000 | 3.121GiB |
| PHP | 1 CPU / 512m | 1,000 | 20.99MiB |
| PHP | 2 CPU / 2g | 1,250 | 52.99MiB |
| PHP | 4 CPU / 4g | 2,500 | 89.01MiB |
| PHP | 4 CPU / 8g | 2,500 | 88.89MiB |
