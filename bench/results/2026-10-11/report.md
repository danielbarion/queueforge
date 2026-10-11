### Paced

#### 128 confirms in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 11,502.52 | 32000 | 3.94 | 11,538.30 | 32000 |
| Rust | 20,825.92 | 64000 | 1.80 | 24,621.59 | 96000 |
| Bun | 20,375.59 | 64000 | 1.10 | 23,325.64 | 96000 |
| PHP | 6,599.79 | 16000 | 10.29 | 11,403.55 | 32000 |

#### One confirm in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 1,823.69 | 4000 | 0.36 | 2,289.92 | 6400 |
| Rust | 2,032.03 | 8000 | 0.27 | 3,454.48 | 12800 |
| Bun | 2,687.11 | 8000 | 0.20 | 3,344.90 | 12800 |
| PHP | 1,781.62 | 4000 | 0.32 | 2,291.39 | 6400 |

### Load

| Container | Broker | single confirm/s | single consume/s | shared confirm/s | shared consume/s | spread confirm/s | spread consume/s | spread MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB | RabbitMQ | 57,041.2 | 57,035.6 | 41,984.0 | 42,694.0 | 44,294.2 | 44,301.4 | 190 |
| 1 CPU / 512 MiB | Rust | 169,909.4 | 169,914.9 | 108,121.9 | 63,683.4 | 151,411.5 | 151,385.5 | 368 |
| 1 CPU / 512 MiB | Bun | 192,388.0 | 192,388.0 | 165,168.0 | 165,208.0 | 211,872.0 | 211,928.0 | 202 |
| 1 CPU / 512 MiB | PHP | 10,272.0 | 10,272.0 | 80,528.0 | 80,528.0 | 82,568.8 | 82,568.8 | 210 |
| 1 CPU / 1 GiB | RabbitMQ | 57,601.1 | 57,587.6 | 44,032.0 | 43,681.8 | 44,412.5 | 44,444.5 | 194 |
| 1 CPU / 1 GiB | Rust | 173,158.9 | 173,153.6 | 106,040.1 | 66,281.6 | 152,380.6 | 152,375.6 | 368 |
| 1 CPU / 1 GiB | Bun | 197,688.0 | 197,672.0 | 148,464.0 | 149,000.0 | 184,464.0 | 184,904.0 | 212 |
| 1 CPU / 1 GiB | PHP | 10,352.0 | 10,352.0 | 77,528.1 | 77,537.9 | 86,845.5 | 87,209.8 | 214 |
| 2 CPU / 2 GiB | RabbitMQ | 75,097.2 | 75,103.2 | 73,728.0 | 73,516.9 | 86,539.4 | 86,501.1 | 221 |
| 2 CPU / 2 GiB | Rust | 202,128.1 | 202,124.0 | 193,175.1 | 93,385.8 | 274,706.6 | 274,661.0 | 565 |
| 2 CPU / 2 GiB | Bun | 207,380.0 | 207,380.0 | 172,916.0 | 172,796.0 | 329,948.0 | 330,148.0 | 297 |
| 2 CPU / 2 GiB | PHP | 10,144.0 | 10,144.0 | 87,382.0 | 87,382.0 | 88,557.2 | 88,557.2 | 207 |
| 4 CPU / 4 GiB | RabbitMQ | 80,289.0 | 80,284.0 | 76,978.6 | 76,258.5 | 167,793.8 | 167,964.6 | 287 |
| 4 CPU / 4 GiB | Rust | 200,590.4 | 200,592.8 | 240,802.8 | 118,389.0 | 406,018.0 | 406,040.6 | 833 |
| 4 CPU / 4 GiB | Bun | 209,088.0 | 209,096.0 | 171,984.0 | 171,792.0 | 585,756.0 | 586,020.0 | 534 |
| 4 CPU / 4 GiB | PHP | 9,840.0 | 9,840.0 | 87,810.0 | 87,548.2 | 88,558.1 | 88,578.8 | 216 |
| 4 CPU / 8 GiB | RabbitMQ | 79,233.8 | 79,219.5 | 76,952.6 | 76,964.1 | 167,304.1 | 167,345.5 | 247 |
| 4 CPU / 8 GiB | Rust | 204,938.6 | 204,939.4 | 239,406.4 | 123,882.0 | 390,902.2 | 391,099.1 | 714 |
| 4 CPU / 8 GiB | Bun | 227,064.0 | 227,064.0 | 176,656.0 | 176,884.0 | 591,260.0 | 591,144.0 | 531 |
| 4 CPU / 8 GiB | PHP | 9,840.0 | 9,840.0 | 86,524.0 | 86,564.4 | 88,130.0 | 88,130.0 | 215 |

### Scenarios, 1 CPU / 1 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 44,423 [44,190–44,657; n=2] | 127,384 [126,525–128,244; n=2] | 117,618 [116,988–118,249; n=2] | 33,013 [32,980–33,045; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 102,552 [102,545–102,559; n=2] | 97,287 [94,053–100,520; n=2] | 450,079 [450,045–450,114; n=2] | 91,230 [90,498–91,961; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 47,742 [47,648–47,835; n=2] | 134,809 [134,316–135,302; n=2] | unscored (1/2 complete; 1 failed, 0 incomplete, 0 missed rate) | 76,247 [66,960–85,534; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 42,421 [42,126–42,716; n=2] | 91,608 [89,502–93,714; n=2] | 138,157 [137,266–139,048; n=2] | 54,315 [54,263–54,366; n=2] |
| 64 KiB messages | Documents and images: 2 producers of durable 64 KiB bodies, scored in MB/s. | delivered MB/s | 775.0 MB/s [773.2–776.8; n=2] | 255.3 MB/s [254.0–256.6; n=2] | 452.4 MB/s [447.8–456.9; n=2] | 290.9 MB/s [281.2–300.7; n=2] |
| 1 MiB messages | Large payloads: 1 producer of durable 1 MiB bodies, 4 confirms in flight, scored in MB/s. | delivered MB/s | 1,333.4 MB/s [1,320.0–1,346.8; n=2] | 566.0 MB/s [552.3–579.8; n=2] | 433.8 MB/s [433.4–434.2; n=2] | 610.0 MB/s [605.2–614.9; n=2] |
| Latency at 1,000 msg/s | A steady API workload: 1,000 durable 1 KiB msg/s with confirms. Scored on end-to-end latency. | p99 latency at the offered rate | 3.75 ms [3.22–4.27; n=2] | 2.14 ms [1.74–2.54; n=2] | 2.60 ms [2.11–3.10; n=2] | 28.23 ms [25.85–30.61; n=2] |
| Latency at 10,000 msg/s | A busy API workload: 2 producers at 5,000 durable 1 KiB msg/s each. | p99 latency at the offered rate | 14.99 ms [14.37–15.62; n=2] | 2.46 ms [1.96–2.96; n=2] | 4.04 ms [3.50–4.59; n=2] | 26.62 ms [21.75–31.48; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 10.90 ms [7.65–14.15; n=2] | 6.18 ms [4.71–7.65; n=2] | 3.20 ms [2.19–4.21; n=2] | 36.19 ms [34.70–37.67; n=2] |
| Slow workers, prefetch 1 | 20 workers that each spend 2 ms per job with prefetch 1, fed 5,000 msg/s. | p99 latency at the offered rate | 14.29 ms [14.09–14.48; n=2] | 6.44 ms [3.56–9.32; n=2] | 4.76 ms [0.59–8.94; n=2] | 35.38 ms [34.20–36.55; n=2] |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 88,427 [87,014–89,840; n=2] | 90,750 [90,227–91,273; n=2] | 8,478 [8,346–8,609; n=2] |
| Stream queue | An append-only log read by 2 consumers from the start, over AMQP 0-9-1. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 11,528 [11,504–11,552; n=2] | 24,868 [22,577–27,158; n=2] | 32,330 [32,247–32,413; n=2] |
| Priority queue | x-max-priority=10, every message at priority 5, 2 producers and 2 consumers. | delivered msg/s | 34,187 [34,134–34,239; n=2] | 124,284 [124,095–124,473; n=2] | 112,213 [110,805–113,621; n=2] | 15,906 [15,814–15,998; n=2] |
| Transactions | tx.select publishers committing every 10 messages, transactional consumers acking every 10. | delivered msg/s | 20,523 [20,002–21,045; n=2] | 41,297 [40,620–41,974; n=2] | 53,633 [50,989–56,277; n=2] | 15,191 [8,266–22,115; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 35,689 [35,575–35,803; n=2] | 98,535 [98,322–98,749; n=2] | 121,853 [121,539–122,167; n=2] | 65,431 [64,914–65,948; n=2] |
| Backlog fill and drain | A consumer outage: 300,000 durable 1 KiB messages pile up, then 4 consumers drain them. | fill / drain msg/s | 74,543 / 50,007 [50,001–50,012; n=2] | 156,186 / 149,919 [149,838–150,000; n=2] | 141,357 / 149,456 [149,241–149,672; n=2] | 64,120 / 5,884 [5,884–5,884; n=2] |

Peak memory, 1 CPU / 1 GiB (anonymous / cgroup total MiB):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 122 / 152 | 8 / 78 | 102 / 112 | 12 / 78 |
| Fire-and-forget telemetry | 194 / 255 | 65 / 186 | 59 / 95 | 12 / 22 |
| Broadcast to 20 services | 144 / 184 | 12 / 1,024 | 104 / 114 | 19 / 368 |
| 64 queues, own publisher each | 188 / 224 | 26 / 1,024 | 102 / 116 | 22 / 232 |
| 64 KiB messages | 264 / 362 | 10 / 79 | 95 / 106 | 12 / 52 |
| 1 MiB messages | 240 / 330 | 14 / 89 | 95 / 106 | 17 / 154 |
| Latency at 1,000 msg/s | 120 / 152 | 6 / 42 | 54 / 86 | 10 / 27 |
| Latency at 10,000 msg/s | 120 / 151 | 6 / 74 | 74 / 86 | 12 / 30 |
| 500 queues, 1,000 connections | 372 / 416 | 110 / 1,024 | 98 / 139 | 42 / 64 |
| Slow workers, prefetch 1 | 124 / 160 | 10 / 78 | 68 / 92 | 12 / 29 |
| Quorum queue | 667 / 1,024 | 792 / 1,024 | 604 / 618 | 32 / 306 |
| Stream queue | 120 / 1,024 | 404 / 707 | 232 / 1,024 | 251 / 728 |
| Priority queue | 120 / 152 | 8 / 78 | 98 / 108 | 11 / 49 |
| Transactions | 115 / 144 | 6 / 72 | 70 / 82 | 10 / 31 |
| Heavy mixed load | 200 / 234 | 63 / 1,024 | 288 / 333 | 51 / 1,024 |
| Backlog fill and drain | 169 / 539 | 464 / 812 | 524 / 778 | 706 / 1,012 |

### Scenarios, 4 CPU / 4 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 79,742 [79,149–80,335; n=2] | 224,839 [222,658–227,019; n=2] | 143,478 [143,040–143,916; n=2] | 32,560 [32,353–32,767; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 365,398 [364,367–366,428; n=2] | 190,163 [189,721–190,605; n=2] | 400,702 [396,624–404,780; n=2] | 92,435 [92,289–92,582; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 182,490 [182,176–182,804; n=2] | 211,845 [210,683–213,007; n=2] | 157,279 [152,907–161,651; n=2] | 79,375 [67,814–90,936; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 156,559 [155,072–158,046; n=2] | 124,678 [123,519–125,838; n=2] | 282,647 [279,011–286,284; n=2] | 54,222 [54,199–54,246; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 3.56 ms [2.58–4.55; n=2] | 2.42 ms [1.97–2.86; n=2] | 1.81 ms [1.67–1.96; n=2] | 31.82 ms [24.68–38.97; n=2] |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | 90,123 [86,136–94,110; n=2] | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 10,022 [8,197–11,847; n=2] | 8,775 [8,756–8,794; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 139,106 [137,587–140,624; n=2] | 237,896 [213,337–262,455; n=2] | 318,865 [315,489–322,241; n=2] | 65,603 [65,095–66,111; n=2] |

Peak memory, 4 CPU / 4 GiB (anonymous / cgroup total MiB):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 138 / 170 | 12 / 78 | 208 / 323 | 12 / 80 |
| Fire-and-forget telemetry | 188 / 242 | 118 / 240 | 170 / 310 | 12 / 23 |
| Broadcast to 20 services | 174 / 210 | 14 / 1,336 | 2,198 / 4,066 | 21 / 362 |
| 64 queues, own publisher each | 238 / 310 | 36 / 1,904 | 416 / 468 | 22 / 236 |
| 500 queues, 1,000 connections | 420 / 467 | 136 / 2,220 | 345 / 482 | 43 / 66 |
| Quorum queue | 348 / 786 | 3,082 / 4,096 | 782 / 834 | 34 / 314 |
| Heavy mixed load | 342 / 430 | 153 / 1,220 | 608 / 684 | 44 / 1,065 |

### Connections

| Broker | Size | Held | Memory at hold |
| --- | --- | ---: | ---: |
| RabbitMQ | 1 CPU / 512m | 3,500 | 481.6MiB |
| RabbitMQ | 2 CPU / 2g | 18,750 | 1.831GiB |
| RabbitMQ | 4 CPU / 4g | 38,040 | 3.35GiB |
| RabbitMQ | 4 CPU / 8g | 61,562 | 5.088GiB |
| Rust | 1 CPU / 512m | 8,000 | 448.8MiB |
| Rust | 2 CPU / 2g | 34,600 | 1.875GiB |
| Rust | 4 CPU / 4g | 67,086 | 3.637GiB |
| Rust | 4 CPU / 8g | 95,000 | 5.126GiB |
| Bun | 1 CPU / 512m | 36,777 | 379.3MiB |
| Bun | 2 CPU / 2g | 123,636 | 1.609GiB |
| Bun | 4 CPU / 4g | 253,332 | 3.136GiB |
| Bun | 4 CPU / 8g | 260,000 | 3.337GiB |
| PHP | 1 CPU / 512m | 1,000 | 23.91MiB |
| PHP | 2 CPU / 2g | 1,000 | 23.74MiB |
| PHP | 4 CPU / 4g | 1,000 | 23.68MiB |
| PHP | 4 CPU / 8g | 1,000 | 23.7MiB |
