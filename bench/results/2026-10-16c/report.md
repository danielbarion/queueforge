### Paced

#### 128 confirms in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 9,310.34 | 32000 | 4.41 | 7,022.51 | 32000 |
| Rust | 20,778.45 | 64000 | 1.78 | 24,679.52 | 96000 |
| Bun | 20,279.40 | 64000 | 0.76 | 23,935.41 | 96000 |
| PHP | 12,391.87 | 32000 | 4.27 | 16,041.45 | 48000 |

#### One confirm in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 1,604.59 | 2000 | 0.45 | 2,202.27 | 6400 |
| Rust | 2,726.21 | 8000 | 0.22 | 3,400.70 | 12800 |
| Bun | 2,038.78 | 8000 | 0.28 | 3,411.68 | 12800 |
| PHP | 1,848.26 | 4000 | 0.35 | 2,363.57 | 6400 |

### Load

Each cell is the lower of confirm/s and consume/s, the median over repeats with
its range; a cell without a range was measured once.

| Container | Broker | single msg/s | shared msg/s | spread msg/s | spread MiB |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB | RabbitMQ | 55,632.2 | 40,959.9 | 41,172.8 | 180 |
| 1 CPU / 512 MiB | Rust | 191,082.4 [143,349–230,054; n=3] | 163,042.4 [137,819–167,960; n=3] | 215,546.8 [215,013–221,582; n=3] | 512 |
| 1 CPU / 512 MiB | Bun | 144,768.0 [59,228–298,524; n=3] | 264,212.0 [199,560–266,908; n=3] | 324,240.0 [222,940–359,584; n=3] | 270 |
| 1 CPU / 512 MiB | PHP | 33,840.0 [28,000–45,920; n=3] | 100,231.9 [98,090–101,946; n=3] | 106,525.5 [103,104–106,954; n=3] | 252 |
| 1 CPU / 1 GiB | RabbitMQ | 53,795.9 | 38,629.5 | 40,813.8 | 181 |
| 1 CPU / 1 GiB | Rust | 228,224.1 [225,259–228,720; n=3] | 149,375.8 [147,880–150,616; n=3] | 218,282.0 [196,233–222,480; n=3] | 550 |
| 1 CPU / 1 GiB | Bun | 291,112.0 [229,324–295,520; n=3] | 238,116.0 [187,328–269,920; n=3] | 362,313.5 [327,828–375,264; n=3] | 270 |
| 1 CPU / 1 GiB | PHP | 47,424.0 [46,912–47,728; n=3] | 99,374.0 [98,090–102,374; n=3] | 106,098.2 [103,959–108,665; n=3] | 254 |
| 2 CPU / 2 GiB | RabbitMQ | 67,769.0 | 65,547.4 | 81,053.1 | 199 |
| 2 CPU / 2 GiB | Rust | 254,759.1 [192,224–261,296; n=3] | 226,596.5 [0–233,171; n=3] | 193,331.8 [16,232–373,417; n=3] | 740 |
| 2 CPU / 2 GiB | Bun | 242,156.0 [111,696–386,788; n=3] | 254,351.2 [169,847–282,580; n=3] | 556,804.0 [355,612–655,560; n=3] | 506 |
| 2 CPU / 2 GiB | PHP | 46,464.0 [44,336–46,752; n=3] | 101,089.9 [98,946–101,518; n=3] | 107,381.9 [105,670–107,382; n=3] | 258 |
| 4 CPU / 4 GiB | RabbitMQ | 76,623.0 | 67,918.0 | 159,492.4 | 245 |
| 4 CPU / 4 GiB | Rust | 213,445.4 [106,574–271,154; n=3] | 182,838.6 [108,845–192,856; n=3] | 329,646.2 [301,367–529,719; n=3] | 1095 |
| 4 CPU / 4 GiB | Bun | 250,712.0 [175,764–277,260; n=3] | 224,560.0 [139,020–291,664; n=3] | 764,092.0 [445,592–1,076,770; n=3] | 957 |
| 4 CPU / 4 GiB | PHP | 43,728.0 [36,816–46,512; n=3] | 101,517.6 [98,518–102,802; n=3] | 108,237.8 [105,243–109,093; n=3] | 251 |
| 4 CPU / 8 GiB | RabbitMQ | 77,596.2 | 65,965.4 | 156,266.4 | 228 |
| 4 CPU / 8 GiB | Rust | 239,872.0 [58,293–260,561; n=3] | 185,983.1 [182,278–186,800; n=3] | 321,336.6 [298,077–431,358; n=3] | 872 |
| 4 CPU / 8 GiB | Bun | 179,712.0 [131,160–183,504; n=3] | 211,108.0 [156,234–290,562; n=3] | 820,604.0 [717,268–968,076; n=3] | 969 |
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
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 43,341 [43,074–43,608; n=2] | 165,622 [165,491–165,752; n=2] | 191,576 [191,409–191,744; n=2] | 64,815 [64,538–65,091; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 104,101 [103,745–104,457; n=2] | 263,585 [260,598–266,572; n=2] | 676,117 [674,522–677,712; n=2] | 183,128 [180,471–185,785; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 47,547 [47,106–47,988; n=2] | 176,763 [176,676–176,850; n=2] | 439,595 [437,817–441,373; n=2] | 117,680 [117,668–117,693; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 42,298 [41,914–42,682; n=2] | 118,542 [117,093–119,991; n=2] | 189,581 [181,410–197,752; n=2] | 68,232 [67,833–68,630; n=2] |
| 64 KiB messages | Documents and images: 2 producers of durable 64 KiB bodies, scored in MB/s. | delivered MB/s | 778.0 MB/s [776.9–779.1; n=2] | 1,201.7 MB/s [1,195.7–1,207.7; n=2] | 943.3 MB/s [876.8–1,009.9; n=2] | 272.5 MB/s [255.0–290.0; n=2] |
| 1 MiB messages | Large payloads: 1 producer of durable 1 MiB bodies, 4 confirms in flight, scored in MB/s. | delivered MB/s | 1,345.8 MB/s [1,336.2–1,355.4; n=2] | 1,236.2 MB/s [1,175.4–1,296.9; n=2] | 1,004.9 MB/s [998.7–1,011.0; n=2] | 371.7 MB/s [351.6–391.8; n=2] |
| 64 KiB messages, quorum queue | The 64 KiB cell on a quorum queue, which RabbitMQ also fsyncs before the confirm. | delivered MB/s | – | 698.9 MB/s [611.7–786.0; n=2] | 553.2 MB/s [532.9–573.6; n=2] | 60.2 MB/s [55.8–64.7; n=2] |
| 1 MiB messages, quorum queue | The 1 MiB cell on a quorum queue, which RabbitMQ also fsyncs before the confirm. | delivered MB/s | – | 884.4 MB/s [727.7–1,041.0; n=2] | 936.0 MB/s [911.4–960.7; n=2] | 163.4 MB/s [155.4–171.5; n=2] |
| Latency at 1,000 msg/s | A steady API workload: 1,000 durable 1 KiB msg/s with confirms. Scored on end-to-end latency. | p99 latency at the offered rate | 1.73 ms [1.30–2.16; n=2] | 1.54 ms [1.24–1.85; n=2] | 1.84 ms [1.67–2.02; n=2] | 17.59 ms [10.68–24.49; n=2] |
| Latency at 10,000 msg/s | A busy API workload: 2 producers at 5,000 durable 1 KiB msg/s each. | p99 latency at the offered rate | 12.80 ms [12.07–13.54; n=2] | 0.69 ms [0.23–1.16; n=2] | 2.37 ms [1.01–3.72; n=2] | 24.79 ms [21.61–27.96; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 16.53 ms [15.29–17.77; n=2] | 25.22 ms [7.19–43.26; n=2] | 4.40 ms [3.47–5.32; n=2] | 151.18 ms [39.61–262.75; n=2] |
| Slow workers, prefetch 1 | 20 workers that each spend 2 ms per job with prefetch 1, fed 5,000 msg/s. | p99 latency at the offered rate | 10.72 ms [10.44–11.01; n=2] | 0.33 ms [0.12–0.53; n=2] | 9.35 ms [1.51–17.19; n=2] | unscored (1/2 complete; 0 failed, 0 incomplete, 1 missed rate) |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 168,479 [167,914–169,044; n=2] | 176,558 [174,964–178,151; n=2] | 39,374 [38,467–40,280; n=2] |
| Stream queue | An append-only log read by 2 consumers from the start, over AMQP 0-9-1. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 129,943 [122,616–137,269; n=2] | 133,806 [131,433–136,178; n=2] | 74,058 [72,744–75,372; n=2] |
| Priority queue | x-max-priority=10, every message at priority 5, 2 producers and 2 consumers. | delivered msg/s | 32,635 [32,588–32,682; n=2] | 168,160 [165,116–171,204; n=2] | 182,698 [170,843–194,552; n=2] | 45,125 [44,829–45,420; n=2] |
| Transactions | tx.select publishers committing every 10 messages, transactional consumers acking every 10. | delivered msg/s | 21,134 [20,235–22,033; n=2] | 34,427 [21,531–47,323; n=2] | 42,652 [41,820–43,484; n=2] | 24,187 [20,239–28,134; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 34,843 [34,829–34,858; n=2] | 94,891 [86,560–103,222; n=2] | 184,168 [171,623–196,713; n=2] | 74,043 [73,453–74,632; n=2] |
| Backlog fill and drain | A consumer outage: 300,000 durable 1 KiB messages pile up, then 4 consumers drain them. | fill / drain msg/s | 74,235 / 50,007 [50,002–50,012; n=2] | 144,869 / 149,919 [149,880–149,959; n=2] | 182,660 / 149,794 [149,776–149,811; n=2] | 96,411 / 149,586 [149,396–149,778; n=2] |

Peak container memory, 1 CPU / 1 GiB (MiB, page cache included):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 194 | 170 | 231 | 108 |
| Fire-and-forget telemetry | 254 | 169 | 90 | 22 |
| Broadcast to 20 services | 186 | 1,024 | 194 | 366 |
| 64 queues, own publisher each | 220 | 1,024 | 262 | 246 |
| 64 KiB messages | 368 | 182 | 238 | 54 |
| 1 MiB messages | 335 | 196 | 226 | 152 |
| 64 KiB messages, quorum queue | – | 176 | 393 | 1,024 |
| 1 MiB messages, quorum queue | – | 194 | 364 | 258 |
| Latency at 1,000 msg/s | 152 | 66 | 100 | 27 |
| Latency at 10,000 msg/s | 153 | 168 | 140 | 30 |
| 500 queues, 1,000 connections | 422 | 1,024 | 148 | 124 |
| Slow workers, prefetch 1 | 176 | 118 | 110 | 46 |
| Quorum queue | 971 | 170 | 208 | 1,024 |
| Stream queue | 1,024 | 1,024 | 1,024 | 1,024 |
| Priority queue | 152 | 167 | 238 | 52 |
| Transactions | 151 | 168 | 197 | 32 |
| Heavy mixed load | 234 | 1,024 | 364 | 1,022 |
| Backlog fill and drain | 534 | 852 | 872 | 716 |

Disk writes and fsyncs over the measured window, 1 CPU / 1 GiB:

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | – | 200.3 MB/s, 1,157 fsync/s at 0.36 ms | 247.5 MB/s, 2,034 fsync/s at 0.23 ms | 79.8 MB/s, 324 fsync/s at 0.53 ms |
| Fire-and-forget telemetry | – | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s |
| Broadcast to 20 services | – | 228.4 MB/s, 1,838 fsync/s at 1.33 ms | 34.4 MB/s, 527 fsync/s at 0.29 ms | 78.9 MB/s, 60 fsync/s at 1.18 ms |
| 64 queues, own publisher each | – | 189.9 MB/s, 5,939 fsync/s at 3.06 ms | 146.1 MB/s, 2,500 fsync/s at 0.15 ms | 46.1 MB/s, 53 fsync/s at 0.99 ms |
| 64 KiB messages | – | 1,202.3 MB/s, 1,083 fsync/s at 0.47 ms | 952.3 MB/s, 2,106 fsync/s at 0.41 ms | 284.9 MB/s, 2,091 fsync/s at 0.32 ms |
| 1 MiB messages | – | 1,227.5 MB/s, 737 fsync/s at 0.89 ms | 985.6 MB/s, 881 fsync/s at 1.00 ms | 362.4 MB/s, 682 fsync/s at 0.98 ms |
| 64 KiB messages, quorum queue | – | 670.5 MB/s, 599 fsync/s at 1.22 ms | 541.1 MB/s, 1,060 fsync/s at 0.84 ms | 138.1 MB/s, 438 fsync/s at 0.60 ms |
| 1 MiB messages, quorum queue | – | 832.2 MB/s, 474 fsync/s at 1.70 ms | 935.9 MB/s, 791 fsync/s at 1.06 ms | 369.1 MB/s, 295 fsync/s at 0.64 ms |
| Latency at 1,000 msg/s | – | 3.2 MB/s, 100 fsync/s at 0.75 ms | 5.5 MB/s, 1,032 fsync/s at 0.21 ms | 8.7 MB/s, 1,811 fsync/s at 0.31 ms |
| Latency at 10,000 msg/s | – | 16.0 MB/s, 99 fsync/s at 0.27 ms | 50.2 MB/s, 9,117 fsync/s at 0.07 ms | 23.3 MB/s, 2,869 fsync/s at 0.27 ms |
| 500 queues, 1,000 connections | – | 80.8 MB/s, 9,474 fsync/s at 17.26 ms | 11.7 MB/s, 1,984 fsync/s at 0.13 ms | 6.3 MB/s, 727 fsync/s at 0.65 ms |
| Slow workers, prefetch 1 | – | 7.3 MB/s, 99 fsync/s at 0.23 ms | 22.8 MB/s, 4,661 fsync/s at 0.08 ms | 14.9 MB/s, 2,599 fsync/s at 0.39 ms |
| Quorum queue | – | 204.4 MB/s, 1,199 fsync/s at 0.33 ms | 238.0 MB/s, 2,233 fsync/s at 0.21 ms | 123.3 MB/s, 166 fsync/s at 0.45 ms |
| Stream queue | – | 270.1 MB/s | 236.8 MB/s, 0 fsync/s | 46.0 MB/s, 0 fsync/s |
| Priority queue | – | 206.8 MB/s, 1,303 fsync/s at 0.19 ms | 238.2 MB/s, 2,583 fsync/s at 0.19 ms | 57.0 MB/s, 462 fsync/s at 0.42 ms |
| Transactions | – | 71.3 MB/s, 3,621 fsync/s at 0.26 ms | 60.0 MB/s, 2,887 fsync/s at 0.23 ms | 17.7 MB/s, 616 fsync/s at 1.39 ms |
| Heavy mixed load | – | 193.4 MB/s, 1,053 fsync/s at 5.92 ms | 231.6 MB/s, 1,757 fsync/s at 0.26 ms | 87.0 MB/s, 19 fsync/s at 2.63 ms |
| Backlog fill and drain | – | 0.0 MB/s, 0 fsync/s at 0.58 ms | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 3 fsync/s at 0.55 ms |

### Scenarios, 4 CPU / 4 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 78,439 [76,724–80,154; n=2] | 302,073 [301,722–302,423; n=2] | 214,488 [214,449–214,528; n=2] | 69,460 [69,454–69,465; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 326,615 [325,030–328,199; n=2] | 275,549 [271,396–279,702; n=2] | 634,740 [631,247–638,234; n=2] | 190,221 [189,356–191,086; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 161,952 [161,576–162,329; n=2] | 309,820 [309,237–310,403; n=2] | 517,508 [510,136–524,880; n=2] | 121,924 [120,993–122,856; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 139,264 [137,458–141,070; n=2] | 252,359 [251,276–253,442; n=2] | 284,493 [276,101–292,885; n=2] | 63,202 [62,712–63,692; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 3.29 ms [3.01–3.56; n=2] | 3.14 ms [2.95–3.33; n=2] | 2.64 ms [1.77–3.51; n=2] | 61.44 ms [36.67–86.21; n=2] |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | 85,414 [85,270–85,557; n=2] | 266,996 [248,166–285,826; n=2] | 153,380 [132,019–174,742; n=2] | 37,653 [37,375–37,930; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 122,603 [122,138–123,069; n=2] | 243,352 [204,601–282,103; n=2] | 252,712 [244,398–261,027; n=2] | 80,807 [80,419–81,194; n=2] |

Peak container memory, 4 CPU / 4 GiB (MiB, page cache included):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 172 | 192 | 348 | 80 |
| Fire-and-forget telemetry | 238 | 214 | 198 | 22 |
| Broadcast to 20 services | 214 | 2,710 | 748 | 350 |
| 64 queues, own publisher each | 299 | 3,778 | 836 | 238 |
| 500 queues, 1,000 connections | 480 | 2,555 | 589 | 126 |
| Quorum queue | 770 | 188 | 326 | 1,192 |
| Heavy mixed load | 410 | 2,620 | 1,048 | 1,216 |

Disk writes and fsyncs over the measured window, 4 CPU / 4 GiB:

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | – | 366.6 MB/s, 2,180 fsync/s at 0.24 ms | 279.5 MB/s, 2,742 fsync/s at 0.15 ms | 85.4 MB/s, 348 fsync/s at 0.41 ms |
| Fire-and-forget telemetry | – | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s | 0.0 MB/s, 0 fsync/s |
| Broadcast to 20 services | – | 309.6 MB/s, 3,136 fsync/s at 1.50 ms | 103.9 MB/s, 3,153 fsync/s at 0.23 ms | 82.3 MB/s, 63 fsync/s at 1.12 ms |
| 64 queues, own publisher each | – | 415.1 MB/s, 12,655 fsync/s at 2.13 ms | 263.6 MB/s, 12,417 fsync/s at 0.19 ms | 42.6 MB/s, 49 fsync/s at 1.10 ms |
| 500 queues, 1,000 connections | – | 77.5 MB/s, 9,068 fsync/s at 6.70 ms | 20.3 MB/s, 4,106 fsync/s at 0.25 ms | 8.2 MB/s, 1,146 fsync/s at 0.31 ms |
| Quorum queue | – | 316.0 MB/s, 1,862 fsync/s at 0.33 ms | 207.7 MB/s, 2,142 fsync/s at 0.27 ms | 117.5 MB/s, 158 fsync/s at 0.54 ms |
| Heavy mixed load | – | 356.1 MB/s, 2,200 fsync/s at 3.79 ms | 300.2 MB/s, 2,867 fsync/s at 0.97 ms | 95.7 MB/s, 21 fsync/s at 2.40 ms |

### Connections

| Broker | Size | Held | Memory at hold |
| --- | --- | ---: | ---: |
| RabbitMQ | 1 CPU / 512m | 3,500 | 454.7 MiB |
| RabbitMQ | 2 CPU / 2g | 18,750 | 1.93 GiB |
| RabbitMQ | 4 CPU / 4g | 41,622 | 3.579 GiB |
| RabbitMQ | 4 CPU / 8g | 62,065 | 5.55 GiB |
| Rust | 1 CPU / 512m | 6,860 | 468.4 MiB |
| Rust | 2 CPU / 2g | 28,612 | 1.814 GiB |
| Rust | 4 CPU / 4g | 57,211 | 3.631 GiB |
| Rust | 4 CPU / 8g | 95,000 | 5.97 GiB |
| Bun | 1 CPU / 512m | 37,125 | 351.1 MiB |
| Bun | 2 CPU / 2g | 124,864 | 1.555 GiB |
| Bun | 4 CPU / 4g | 219,709 | 3.382 GiB |
| Bun | 4 CPU / 8g | 260,000 | 4.054 GiB |
| PHP | 1 CPU / 512m | 41,961 | 318.1 MiB |
| PHP | 2 CPU / 2g | 40,000 | 302.6 MiB |
| PHP | 4 CPU / 4g | 40,000 | 303 MiB |
| PHP | 4 CPU / 8g | 40,000 | 300.6 MiB |
