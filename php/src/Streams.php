<?php
declare(strict_types=1);

/** Durable append-only stream records, offsets and publisher deduplication. */
final class Streams
{
    private Store $store;
    private array $records = [];
    private array $sequences = [];
    private array $offsets = [];
    private int $next = 0;
    private array $raftApplied = [];
    private string $offsetPath;
    private ?string $generation = null;
    private ?int $maxBytes = null;
    private ?int $maxAgeMs = null;
    public function __construct(string $dir)
    {
        if (!is_dir($dir) && !mkdir($dir, 0700, true) && !is_dir($dir)) throw new RuntimeException('Cannot create stream directory');
        $this->store = new Store($dir . '/records.log');
        $this->offsetPath = $dir . '/offsets.json';
        foreach ($this->store->replay() as $record) {
            $meta = $record['meta'];
            if ($record['queue'] === 'stream-state') {
                $this->next = max($this->next, (int) ($meta['next'] ?? 0));
                $this->offsets = $meta['offsets'] ?? []; $this->sequences = $meta['sequences'] ?? [];
                $this->raftApplied = $meta['raftApplied'] ?? []; $this->generation = $meta['generation'] ?? null;
                continue;
            }
            $offset = $record['id'] - 1;
            $this->records[$offset] = ['offset' => $offset, 'body' => $record['body'], 'headers' => $meta['headers'] ?? [], 'propRaw' => $record['propRaw'], 'time' => $meta['time'] ?? 0, 'exchange' => $meta['exchange'] ?? '', 'key' => $meta['key'] ?? ''];
            if (is_string($meta['raftGroup'] ?? null)) $this->raftApplied[$meta['raftGroup']] = max($this->raftApplied[$meta['raftGroup']] ?? 0, (int)($meta['raftIndex'] ?? 0));
            $this->next = max($this->next, $offset + 1);
            if (is_string($meta['reference'] ?? null) && is_int($meta['sequence'] ?? null)) $this->sequences[$meta['reference']] = ['sequence' => $meta['sequence'], 'offset' => $offset];
        }
        if (is_file($this->offsetPath)) {
            $value = json_decode((string) file_get_contents($this->offsetPath), true, 512, JSON_THROW_ON_ERROR);
            if (!is_array($value)) throw new RuntimeException('Invalid stream offsets');
            if (($value['generation'] ?? null) === $this->generation) {
                $this->offsets = $value['offsets'] ?? $value;
                foreach ($value['sequences'] ?? [] as $ref => $item) {
                    if (!isset($this->sequences[$ref]) || self::unsignedLessOrEqual($this->sequences[$ref]['sequence'], $item['sequence'])) $this->sequences[$ref] = $item;
                }
                foreach ($value['raftApplied'] ?? [] as $g => $i) $this->raftApplied[$g] = max($this->raftApplied[$g] ?? 0, $i);
            }
        }
    }
    public function append(string $body, array $headers, ?string $propRaw, ?string $reference, ?int $sequence, ?string $raftGroup = null, ?int $raftIndex = null, ?int $timestamp = null, string $exchange = '', string $key = ''): int
    {
        if ($raftGroup !== null && $raftIndex !== null && $raftIndex <= ($this->raftApplied[$raftGroup] ?? 0)) return max(0,$this->next-1);
        if ($raftIndex === null && $reference !== null && $sequence !== null && isset($this->sequences[$reference]) && self::unsignedLessOrEqual($sequence, $this->sequences[$reference]['sequence'])) return $this->sequences[$reference]['offset'];
        $offset = $this->next; $time = $timestamp ?? (int) (microtime(true) * 1000);
        $this->store->appendPublish($offset + 1, 'stream', $body, 2, $propRaw, ['headers' => $headers, 'time' => $time, 'reference' => $reference, 'sequence' => $sequence, 'raftGroup'=>$raftGroup, 'raftIndex'=>$raftIndex, 'exchange'=>$exchange, 'key'=>$key]);
        $this->store->sync();
        $this->records[$offset] = ['offset' => $offset, 'body' => $body, 'headers' => $headers, 'propRaw' => $propRaw, 'time' => $time, 'exchange' => $exchange, 'key' => $key];
        $this->next++;
        if ($raftGroup !== null && $raftIndex !== null) $this->raftApplied[$raftGroup]=$raftIndex;
        if ($reference !== null && $sequence !== null) $this->sequences[$reference] = ['sequence' => $sequence, 'offset' => $offset];
        $this->trim();
        return $offset;
    }
    private static function unsignedLessOrEqual(int $a,int $b):bool { if(($a<0)!==($b<0))return $a>=0;return $a<=$b; }
    public function read(int $offset, int $count): array { $this->trim(); return array_values(array_slice($this->records, max(0, $offset - $this->first()), max(0, $count), true)); }
    public function first(): int { return $this->records === [] ? $this->next : (int) array_key_first($this->records); }
    public function next(): int { return $this->next; }
    public function sequence(string $reference): ?int { return $this->sequences[$reference]['sequence'] ?? null; }
    public function stored(string $reference): ?int { return $this->offsets[$reference] ?? null; }
    public function storeOffset(string $reference, int $offset): void
    {
        if ($offset < 0) throw new RuntimeException('Invalid stream offset');
        $next = $this->offsets; $next[$reference] = $offset;
        Broker::writeDurable($this->offsetPath, json_encode(['generation'=>$this->generation,'offsets'=>$next,'sequences'=>$this->sequences,'raftApplied'=>$this->raftApplied], JSON_THROW_ON_ERROR)); $this->offsets = $next;
    }
    public function retention(?int $maxBytes, ?int $maxAgeMs): void
    {
        $this->maxBytes = $maxBytes; $this->maxAgeMs = $maxAgeMs;
        $this->trim();
    }
    /** Match the other stacks: remove an expired prefix but keep the newest entry. */
    private function trim(): void
    {
        if (($this->maxBytes === null && $this->maxAgeMs === null) || count($this->records) < 2) return;
        $bytes = array_sum(array_map(static fn(array $record): int => strlen($record['body']), $this->records));
        $now = (int)(microtime(true) * 1000); $drop = 0;
        foreach ($this->records as $record) {
            if ($drop >= count($this->records) - 1) break;
            if (!($this->maxBytes !== null && $bytes > $this->maxBytes) && !($this->maxAgeMs !== null && $now - $record['time'] > $this->maxAgeMs)) break;
            $bytes -= strlen($record['body']); $drop++;
        }
        if ($drop === 0) return;
        $snapshot = $this->snapshot();
        $snapshot['stream']['entries'] = array_slice($snapshot['stream']['entries'], $drop);
        $snapshot['stream']['first'] = $snapshot['stream']['entries'][0]['offset'];
        // The checkpoint retains offsets, producer sequences and Raft indexes in
        // the same atomic log replacement as the retained bodies.
        $this->install($snapshot);
    }
    public function sync(): void { $this->store->sync(); }
    /** Validate the complete portable snapshot before replacing durable records. */
    public function install(array $state, ?string $group = null): void
    {
        $snap = $state['stream'] ?? $state;
        if (!is_array($snap) || !isset($snap['entries'], $snap['next'], $snap['first'], $snap['raftIndex']) || !is_array($snap['entries'])) throw new RuntimeException('Unsupported stream snapshot');
        foreach (['first', 'next', 'raftIndex'] as $field) if (!is_int($snap[$field]) || $snap[$field] < 0) throw new RuntimeException('Invalid stream snapshot bounds');
        if ($snap['next'] < $snap['first']) throw new RuntimeException('Invalid stream snapshot bounds');
        foreach (['offsets','sequences','raftApplied'] as $field) if (isset($snap[$field]) && !is_array($snap[$field])) throw new RuntimeException('Invalid stream snapshot state');
        foreach ($snap['offsets'] ?? [] as $offset) if (!is_int($offset) || $offset < 0) throw new RuntimeException('Invalid stream snapshot consumer offset');
        foreach ($snap['sequences'] ?? [] as $item) if (!is_array($item) || !is_int($item['sequence'] ?? null) || !is_int($item['offset'] ?? null) || $item['offset'] < 0 || $item['offset'] >= $snap['next']) throw new RuntimeException('Invalid stream snapshot publisher sequence');
        foreach ($snap['raftApplied'] ?? [] as $index) if (!is_int($index) || $index < 0) throw new RuntimeException('Invalid stream snapshot Raft index');
        $records = []; $last = $snap['first'] - 1;
        foreach ($snap['entries'] as $entry) {
            if (!is_array($entry) || !is_int($entry['offset'] ?? null) || $entry['offset'] !== $last + 1 || $entry['offset'] >= $snap['next'] || !is_string($entry['body_b64'] ?? null) || !is_int($entry['ts'] ?? null)) throw new RuntimeException('Invalid stream snapshot entry');
            $body = base64_decode($entry['body_b64'], true); $raw = isset($entry['propRaw']) ? base64_decode($entry['propRaw'], true) : null;
            if ($body === false || $raw === false || !is_array($entry['headers'] ?? [])) throw new RuntimeException('Invalid stream snapshot payload');
            $last = $entry['offset'];
            $records[$last] = ['offset'=>$last, 'body'=>$body, 'headers'=>$entry['headers'] ?? [], 'propRaw'=>$raw, 'time'=>$entry['ts'], 'exchange'=>(string)($entry['exchange'] ?? ''), 'key'=>(string)($entry['routing_key'] ?? '')];
        }
        if ($last + 1 !== $snap['next']) throw new RuntimeException('Incomplete stream snapshot');
        $generation = bin2hex(random_bytes(16));
        $applied = $snap['raftApplied'] ?? []; if ($group !== null) $applied[$group] = $snap['raftIndex'];
        $checkpoint = ['generation'=>$generation, 'next'=>$snap['next'], 'offsets'=>$snap['offsets'] ?? [], 'sequences'=>$snap['sequences'] ?? [], 'raftApplied'=>$applied];
        $path = $this->store->path; $temp = $path . '.snapshot-' . $generation; $fresh = new Store($temp);
        try {
            foreach ($records as $record) $fresh->appendPublish($record['offset'] + 1, 'stream', $record['body'], 2, $record['propRaw'], ['headers'=>$record['headers'], 'time'=>$record['time'], 'exchange'=>$record['exchange'], 'key'=>$record['key']]);
            // Keep replay boundaries and dedup state in the same atomic log replacement.
            $fresh->appendPublish(0, 'stream-state', '', 2, null, $checkpoint); $fresh->sync();
            if (!rename($temp, $path)) throw new RuntimeException('Cannot install stream snapshot');
            fclose($this->store->fp); $fresh->path = $path; $this->store = $fresh;
            $this->records = $records; $this->next = $snap['next']; $this->offsets = $checkpoint['offsets']; $this->sequences = $checkpoint['sequences']; $this->raftApplied = $applied; $this->generation = $generation;
        } finally { if (is_file($temp)) { fclose($fresh->fp); unlink($temp); } }
    }
    public function snapshot(string $vhost = '/', string $queue = ''): array
    {
        return ['stream'=>['vhost'=>$vhost, 'queue'=>$queue, 'first'=>$this->first(), 'next'=>$this->next, 'raftIndex'=>$this->raftApplied === [] ? 0 : max($this->raftApplied),
            'entries'=>array_map(static fn(array $r): array => ['offset'=>$r['offset'], 'ts'=>$r['time'], 'body_b64'=>base64_encode($r['body']), 'exchange'=>$r['exchange'] ?? '', 'routing_key'=>$r['key'] ?? '', 'headers'=>$r['headers'], 'propRaw'=>$r['propRaw'] === null ? null : base64_encode($r['propRaw'])], array_values($this->records)),
            'offsets'=>$this->offsets, 'sequences'=>$this->sequences, 'raftApplied'=>$this->raftApplied]];
    }
}
