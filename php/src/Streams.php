<?php
declare(strict_types=1);

/** Durable append-only stream records, offsets and publisher deduplication. */
final class Streams
{
    private Store $store;
    private array $records = [];
    /** Body bytes in `records`, kept as they change so `trim` need not sum them. */
    private int $bytes = 0;
    /** Offsets below this are fsynced. A reader sees only those. */
    private int $synced = 0;
    /**
     * Synced bodies kept in memory. Older records keep only their log
     * position, length and time, and are read back from the log: holding a
     * whole stream in memory reached the 1 GiB limit in a 30 s run.
     */
    public const MEMORY_BYTES = 64 * 1024 * 1024;
    private int $memBytes = 0;
    /** Records below this offset are paged out or trimmed. */
    private int $pagedTo = 0;
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
            $this->bytes += strlen($record['body']) - strlen($this->records[$offset]['body'] ?? '');
            $this->memBytes += strlen($record['body']) - self::held($this->records[$offset] ?? null);
            $this->records[$offset] = ['offset' => $offset, 'body' => $record['body'], 'headers' => $meta['headers'] ?? [], 'propRaw' => $record['propRaw'], 'time' => $meta['time'] ?? 0, 'exchange' => $meta['exchange'] ?? '', 'key' => $meta['key'] ?? '', 'pos' => $record['pos']];
            if (is_string($meta['raftGroup'] ?? null)) $this->raftApplied[$meta['raftGroup']] = max($this->raftApplied[$meta['raftGroup']] ?? 0, (int)($meta['raftIndex'] ?? 0));
            $this->next = max($this->next, $offset + 1);
            if (is_string($meta['reference'] ?? null) && is_int($meta['sequence'] ?? null)) $this->sequences[$meta['reference']] = ['sequence' => $meta['sequence'], 'offset' => $offset];
        }
        $this->synced = $this->next;
        $this->pagedTo = $this->records === [] ? $this->next : (int) array_key_first($this->records);
        $this->pageOut();
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
    public function append(string $body, array $headers, ?string $propRaw, ?string $reference, ?int $sequence, ?string $raftGroup = null, ?int $raftIndex = null, ?int $timestamp = null, string $exchange = '', string $key = '', bool $sync = true): int
    {
        if ($raftGroup !== null && $raftIndex !== null && $raftIndex <= ($this->raftApplied[$raftGroup] ?? 0)) return max(0,$this->next-1);
        if ($raftIndex === null && $reference !== null && $sequence !== null && isset($this->sequences[$reference]) && self::unsignedLessOrEqual($sequence, $this->sequences[$reference]['sequence'])) return $this->sequences[$reference]['offset'];
        $offset = $this->next; $time = $timestamp ?? (int) (microtime(true) * 1000);
        $this->store->appendPublish($offset + 1, 'stream', $body, 2, $propRaw, ['headers' => $headers, 'time' => $time, 'reference' => $reference, 'sequence' => $sequence, 'raftGroup'=>$raftGroup, 'raftIndex'=>$raftIndex, 'exchange'=>$exchange, 'key'=>$key]);
        // Without `$sync` the caller runs `sync` later, once for many appends.
        // Readers see the record only after that.
        $this->bytes += strlen($body);
        $this->memBytes += strlen($body);
        $this->records[$offset] = ['offset' => $offset, 'body' => $body, 'headers' => $headers, 'propRaw' => $propRaw, 'time' => $time, 'exchange' => $exchange, 'key' => $key, 'pos' => $this->store->lastStart];
        $this->next++;
        if ($raftGroup !== null && $raftIndex !== null) $this->raftApplied[$raftGroup]=$raftIndex;
        if ($reference !== null && $sequence !== null) $this->sequences[$reference] = ['sequence' => $sequence, 'offset' => $offset];
        if ($sync) $this->sync();
        $this->trim();
        return $offset;
    }
    private static function unsignedLessOrEqual(int $a,int $b):bool { if(($a<0)!==($b<0))return $a>=0;return $a<=$b; }
    /** Records are keyed by offset with no gaps, so a read indexes them directly. */
    public function read(int $offset, int $count): array
    {
        $this->trim();
        $out = [];
        $end = min($this->synced, max($offset, $this->first()) + max(0, $count));
        for ($i = max($offset, $this->first()); $i < $end; $i++) if (isset($this->records[$i])) $out[] = $this->resolve($this->records[$i]);
        return $out;
    }
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
        $bytes = $this->bytes;
        $now = (int)(microtime(true) * 1000); $drop = 0;
        foreach ($this->records as $record) {
            if ($drop >= count($this->records) - 1) break;
            if (!($this->maxBytes !== null && $bytes > $this->maxBytes) && !($this->maxAgeMs !== null && $now - $record['time'] > $this->maxAgeMs)) break;
            $bytes -= self::size($record); $drop++;
        }
        if ($drop === 0) return;
        $snapshot = $this->snapshot();
        $snapshot['stream']['entries'] = array_slice($snapshot['stream']['entries'], $drop);
        $snapshot['stream']['first'] = $snapshot['stream']['entries'][0]['offset'];
        // The checkpoint retains offsets, producer sequences and Raft indexes in
        // the same atomic log replacement as the retained bodies.
        $this->install($snapshot);
    }
    public function sync(): void { $this->store->sync(); $this->synced = $this->next; $this->pageOut(); }

    /** Body bytes a record holds in memory. */
    private static function held(?array $record): int
    {
        return $record === null || isset($record['paged']) ? 0 : strlen($record['body']);
    }

    /** Body length, paged or not. */
    private static function size(array $record): int
    {
        return isset($record['paged']) ? $record['len'] : strlen($record['body']);
    }

    /** Page the oldest synced bodies out until the memory budget holds. */
    private function pageOut(): void
    {
        if ($this->memBytes <= self::MEMORY_BYTES) return;
        $this->pagedTo = max($this->pagedTo, $this->first());
        while ($this->memBytes > self::MEMORY_BYTES && $this->pagedTo < $this->synced) {
            $r = $this->records[$this->pagedTo] ?? null;
            if ($r !== null && !isset($r['paged'])) {
                $this->memBytes -= strlen($r['body']);
                $this->records[$this->pagedTo] = ['offset' => $r['offset'], 'paged' => true, 'pos' => $r['pos'], 'len' => strlen($r['body']), 'time' => $r['time']];
            }
            $this->pagedTo++;
        }
    }

    /** A record with its body, read back from the log when it was paged out. */
    private function resolve(array $record): array
    {
        if (!isset($record['paged'])) return $record;
        $row = $this->store->readAt($record['pos']);
        if ($row === null) throw new RuntimeException('Stream record unreadable at offset ' . $record['offset']);
        $meta = $row['meta'];
        return ['offset' => $record['offset'], 'body' => $row['body'], 'headers' => $meta['headers'] ?? [], 'propRaw' => $row['propRaw'], 'time' => $meta['time'] ?? $record['time'], 'exchange' => $meta['exchange'] ?? '', 'key' => $meta['key'] ?? '', 'pos' => $record['pos']];
    }
    /** True when an append is not yet covered by `sync`. */
    public function dirty(): bool { return $this->synced < $this->next; }
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
            foreach ($records as $i => $record) { $fresh->appendPublish($record['offset'] + 1, 'stream', $record['body'], 2, $record['propRaw'], ['headers'=>$record['headers'], 'time'=>$record['time'], 'exchange'=>$record['exchange'], 'key'=>$record['key']]); $records[$i]['pos'] = $fresh->lastStart; }
            // Keep replay boundaries and dedup state in the same atomic log replacement.
            $fresh->appendPublish(0, 'stream-state', '', 2, null, $checkpoint); $fresh->sync();
            if (!rename($temp, $path)) throw new RuntimeException('Cannot install stream snapshot');
            fclose($this->store->fp); $fresh->path = $path; $this->store = $fresh;
            $this->records = $records; $this->bytes = 0; foreach ($records as $record) $this->bytes += strlen($record['body']); $this->memBytes = $this->bytes; $this->pagedTo = $snap['first']; $this->next = $snap['next']; $this->offsets = $checkpoint['offsets']; $this->sequences = $checkpoint['sequences']; $this->raftApplied = $applied; $this->generation = $generation; $this->synced = $this->next; $this->pageOut();
        } finally { if (is_file($temp)) { fclose($fresh->fp); unlink($temp); } }
    }
    public function snapshot(string $vhost = '/', string $queue = ''): array
    {
        return ['stream'=>['vhost'=>$vhost, 'queue'=>$queue, 'first'=>$this->first(), 'next'=>$this->next, 'raftIndex'=>$this->raftApplied === [] ? 0 : max($this->raftApplied),
            'entries'=>array_map(static fn(array $r): array => ['offset'=>$r['offset'], 'ts'=>$r['time'], 'body_b64'=>base64_encode($r['body']), 'exchange'=>$r['exchange'] ?? '', 'routing_key'=>$r['key'] ?? '', 'headers'=>$r['headers'], 'propRaw'=>$r['propRaw'] === null ? null : base64_encode($r['propRaw'])], array_map(fn (array $r): array => $this->resolve($r), array_values($this->records))),
            'offsets'=>$this->offsets, 'sequences'=>$this->sequences, 'raftApplied'=>$this->raftApplied]];
    }
}
