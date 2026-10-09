<?php
declare(strict_types=1);

/**
 * Append-only log. A durable confirm is released only after fsync covers its record.
 */
final class Store
{
    /** A log smaller than this is never rewritten; the work is not worth it. */
    public const COMPACT_MIN_BYTES = 8 * 1024 * 1024;

    /** @var resource */
    public $fp;
    public int $end = 0;
    public int $synced = 0;
    /** Syncs since the last rewrite, used to rate-limit the size check. */
    public int $sinceCompact = 0;

    public function __construct(public string $path)
    {
        $dir = dirname($path);
        if (!is_dir($dir) && !mkdir($dir, 0777, true) && !is_dir($dir)) {
            throw new RuntimeException("cannot create $dir");
        }
        $fp = fopen($path, 'c+b');
        if ($fp === false) {
            throw new RuntimeException("cannot open $path");
        }
        $this->fp = $fp;
        $this->end = (int) filesize($path);
        $this->synced = $this->end;
    }

    /** @return list<array{id:int,queue:string,body:string,mode:int,propRaw:?string,meta:array<string, mixed>}> */
    public function replay(): array
    {
        rewind($this->fp);
        $live = [];
        $pos = 0;
        $size = $this->end;
        while ($pos + 5 <= $size) {
            $head = fread($this->fp, 5);
            if ($head === false || strlen($head) < 5) {
                break;
            }
            $type = ord($head[0]);
            $len = unpack('N', substr($head, 1, 4))[1];
            $payload = $len > 0 ? fread($this->fp, $len) : '';
            if ($payload === false || strlen($payload) < $len) {
                break;
            }
            $pos += 5 + $len;
            if ($type === 1 && strlen($payload) >= 8) {
                $id = Codec::readU64($payload, 0);
                $o = 8;
                $queue = Codec::readShortstr($payload, $o);
                if ($o + 4 > strlen($payload)) {
                    continue;
                }
                $bodyLen = unpack('N', substr($payload, $o, 4))[1];
                $o += 4;
                $body = substr($payload, $o, $bodyLen);
                $o += $bodyLen;
                $mode = $o < strlen($payload) ? ord($payload[$o]) : 2;
                $o += 1;
                // Records written before properties were persisted stop here.
                $propRaw = null;
                if ($o + 4 <= strlen($payload)) {
                    $propLen = unpack('N', substr($payload, $o, 4))[1];
                    $o += 4;
                    if ($propLen > 0) {
                        $propRaw = substr($payload, $o, $propLen);
                    }
                    $o += $propLen;
                }
                // The metadata field was added after that, so its absence is
                // also tolerated.
                $meta = [];
                if ($o + 4 <= strlen($payload)) {
                    $metaLen = unpack('N', substr($payload, $o, 4))[1];
                    $o += 4;
                    if ($metaLen > 0) {
                        $decoded = json_decode(substr($payload, $o, $metaLen), true);
                        if (is_array($decoded) && ($decoded['qf_encoding'] ?? '') === 'php-serialized-v1') {
                            $raw = base64_decode((string) ($decoded['data'] ?? ''), true);
                            $decoded = $raw === false ? null : unserialize($raw, ['allowed_classes' => false]);
                        }
                        $meta = is_array($decoded) ? $decoded : [];
                    }
                }
                $live[$id] = [
                    'id' => $id,
                    'queue' => $queue,
                    'body' => $body,
                    'mode' => $mode,
                    'propRaw' => $propRaw,
                    'meta' => $meta,
                ];
            } elseif ($type === 2 && strlen($payload) >= 8) {
                $id = Codec::readU64($payload, 0);
                unset($live[$id]);
            }
        }
        fseek($this->fp, $this->end);
        return array_values($live);
    }

    /**
     * Publish record: u64 id, shortstr queue, u32+body, mode byte, then
     * u32+propRaw and u32+meta JSON. Both trailing fields were added later,
     * so replay treats an absent one as empty and older logs still load.
     *
     * @param array<string, mixed> $meta
     */
    private static function encodeMetadata(array $meta): string
    {
        try { return json_encode($meta, JSON_THROW_ON_ERROR); }
        catch (JsonException) { return json_encode(['qf_encoding'=>'php-serialized-v1','data'=>base64_encode(serialize($meta))], JSON_THROW_ON_ERROR); }
    }

    public function appendPublish(int $id, string $queue, string $body, int $mode, ?string $propRaw = null, array $meta = []): int
    {
        $props = $propRaw ?? '';
        $encoded = $meta === [] ? '' : self::encodeMetadata($meta);
        $payload = Codec::u64($id)
            . Codec::shortstr($queue)
            . pack('N', strlen($body)) . $body
            . chr($mode)
            . pack('N', strlen($props)) . $props
            . pack('N', strlen($encoded)) . $encoded;
        return $this->append(1, $payload);
    }

    public function appendAck(int $id): int
    {
        return $this->append(2, Codec::u64($id));
    }

    /** How many fsync calls have run, for the metrics histogram. */
    public int $fsyncCount = 0;
    /** Total wall time spent in fsync, in seconds. */
    public float $fsyncSeconds = 0.0;
    /**
     * Confirms released before the fsync that covered them. This must stay
     * zero: a non-zero value means the durability invariant was broken.
     */
    public int $confirmsBeforeFsync = 0;
    /** How many times the whole log was flushed rather than a tail. */
    public int $fullFlushes = 0;

    public function sync(): void
    {
        if ($this->synced === $this->end) {
            return;
        }
        $started = microtime(true);
        fflush($this->fp);
        if (!fsync($this->fp)) {
            throw new RuntimeException('fsync failed');
        }
        $this->fsyncCount++;
        $this->fsyncSeconds += microtime(true) - $started;
        $this->fullFlushes++;
        $this->synced = $this->end;
        $this->sinceCompact++;
    }

    /**
     * Rewrites the log with only the records still live. The log is otherwise
     * append-only and grows without bound, so a long-running broker keeps
     * every ack record for every message it ever handled.
     *
     * Returns true when a rewrite happened. Callers should only invoke this
     * when nothing is waiting on a confirm, because it resets the byte
     * offsets those confirms are gated on.
     *
     * @param list<array{id:int,queue:string,body:string,mode:int,propRaw:?string,meta:array<string, mixed>}> $live
     */
    public function compact(array $live): bool
    {
        $temp = $this->path . '.compact';
        $fp = fopen($temp, 'w+b');
        if ($fp === false) {
            return false;
        }
        // Writes only inserts, so a delete can never be replayed after the
        // insert it was meant to cancel. Bun needs an equivalent rule for its
        // batches (bun/src/store.ts:504-515); compaction gives it for free by
        // dropping the acks entirely.
        foreach ($live as $msg) {
            $props = $msg['propRaw'] ?? '';
            $meta = $msg['meta'] ?? [];
            $encoded = $meta === [] ? '' : self::encodeMetadata($meta);
            $payload = Codec::u64($msg['id'])
                . Codec::shortstr($msg['queue'])
                . pack('N', strlen($msg['body'])) . $msg['body']
                . chr($msg['mode'])
                . pack('N', strlen($props)) . $props
                . pack('N', strlen($encoded)) . $encoded;
            $record = chr(1) . pack('N', strlen($payload)) . $payload;
            if (fwrite($fp, $record) !== strlen($record)) {
                fclose($fp);
                @unlink($temp);
                return false;
            }
        }
        fflush($fp);
        if (!fsync($fp)) {
            fclose($fp);
            @unlink($temp);
            return false;
        }
        fclose($fp);
        // Replace only after the replacement is durable, so a crash mid-way
        // leaves the original log intact.
        if (!@rename($temp, $this->path)) {
            @unlink($temp);
            return false;
        }
        fclose($this->fp);
        $fp = fopen($this->path, 'c+b');
        if ($fp === false) {
            throw new RuntimeException('cannot reopen ' . $this->path);
        }
        $this->fp = $fp;
        $this->end = (int) filesize($this->path);
        $this->synced = $this->end;
        fseek($this->fp, $this->end);
        $this->sinceCompact = 0;
        return true;
    }

    /**
     * Whether the log has grown enough to be worth rewriting: at least the
     * threshold in bytes, and mostly dead weight.
     */
    public function shouldCompact(int $liveBytes): bool
    {
        if ($this->end < self::COMPACT_MIN_BYTES) {
            return false;
        }
        return $liveBytes * 2 < $this->end;
    }

    private function append(int $type, string $payload): int
    {
        $rec = chr($type) . pack('N', strlen($payload)) . $payload;
        $n = fwrite($this->fp, $rec);
        if ($n !== strlen($rec)) {
            throw new RuntimeException('short write');
        }
        $this->end += $n;
        return $this->end;
    }
}
