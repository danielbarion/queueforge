<?php
declare(strict_types=1);

/**
 * Client-visible rules shared with the Bun and Rust brokers:
 * header bindings, queue arguments, quorum majority, and the version-1 append body.
 */
final class Features
{
    /**
     * @param list<array{0:string,1:string}> $args
     * @param list<array{0:string,1:string}> $headers
     */
    public static function headersMatch(array $args, array $headers): bool
    {
        $any = false;
        $checks = [];
        foreach ($args as $pair) {
            if ($pair[0] === 'x-match' && $pair[1] === 'any') {
                $any = true;
                continue;
            }
            if ($pair[0] !== 'x-match') {
                $checks[] = $pair;
            }
        }
        if ($checks === []) {
            return !$any;
        }
        $hit = static function (string $k, mixed $v) use ($headers): bool {
            foreach ($headers as $header) {
                if ($header[0] === $k && $header[1] === $v) {
                    return true;
                }
            }
            return false;
        };
        if ($any) {
            foreach ($checks as $pair) {
                if ($hit($pair[0], $pair[1])) {
                    return true;
                }
            }
            return false;
        }
        foreach ($checks as $pair) {
            if (!$hit($pair[0], $pair[1])) {
                return false;
            }
        }
        return true;
    }

    /** @param array<string, string|int|float> $raw */
    public static function parseArgs(array $raw): array
    {
        $num = static function (string $key) use ($raw): ?int {
            if (!isset($raw[$key]) || $raw[$key] === '') {
                return null;
            }
            return (int) $raw[$key];
        };
        $str = static function (string $key) use ($raw): ?string {
            if (!isset($raw[$key])) {
                return null;
            }
            return (string) $raw[$key];
        };
        $overflow = $str('x-overflow');
        if ($overflow !== 'reject-publish' && $overflow !== 'reject-publish-dlx') {
            $overflow = 'drop-head';
        }
        $maxPriority = $num('x-max-priority');
        $type = in_array($str('x-queue-type'), ['quorum', 'stream'], true) ? $str('x-queue-type') : 'classic';
        $delivery = $num('x-delivery-limit');
        if ($type === 'quorum' && $delivery === null) {
            $delivery = 20;
        }
        $strategy = $str('x-dead-letter-strategy');
        if ($strategy !== 'at-least-once') {
            $strategy = 'at-most-once';
        }
        // x-single-active-consumer is accepted as the string "true" or as 1,
        // which is how Bun reads it (bun/src/broker/args.ts:33).
        $single = $raw['x-single-active-consumer'] ?? null;
        return [
            'messageTtl' => $num('x-message-ttl'),
            'maxLength' => $num('x-max-length'),
            'maxLengthBytes' => $num('x-max-length-bytes'),
            'overflow' => $overflow,
            'dlx' => $str('x-dead-letter-exchange'),
            'dlxKey' => $str('x-dead-letter-routing-key'),
            'dlxStrategy' => $strategy,
            'maxPriority' => $maxPriority !== null && $maxPriority > 0 ? $maxPriority : null,
            'queueType' => $type,
            'deliveryLimit' => $delivery,
            'expiresMs' => $num('x-expires'),
            'singleActive' => $single === 'true' || $single === 1 || $single === true,
        ];
    }

    /**
     * Whether an x-queue-type value is one this broker serves. Anything else
     * is a 406 rather than being silently coerced to classic.
     */
    public static function knownQueueType(string $type): bool
    {
        return $type === '' || $type === 'classic' || $type === 'quorum' || $type === 'stream';
    }

    /**
     * Builds the x-death header set for a dead-lettered message, matching
     * Bun's layout (bun/src/broker/args.ts:49-70): one entry with queue,
     * reason, count of 1, exchange and a single-element routing-keys array,
     * plus first and last death fields. Repeated deaths for the same queue and
     * reason increment the count; records for other queues or reasons remain.
     *
     * @param list<array{0:string,1:mixed}> $headers
     * @return list<array{0:string,1:mixed}>
     */
    public static function deathHeaders(array $headers, string $queue, string $reason, string $exchange, string $routingKey): array
    {
        $rest = [];
        $deaths = [];
        $first = [];
        foreach ($headers as $pair) {
            if ($pair[0] === 'x-death') $deaths = is_array($pair[1]) ? $pair[1] : [];
            elseif (str_starts_with($pair[0], 'x-first-death-')) $first[$pair[0]] = $pair[1];
            elseif (!str_starts_with($pair[0], 'x-last-death-')) $rest[] = $pair;
        }
        if (isset($deaths['queue'])) $deaths = [$deaths];
        $count = 1;
        $keep = [];
        foreach ($deaths as $death) {
            if (($death['queue'] ?? null) === $queue && ($death['reason'] ?? null) === $reason) $count += (int) ($death['count'] ?? 0);
            else $keep[] = $death;
        }
        $rest[] = ['x-death', [['queue' => $queue, 'reason' => $reason, 'count' => $count,
            'exchange' => $exchange, 'routing-keys' => [$routingKey]], ...$keep]];
        foreach (['reason' => $reason, 'queue' => $queue, 'exchange' => $exchange] as $key => $value) {
            $rest[] = ['x-first-death-' . $key, $first['x-first-death-' . $key] ?? $value];
            $rest[] = ['x-last-death-' . $key, $value];
        }
        return $rest;
    }

    /** A quorum publish confirms only after this many durable copies. */
    public static function majority(int $members): int
    {
        return intdiv(max($members, 1), 2) + 1;
    }

    /**
     * Whether this node should open the connection to a peer. Only the lower
     * id dials, so a pair gets exactly one connection instead of two. Bun
     * applies the same rule (bun/src/cluster.ts:143).
     */
    public static function shouldDial(string $self, string $peer): bool
    {
        return strcmp($peer, $self) > 0;
    }

    /**
     * 64-bit FNV-1a over the UTF-8 bytes of the vhost, one 0xff byte, then
     * the queue name. The same value Rust and Bun use (docs/raft.md, section 9).
     */
    public static function homeHash(string $vhost, string $name): string
    {
        [$hi, $lo] = self::fnv1a64($vhost . "\xff" . $name);
        return sprintf('%08x%08x', $hi, $lo);
    }

    /**
     * FNV-1a 64 as two 32-bit halves, so it needs no GMP and never overflows
     * into a float. The prime is 2^40 + 0x1b3.
     *
     * @return array{0:int,1:int} high and low 32 bits
     */
    private static function fnv1a64(string $bytes): array
    {
        $hi = 0xcbf29ce4;
        $lo = 0x84222325;
        $length = strlen($bytes);
        for ($i = 0; $i < $length; $i++) {
            $lo ^= ord($bytes[$i]);
            $low = $lo * 0x1b3;
            $hi = ($hi * 0x1b3 + ($low >> 32) + ($lo << 8)) & 0xffffffff;
            $lo = $low & 0xffffffff;
        }
        return [$hi, $lo];
    }

    /**
     * Picks the home node of a classic queue. Member ids are sorted, and the
     * index is {@see homeHash} modulo that count. The chosen home is stored
     * with the queue, so a later membership change does not move it.
     *
     * @param list<array{id:string,addr:string}> $members
     */
    public static function home(array $members, string $vhost, string $name): string
    {
        if ($members === []) {
            return '';
        }
        $ids = array_column($members, 'id');
        sort($ids);
        [$hi, $lo] = self::fnv1a64($vhost . "\xff" . $name);
        $n = count($ids);
        // (hi * 2^32 + lo) mod n, kept in range of a native int.
        $index = (($hi % $n) * ((1 << 32) % $n) + $lo % $n) % $n;
        return $ids[$index];
    }

    /** @param list<string> $copies durable|memory */
    public static function durableMajority(int $members, array $copies): bool
    {
        $durable = 0;
        foreach ($copies as $copy) {
            if ($copy === 'durable') {
                $durable++;
            }
        }
        return $durable >= self::majority($members);
    }

    /** Version 1 quorum body. Rust and Bun encode these same fields. */
    public static function encodeQuorumAppend(string $vhost, string $queue, string $messageId, string $body, string $exchange, string $routingKey, bool $persistent): array
    {
        return [
            'v' => 1,
            'vhost' => $vhost,
            'queue' => $queue,
            'message_id' => $messageId,
            'body_b64' => base64_encode($body),
            'persistent' => $persistent,
            'routing_key' => $routingKey,
            'exchange' => $exchange,
        ];
    }

    /** @param array<string, mixed> $payload */
    public static function decodeQuorumAppend(array $payload): array
    {
        $source = isset($payload['message']) && is_array($payload['message']) ? $payload['message'] : $payload;
        $b64 = (string) ($source['body_b64'] ?? $source['body'] ?? '');
        $messageId = (string) ($source['message_id'] ?? $source['qid'] ?? $source['id'] ?? '');
        $routing = (string) ($source['routing_key'] ?? $source['routingKey'] ?? '');
        return [
            'vhost' => (string) ($payload['vhost'] ?? ''),
            'queue' => (string) ($payload['queue'] ?? ''),
            'messageId' => $messageId,
            'body' => base64_decode($b64, true) === false ? '' : (string) base64_decode($b64, true),
            'exchange' => (string) ($source['exchange'] ?? ''),
            'routingKey' => $routing,
            'persistent' => ($source['persistent'] ?? true) !== false,
        ];
    }

    /** @param list<string> $reachable member ids including this node */
    public static function leader(array $reachable): string
    {
        if ($reachable === []) {
            return '';
        }
        $ids = $reachable;
        sort($ids, SORT_STRING);
        return $ids[0];
    }

    public static function mqttMatch(string $filter, string $topic): bool
    {
        if ($filter === $topic || $filter === '#') {
            return true;
        }
        $f = explode('/', $filter);
        $t = explode('/', $topic);
        foreach ($f as $i => $word) {
            if ($word === '#') {
                return true;
            }
            if (!isset($t[$i])) {
                return false;
            }
            if ($word !== '+' && $word !== $t[$i]) {
                return false;
            }
        }
        return count($f) === count($t);
    }
}
