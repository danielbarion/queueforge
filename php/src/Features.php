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
        $hit = static function (string $k, string $v) use ($headers): bool {
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
        $type = $str('x-queue-type') === 'quorum' ? 'quorum' : 'classic';
        $delivery = $num('x-delivery-limit');
        if ($type === 'quorum' && $delivery === null) {
            $delivery = 20;
        }
        return [
            'messageTtl' => $num('x-message-ttl'),
            'maxLength' => $num('x-max-length'),
            'maxLengthBytes' => $num('x-max-length-bytes'),
            'overflow' => $overflow,
            'dlx' => $str('x-dead-letter-exchange'),
            'dlxKey' => $str('x-dead-letter-routing-key'),
            'maxPriority' => $maxPriority !== null && $maxPriority > 0 ? $maxPriority : null,
            'queueType' => $type,
            'deliveryLimit' => $delivery,
        ];
    }

    /** A quorum publish confirms only after this many durable copies. */
    public static function majority(int $members): int
    {
        return intdiv(max($members, 1), 2) + 1;
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
