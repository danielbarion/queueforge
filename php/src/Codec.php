<?php
declare(strict_types=1);

/** AMQP 0-9-1 frame helpers for the classic load path. */
final class Codec
{
    public static function u64(int $n): string
    {
        return pack('NN', ($n >> 32) & 0xffffffff, $n & 0xffffffff);
    }

    public static function readU64(string $buf, int $o): int
    {
        $p = unpack('Nhi/Nlo', substr($buf, $o, 8));
        return ($p['hi'] << 32) | $p['lo'];
    }

    public static function frame(int $type, int $channel, string $payload): string
    {
        return chr($type) . pack('n', $channel) . pack('N', strlen($payload)) . $payload . "\xce";
    }

    public static function method(int $channel, int $class, int $method, string $args = ''): string
    {
        return self::frame(1, $channel, pack('nn', $class, $method) . $args);
    }

    public static function shortstr(string $s): string
    {
        if (strlen($s) > 255) {
            $s = substr($s, 0, 255);
        }
        return chr(strlen($s)) . $s;
    }

    public static function longstr(string $s): string
    {
        return pack('N', strlen($s)) . $s;
    }

    public static function connectionStart(): string
    {
        $args = chr(0) . chr(9) . pack('N', 0) . self::longstr('PLAIN') . self::longstr('en_US');
        return self::method(0, 10, 10, $args);
    }

    public static function connectionTune(): string
    {
        return self::method(0, 10, 30, pack('nNn', 2047, 131072, 60));
    }

    public static function connectionOpenOk(): string
    {
        return self::method(0, 10, 41, self::shortstr(''));
    }

    public static function connectionCloseOk(): string
    {
        return self::method(0, 10, 51);
    }

    public static function channelOpenOk(int $channel): string
    {
        return self::method($channel, 20, 11, pack('N', 0));
    }

    public static function channelCloseOk(int $channel): string
    {
        return self::method($channel, 20, 41);
    }

    public static function queueDeclareOk(int $channel, string $name, int $messages, int $consumers): string
    {
        return self::method($channel, 50, 11, self::shortstr($name) . pack('NN', $messages, $consumers));
    }

    public static function confirmSelectOk(int $channel): string
    {
        return self::method($channel, 85, 11);
    }

    public static function qosOk(int $channel): string
    {
        return self::method($channel, 60, 11);
    }

    public static function consumeOk(int $channel, string $tag): string
    {
        return self::method($channel, 60, 21, self::shortstr($tag));
    }

    public static function basicAck(int $channel, int $tag): string
    {
        return self::method($channel, 60, 80, self::u64($tag) . chr(0));
    }

    /**
     * Builds a content header. When the publisher's raw property bytes are
     * known they are re-emitted verbatim, so content-type, headers,
     * correlation-id and the rest survive the round trip. Bun keeps the same
     * opaque slice from the property flag word onward.
     *
     * $propRaw starts at the property flag word and runs to the end of the
     * publisher's content header, continuation flag words included.
     */
    public static function contentHeader(int $bodyLen, int $deliveryMode, ?string $propRaw = null): string
    {
        $props = $propRaw !== null && $propRaw !== ''
            ? $propRaw
            : pack('n', 0x1000) . chr($deliveryMode);
        return pack('nn', 60, 0) . self::u64($bodyLen) . $props;
    }

    public static function deliver(int $channel, string $tag, int $deliveryTag, string $routingKey, int $deliveryMode, string $body, bool $redelivered = false, string $exchange = '', ?string $propRaw = null): string
    {
        $method = self::shortstr($tag) . self::u64($deliveryTag) . chr($redelivered ? 1 : 0) . self::shortstr($exchange) . self::shortstr($routingKey);
        return self::method($channel, 60, 60, $method)
            . self::frame(2, $channel, self::contentHeader(strlen($body), $deliveryMode, $propRaw))
            . self::frame(3, $channel, $body);
    }

    /** Heartbeat frame: type 8 on channel 0 with an empty payload. */
    public static function heartbeat(): string
    {
        return self::frame(8, 0, '');
    }

    public static function readShortstr(string $buf, int &$o): string
    {
        $n = ord($buf[$o]);
        $o += 1;
        $s = substr($buf, $o, $n);
        $o += $n;
        return $s;
    }

    /**
     * Reads a field table. Every AMQP field type advances the offset by the
     * right width, so a type this broker does not keep cannot truncate the
     * fields behind it. Only scalars reach the result, which keeps the
     * name/value shape the broker and Features::parseArgs expect.
     *
     * Bun reads the same type set in bun/src/codec.ts:174-231, but its 'F'
     * branch reads the nested size without skipping the nested body, so its
     * fields after a nested table are misparsed. This skips the body.
     *
     * @return array<string, string|int|bool>
     */
    public static function readTable(string $buf, int &$o): array
    {
        if ($o + 4 > strlen($buf)) {
            return [];
        }
        $size = unpack('N', substr($buf, $o, 4))[1];
        $o += 4;
        $end = min(strlen($buf), $o + $size);
        $out = [];
        while ($o < $end) {
            $name = self::readShortstr($buf, $o);
            if ($o >= $end) {
                break;
            }
            $type = $buf[$o];
            $o++;
            $value = self::readField($buf, $o, $type, $end);
            if (is_scalar($value)) {
                $out[$name] = $value;
            }
        }
        $o = $end;
        return $out;
    }

    /**
     * Reads one field value and advances past it. Returns null for the types
     * this broker does not keep, having consumed their bytes.
     */
    private static function readField(string $buf, int &$o, string $type, int $end): string|int|bool|null
    {
        if ($type === 'S') {
            return self::readLongstr($buf, $o);
        }
        if ($type === 's') {
            return self::readShortstr($buf, $o);
        }
        if ($type === 't') {
            // Kept as 1/0, not a bool, so the (string) cast the broker applies
            // to header values stays "1"/"0" rather than "1"/"".
            $value = isset($buf[$o]) && ord($buf[$o]) !== 0 ? 1 : 0;
            $o++;
            return $value;
        }
        if ($type === 'b' || $type === 'B') {
            if (!isset($buf[$o])) {
                $o = $end;
                return null;
            }
            $value = ord($buf[$o]);
            $o++;
            return $type === 'b' && $value >= 0x80 ? $value - 0x100 : $value;
        }
        if ($type === 'U' || $type === 'u') {
            if ($o + 2 > $end) {
                $o = $end;
                return null;
            }
            $value = unpack('n', substr($buf, $o, 2))[1];
            $o += 2;
            return $type === 'U' && $value >= 0x8000 ? $value - 0x10000 : $value;
        }
        if ($type === 'I' || $type === 'i') {
            if ($o + 4 > $end) {
                $o = $end;
                return null;
            }
            $value = unpack('N', substr($buf, $o, 4))[1];
            $o += 4;
            return $type === 'I' && $value >= 0x80000000 ? $value - 0x100000000 : $value;
        }
        if ($type === 'l' || $type === 'L') {
            if ($o + 8 > $end) {
                $o = $end;
                return null;
            }
            $value = self::readU64($buf, $o);
            $o += 8;
            return $value;
        }
        if ($type === 'x') {
            return self::readLongstr($buf, $o);
        }
        if ($type === 'V') {
            return null;
        }
        if ($type === 'T') {
            $o += 8;
            return null;
        }
        if ($type === 'd') {
            $o += 8;
            return null;
        }
        if ($type === 'D') {
            $o += 5;
            return null;
        }
        if ($type === 'f') {
            $o += 4;
            return null;
        }
        if ($type === 'F' || $type === 'A') {
            if ($o + 4 > $end) {
                $o = $end;
                return null;
            }
            $inner = unpack('N', substr($buf, $o, 4))[1];
            $o = min($end, $o + 4 + $inner);
            return null;
        }
        // An unknown type has no width, so the rest of this table is unreadable.
        $o = $end;
        return null;
    }

    public static function basicNack(int $channel, int $tag): string
    {
        return self::method($channel, 60, 120, self::u64($tag) . chr(0));
    }

    public static function readLongstr(string $buf, int &$o): string
    {
        $n = unpack('N', substr($buf, $o, 4))[1];
        $o += 4;
        $s = substr($buf, $o, $n);
        $o += $n;
        return $s;
    }
}
