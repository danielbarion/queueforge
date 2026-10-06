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

    public static function deliver(int $channel, string $tag, int $deliveryTag, string $routingKey, int $deliveryMode, string $body, bool $redelivered = false, string $exchange = ''): string
    {
        $method = self::shortstr($tag) . self::u64($deliveryTag) . chr($redelivered ? 1 : 0) . self::shortstr($exchange) . self::shortstr($routingKey);
        $header = pack('nn', 60, 0) . self::u64(strlen($body)) . pack('n', 0x1000) . chr($deliveryMode);
        return self::method($channel, 60, 60, $method)
            . self::frame(2, $channel, $header)
            . self::frame(3, $channel, $body);
    }

    public static function readShortstr(string $buf, int &$o): string
    {
        $n = ord($buf[$o]);
        $o += 1;
        $s = substr($buf, $o, $n);
        $o += $n;
        return $s;
    }

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
            if ($type === 'S') {
                $out[$name] = self::readLongstr($buf, $o);
            } elseif ($type === 's') {
                $out[$name] = self::readShortstr($buf, $o);
            } elseif ($type === 't') {
                $out[$name] = ord($buf[$o]) !== 0 ? 1 : 0;
                $o++;
            } elseif ($type === 'I' || $type === 'i') {
                $val = unpack('N', substr($buf, $o, 4))[1];
                if ($val >= 0x80000000) {
                    $val -= 0x100000000;
                }
                $out[$name] = $val;
                $o += 4;
            } else {
                break;
            }
        }
        $o = $end;
        return $out;
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
