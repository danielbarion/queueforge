<?php
declare(strict_types=1);

/** MQTT 3.1.1, STOMP 1.2, and the RabbitMQ stream command set used by the Bun listeners. */
final class Protocols
{
    /** @var list<array{filter:string,conn:int,fp:resource}> */
    public array $mqttSubs = [];
    /** @var list<array{destination:string,id:string,conn:int,fp:resource}> */
    public array $stompSubs = [];
    /** @var array<string, list<string>> */
    public array $streams = [];
    private int $mqttConn = 1;
    private int $stompConn = 1;

    public function __construct(public Broker $broker)
    {
    }

    /**
     * Handles whole MQTT packets in the buffer and leaves any partial tail
     * behind. The buffer is taken by reference because a packet larger than
     * one TCP read arrives in pieces, and discarding the remainder lost it.
     *
     * @param resource $fp
     */
    public function mqtt(string &$buf, $fp, int $conn): string
    {
        $out = '';
        // A fixed header plus a one-byte remaining length is two bytes, which
        // is exactly what PINGREQ and DISCONNECT are. Requiring more than two
        // meant neither was ever handled.
        while (strlen($buf) >= 2) {
            $value = 0;
            $shift = 0;
            $i = 1;
            $done = false;
            while ($i < strlen($buf) && $shift < 28) {
                $byte = ord($buf[$i]);
                $value += ($byte & 0x7f) << $shift;
                $i++;
                if (($byte & 0x80) === 0) {
                    $done = true;
                    break;
                }
                $shift += 7;
            }
            if (!$done || strlen($buf) < $i + $value) {
                break;
            }
            $kind = ord($buf[0]) >> 4;
            $body = substr($buf, $i, $value);
            $buf = substr($buf, $i + $value);
            if ($kind === 1) {
                $out .= "\x20\x02\x00\x00";
            } elseif ($kind === 3) {
                $topic = $this->mqttStr($body, 0);
                if ($topic === null) {
                    continue;
                }
                $payload = substr($body, $topic['next']);
                $this->broker->declareQueue($topic['text']);
                $this->broker->publish(0, 0, 0, '', $topic['text'], $payload, 1);
                $frame = $this->mqttPublish($topic['text'], $payload);
                foreach ($this->mqttSubs as $sub) {
                    if (Features::mqttMatch($sub['filter'], $topic['text'])) {
                        $out .= $sub['fp'] === $fp ? $frame : '';
                        if ($sub['fp'] !== $fp) {
                            @fwrite($sub['fp'], $frame);
                        }
                    }
                }
            } elseif ($kind === 8 && strlen($body) >= 2) {
                $id0 = $body[0];
                $id1 = $body[1];
                $at = 2;
                $codes = '';
                $last = '';
                while ($at < strlen($body)) {
                    $filter = $this->mqttStr($body, $at);
                    if ($filter === null) {
                        break;
                    }
                    $at = $filter['next'] + 1;
                    $last = $filter['text'];
                    $this->mqttSubs[] = ['filter' => $filter['text'], 'conn' => $conn, 'fp' => $fp];
                    $codes .= "\x00";
                }
                $rest = $id0 . $id1 . $codes;
                $out .= "\x90" . self::mqttLen(strlen($rest)) . $rest;
                if ($last !== '') {
                    $queued = $this->broker->pullBody($last);
                    if ($queued !== null) {
                        $out .= $this->mqttPublish($last, $queued);
                    }
                }
            } elseif ($kind === 12) {
                $out .= "\xd0\x00";
            }
        }
        return $out;
    }

    public function nextMqtt(): int
    {
        return $this->mqttConn++;
    }

    public function nextStomp(): int
    {
        return $this->stompConn++;
    }

    /**
     * Handles whole STOMP frames and leaves a partial tail buffered, for the
     * same reason as mqtt().
     *
     * @param resource $fp
     */
    public function stomp(string &$buf, $fp, int $conn): string
    {
        $out = '';
        $text = $buf;
        $consumed = 0;
        while (($nul = strpos($text, "\0")) !== false) {
            $frame = substr($text, 0, $nul);
            $text = substr($text, $nul + 1);
            $consumed += $nul + 1;
            $lines = array_map(static fn (string $l): string => rtrim($l, "\r"), explode("\n", $frame));
            $cmd = $lines[0] ?? '';
            $headers = [];
            $bodyAt = count($lines);
            for ($i = 1; $i < count($lines); $i++) {
                if ($lines[$i] === '') {
                    $bodyAt = $i + 1;
                    break;
                }
                $sep = strpos($lines[$i], ':');
                if ($sep !== false && $sep > 0) {
                    $headers[substr($lines[$i], 0, $sep)] = substr($lines[$i], $sep + 1);
                }
            }
            $body = implode("\n", array_slice($lines, $bodyAt));
            if ($cmd === 'CONNECT' || $cmd === 'STOMP') {
                $out .= "CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\0";
            } elseif ($cmd === 'SEND') {
                $dest = $headers['destination'] ?? '';
                $queue = preg_replace('#^/(queue|topic)/#', '', $dest) ?? $dest;
                $this->broker->declareQueue($queue);
                $this->broker->publish(0, 0, 0, '', $queue, $body, 1);
                foreach ($this->stompSubs as $sub) {
                    $subQueue = preg_replace('#^/(queue|topic)/#', '', $sub['destination']) ?? $sub['destination'];
                    if ($sub['destination'] === $dest || $subQueue === $queue) {
                        $msg = "MESSAGE\nsubscription:{$sub['id']}\ndestination:{$dest}\ncontent-length:" . strlen($body) . "\n\n{$body}\0";
                        if ($sub['fp'] === $fp) {
                            $out .= $msg;
                        } else {
                            @fwrite($sub['fp'], $msg);
                        }
                    }
                }
            } elseif ($cmd === 'SUBSCRIBE') {
                $id = $headers['id'] ?? '0';
                $dest = $headers['destination'] ?? '';
                $this->stompSubs[] = ['destination' => $dest, 'id' => $id, 'conn' => $conn, 'fp' => $fp];
                $queue = preg_replace('#^/(queue|topic)/#', '', $dest) ?? $dest;
                $queued = $this->broker->pullBody($queue);
                if ($queued !== null) {
                    $out .= "MESSAGE\nsubscription:{$id}\ndestination:{$dest}\ncontent-length:" . strlen($queued) . "\n\n{$queued}\0";
                }
            } elseif ($cmd === 'DISCONNECT') {
                $buf = '';
                return $out;
            }
        }
        $buf = substr($buf, $consumed);
        return $out;
    }

    /** @return array{text:string,next:int}|null */
    private function mqttStr(string $buf, int $at): ?array
    {
        if ($at + 2 > strlen($buf)) {
            return null;
        }
        $n = (ord($buf[$at]) << 8) | ord($buf[$at + 1]);
        $end = $at + 2 + $n;
        if ($end > strlen($buf)) {
            return null;
        }
        return ['text' => substr($buf, $at + 2, $n), 'next' => $end];
    }

    /**
     * MQTT remaining length: seven bits per byte, low group first, with 0x80
     * set on every byte but the last. A single chr() only works below 128,
     * which silently corrupted any larger packet.
     */
    private static function mqttLen(int $n): string
    {
        $out = '';
        do {
            $byte = $n % 128;
            $n = intdiv($n, 128);
            $out .= chr($n > 0 ? $byte | 0x80 : $byte);
        } while ($n > 0);
        return $out;
    }

    private function mqttPublish(string $topic, string $payload): string
    {
        $rest = pack('n', strlen($topic)) . $topic . $payload;
        return "\x30" . self::mqttLen(strlen($rest)) . $rest;
    }

    /** @param array<string, mixed> $state */
    public function stream(string &$buf, array &$state): string
    {
        $out = '';
        while (strlen($buf) >= 4) {
            $size = unpack('N', substr($buf, 0, 4))[1];
            if (strlen($buf) < 4 + $size) {
                break;
            }
            $frame = substr($buf, 4, $size);
            $buf = substr($buf, 4 + $size);
            if (strlen($frame) < 4) {
                continue;
            }
            $key = unpack('n', substr($frame, 0, 2))[1];
            $rest = substr($frame, 4);
            $corr = strlen($rest) >= 4 ? unpack('N', substr($rest, 0, 4))[1] : 0;
            if ($key === 0x0015) {
                $out .= $this->streamResp(0x8015, $corr, pack('N', 0));
            } elseif ($key === 0x000d) {
                $name = $this->streamStr($rest, 4)['text'] ?? 'stream';
                $this->streams[$name] = $this->streams[$name] ?? [];
                $this->broker->declareQueue($name);
                $out .= $this->streamResp(0x800d, $corr, '');
            } elseif (($key & 0x8000) === 0) {
                $out .= $this->streamResp($key | 0x8000, $corr, '');
            }
        }
        return $out;
    }

    /** @return array{text:string,next:int}|null */
    private function streamStr(string $buf, int $at): ?array
    {
        if ($at + 2 > strlen($buf)) {
            return null;
        }
        $n = (ord($buf[$at]) << 8) | ord($buf[$at + 1]);
        if ($at + 2 + $n > strlen($buf)) {
            return null;
        }
        return ['text' => substr($buf, $at + 2, $n), 'next' => $at + 2 + $n];
    }

    private function streamResp(int $key, int $corr, string $extra): string
    {
        $payload = pack('n', $key) . pack('n', 1) . pack('N', $corr) . pack('n', 1) . $extra;
        return pack('N', strlen($payload)) . $payload;
    }
}
