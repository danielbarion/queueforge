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
            } elseif ($kind === 10 && strlen($body) >= 2) {
                // UNSUBSCRIBE. Removal is scoped to this connection, so one
                // client cannot cancel another's subscription.
                $at = 2;
                $drop = [];
                while ($at < strlen($body)) {
                    $filter = $this->mqttStr($body, $at);
                    if ($filter === null) {
                        break;
                    }
                    $at = $filter['next'];
                    $drop[] = $filter['text'];
                }
                $this->mqttSubs = array_values(array_filter(
                    $this->mqttSubs,
                    static fn (array $sub): bool => $sub['conn'] !== $conn || !in_array($sub['filter'], $drop, true),
                ));
                $out .= "\xb0\x02" . $body[0] . $body[1];
            } elseif ($kind === 12) {
                $out .= "\xd0\x00";
            } elseif ($kind === 14) {
                // DISCONNECT. The subscriptions go with the connection and
                // the caller closes the socket on an empty buffer.
                $this->dropMqtt($conn);
                $buf = '';
                $this->mqttClosing = true;
                return $out;
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
     * Set when a client sent DISCONNECT, so the caller closes the socket
     * instead of leaving it open.
     */
    public bool $mqttClosing = false;
    public bool $stompClosing = false;

    /** Forgets every MQTT subscription for a connection. */
    public function dropMqtt(int $conn): void
    {
        $this->mqttSubs = array_values(array_filter(
            $this->mqttSubs,
            static fn (array $sub): bool => $sub['conn'] !== $conn,
        ));
    }

    /** Forgets every STOMP subscription for a connection. */
    public function dropStomp(int $conn): void
    {
        $this->stompSubs = array_values(array_filter(
            $this->stompSubs,
            static fn (array $sub): bool => $sub['conn'] !== $conn,
        ));
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
            // content-length is authoritative when present, so a body that
            // contains a newline or a NUL is not truncated at the frame scan.
            $declared = $headers['content-length'] ?? null;
            if ($declared !== null && is_numeric(trim($declared))) {
                $body = substr($body, 0, (int) trim($declared));
            }
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
            } elseif ($cmd === 'UNSUBSCRIBE') {
                // Matched on id and connection, so one client cannot cancel
                // another's subscription.
                $id = $headers['id'] ?? '0';
                $this->stompSubs = array_values(array_filter(
                    $this->stompSubs,
                    static fn (array $sub): bool => $sub['conn'] !== $conn || $sub['id'] !== $id,
                ));
                if (isset($headers['receipt'])) {
                    $out .= "RECEIPT\nreceipt-id:{$headers['receipt']}\n\n\0";
                }
            } elseif ($cmd === 'DISCONNECT') {
                if (isset($headers['receipt'])) {
                    $out .= "RECEIPT\nreceipt-id:{$headers['receipt']}\n\n\0";
                }
                $this->dropStomp($conn);
                $this->stompClosing = true;
                $buf = '';
                return $out;
            } elseif ($cmd !== '') {
                // An unknown command gets an ERROR frame rather than being
                // dropped in silence.
                $out .= "ERROR\nmessage:unknown command " . $cmd . "\n\n\0";
            }
            if (isset($headers['receipt']) && $cmd !== 'DISCONNECT' && $cmd !== 'UNSUBSCRIBE') {
                $out .= "RECEIPT\nreceipt-id:{$headers['receipt']}\n\n\0";
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

    /**
     * The RabbitMQ stream command set.
     *
     * Chunks are kept per stream as raw length-prefixed entries so a
     * subscribe can concatenate them into a chunk body without re-framing.
     * Offsets are not tracked: a subscribe always replays from the start,
     * which is what Bun does.
     *
     * @param array<string, mixed> $state
     */
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
            if ($key === 0x0011) {
                // PeerProperties. A client reads the map back, so an empty
                // body leaves it parsing past the end of the frame.
                $props = pack('N', 2)
                    . self::streamString('product') . self::streamString('RabbitMQ')
                    . self::streamString('version') . self::streamString('4.3.6');
                $out .= $this->streamResp(0x8011, $corr, $props);
            } elseif ($key === 0x0012) {
                // SaslHandshake. The mechanism list is what lets a client
                // choose PLAIN.
                $out .= $this->streamResp(0x8012, $corr, pack('N', 1) . self::streamString('PLAIN'));
            } elseif ($key === 0x0013) {
                // SaslAuthenticate, then an unsolicited Tune so frame-max and
                // the heartbeat interval are negotiated.
                $out .= $this->streamResp(0x8013, $corr, '');
                $tune = pack('n', 0x0014) . pack('n', 1) . pack('N', 1048576) . pack('N', 60);
                $out .= pack('N', strlen($tune)) . $tune;
            } elseif ($key === 0x0015) {
                $out .= $this->streamResp(0x8015, $corr, pack('N', 0));
            } elseif ($key === 0x000d) {
                $name = $this->streamStr($rest, 4)['text'] ?? 'stream';
                $this->streams[$name] = $this->streams[$name] ?? [];
                $this->broker->declareQueue($name);
                $out .= $this->streamResp(0x800d, $corr, '');
            } elseif ($key === 0x0001) {
                // DeclarePublisher. The publisher id has to be remembered so a
                // later Publish can be routed to its stream.
                $at = 4;
                $pid = isset($rest[$at]) ? ord($rest[$at]) : 0;
                $at++;
                $ref = $this->streamStr($rest, $at);
                $at = $ref['next'] ?? $at;
                $name = $this->streamStr($rest, $at);
                $stream = $name['text'] ?? 'stream';
                $state['publishers'] = $state['publishers'] ?? [];
                $state['publishers'][$pid] = $stream;
                $this->streams[$stream] = $this->streams[$stream] ?? [];
                $out .= $this->streamResp(0x8001, $corr, '');
            } elseif ($key === 0x0002) {
                $out .= $this->streamPublish($rest, $state);
            } elseif ($key === 0x0007) {
                // Subscribe, then one Deliver carrying everything stored.
                $at = 4;
                $sub = isset($rest[$at]) ? ord($rest[$at]) : 0;
                $at++;
                $name = $this->streamStr($rest, $at);
                $stream = $name['text'] ?? 'stream';
                $out .= $this->streamResp(0x8007, $corr, '');
                $queued = $this->streams[$stream] ?? [];
                if ($queued !== []) {
                    $out .= $this->streamDeliver($sub, $queued);
                }
            } elseif ($key === 0x0016) {
                $out .= $this->streamResp(0x8016, $corr, '');
                $this->streamClosing = true;
                return $out;
            } elseif ($key === 0x0014 || $key === 0x0017) {
                // The client's Tune response and its heartbeats carry no
                // reply; echoing them would look like a spurious response.
                continue;
            } elseif (($key & 0x8000) === 0) {
                $out .= $this->streamResp($key | 0x8000, $corr, '');
            }
        }
        return $out;
    }

    /** Set when a client sent Close, so the caller ends the socket. */
    public bool $streamClosing = false;

    /**
     * Publish: a publisher id then a count, then that many entries of an
     * 8-byte publishing id and an i32-length payload. Each entry is stored
     * raw and its payload pushed to the broker, then one PublishConfirm
     * (0x0003, not 0x8002) carries the ids back.
     *
     * @param array<string, mixed> $state
     */
    private function streamPublish(string $rest, array &$state): string
    {
        $at = 4;
        if (!isset($rest[$at])) {
            return '';
        }
        $pid = ord($rest[$at]);
        $at++;
        $stream = (string) (($state['publishers'] ?? [])[$pid] ?? 'stream');
        if ($at + 4 > strlen($rest)) {
            return '';
        }
        $count = unpack('N', substr($rest, $at, 4))[1];
        $at += 4;
        $ids = '';
        $sent = 0;
        $this->streams[$stream] = $this->streams[$stream] ?? [];
        for ($i = 0; $i < $count; $i++) {
            if ($at + 12 > strlen($rest)) {
                break;
            }
            // The publishing id is 8 bytes; only the low 32 bits are kept,
            // which is the same truncation Bun applies.
            $low = unpack('N', substr($rest, $at + 4, 4))[1];
            $at += 8;
            $len = unpack('N', substr($rest, $at, 4))[1];
            $at += 4;
            if ($at + $len > strlen($rest)) {
                break;
            }
            $payload = substr($rest, $at, $len);
            $raw = pack('N', $len) . $payload;
            $at += $len;
            $this->streams[$stream][] = $raw;
            $this->broker->declareQueue($stream);
            $this->broker->publish(0, 0, 0, '', $stream, $payload, 1);
            $ids .= pack('N', 0) . pack('N', $low);
            $sent++;
        }
        if ($sent === 0) {
            return '';
        }
        $payload = pack('n', 0x0003) . pack('n', 1) . chr($pid) . pack('N', $sent) . $ids;
        return pack('N', strlen($payload)) . $payload;
    }

    /**
     * A Deliver frame (0x0008) with a chunk header. The header fields that
     * need real bookkeeping — timestamp, CRC, first offset — are zero, since
     * offsets are not tracked.
     *
     * @param list<string> $queued
     */
    private function streamDeliver(int $sub, array $queued): string
    {
        $data = implode('', $queued);
        $n = count($queued);
        $chunk = "\x50\x00"                     // magic and version
            . pack('n', $n)                     // entry count
            . pack('N', $n)                     // record count
            . str_repeat("\x00", 8)             // timestamp
            . pack('N', 0) . pack('N', 1)       // epoch
            . str_repeat("\x00", 8)             // first offset
            . pack('N', 0)                      // crc
            . pack('N', strlen($data))
            . pack('N', 0) . pack('N', 0)
            . $data;
        $payload = pack('n', 0x0008) . pack('n', 1) . chr($sub) . $chunk;
        return pack('N', strlen($payload)) . $payload;
    }

    /** A stream-protocol string: u16 length then the bytes. */
    private static function streamString(string $s): string
    {
        return pack('n', strlen($s)) . $s;
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
