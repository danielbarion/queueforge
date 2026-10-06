<?php
declare(strict_types=1);

/**
 * A minimal AMQP 1.0 shim, enough for a 1.0 client to attach a link and move
 * messages through the same broker as 0-9-1.
 *
 * Performatives are recognised by scanning the frame body for a smallulong
 * descriptor (0x00 0x53 <code>) rather than by decoding the type system. That
 * is a heuristic — those bytes inside a payload would read as a false match —
 * but it is the same approach Bun takes, and it keeps the shim small.
 *
 * Not implemented: disposition, detach, end, credit accounting (one flow
 * yields one message), multi-frame transfers, and amqp-value or amqp-sequence
 * bodies. Lengths are single-byte, so a payload over 255 bytes is not framed.
 */
final class Amqp10
{
    public function __construct(private Broker $broker)
    {
    }

    /**
     * Drives a 1.0 connection. The buffer is taken by reference so a partial
     * frame stays behind for the next read.
     *
     * @param array<string, mixed> $state per-connection phase and link state
     * @return string bytes to write back
     */
    public function drive(string &$buf, array &$state): string
    {
        $out = '';
        $phase = (string) ($state['phase'] ?? 'header');
        if ($phase === 'header') {
            if (strlen($buf) < 8) {
                return '';
            }
            // Byte 4 is 3 for the SASL layer and 0 for the plain one.
            $sasl = ord($buf[4]) === 3;
            $buf = substr($buf, 8);
            if ($sasl) {
                $out .= "AMQP\x03\x01\x00\x00";
                // sasl-mechanisms: descriptor 0x40, a list of one symbol.
                $mechs = "\xc0" . chr(5) . chr(1) . "\xa3" . chr(5) . 'PLAIN';
                $out .= $this->frame(1, "\x00\x53\x40" . $mechs);
                $state['phase'] = 'sasl';
            } else {
                $out .= "AMQP\x00\x01\x00\x00";
                $state['phase'] = 'open';
            }
            return $out . $this->drive($buf, $state);
        }
        while (strlen($buf) >= 8) {
            // After SASL the client resends the plain header.
            if ($buf[0] === 'A' && ord($buf[4]) === 0 && ord($buf[5]) === 1) {
                $buf = substr($buf, 8);
                $out .= "AMQP\x00\x01\x00\x00";
                $state['phase'] = 'open';
                continue;
            }
            $size = unpack('N', substr($buf, 0, 4))[1];
            if ($size < 8 || strlen($buf) < $size) {
                break;
            }
            $body = substr($buf, 8, $size - 8);
            $buf = substr($buf, $size);
            $out .= $this->onFrame($body, $state);
        }
        return $out;
    }

    /**
     * @param array<string, mixed> $state
     */
    private function onFrame(string $body, array &$state): string
    {
        if (self::has($body, 0x41) && ($state['phase'] ?? '') === 'sasl') {
            // sasl-init: answer sasl-outcome with code 0 and null details.
            return $this->frame(1, "\x00\x53\x44" . "\xc0" . chr(4) . chr(2) . "\x50\x00" . "\x40");
        }
        $out = '';
        if (self::has($body, 0x10)) {
            $out .= $this->performative(0x10);
        }
        if (self::has($body, 0x11)) {
            $out .= $this->performative(0x11);
        }
        if (self::has($body, 0x12)) {
            $queue = self::queueFrom($body);
            if ($queue !== null) {
                // A body carrying boolean true and a source descriptor is the
                // receiving end of the link; anything else is a sender.
                if (str_contains($body, "\x41") && str_contains($body, "\x28")) {
                    $state['receiver'] = $queue;
                } else {
                    $state['sender'] = $queue;
                }
            }
            $out .= $this->performative(0x12);
            if (isset($state['sender']) && !isset($state['receiver'])) {
                $out .= $this->performative(0x13);
            }
        }
        $data = self::dataSection($body);
        if ($data !== null && isset($state['sender'])) {
            $queue = (string) $state['sender'];
            $this->broker->declareQueue($queue);
            $this->broker->publish(0, 0, 0, '', $queue, $data, 1);
        }
        if (self::has($body, 0x13) && isset($state['receiver'])) {
            $queued = $this->broker->pullBody((string) $state['receiver']);
            if ($queued !== null) {
                $out .= $this->transfer($queued);
            }
        }
        if (self::has($body, 0x18)) {
            $out .= $this->performative(0x18);
            $state['closing'] = true;
        }
        return $out;
    }

    /** Whether the body carries a smallulong descriptor with this code. */
    private static function has(string $body, int $code): bool
    {
        return str_contains($body, "\x00\x53" . chr($code));
    }

    /**
     * The link's queue name. An attach body names the address as a str8
     * holding /queues/<name>, so the marker is located and the length read
     * from the byte before it.
     */
    private static function queueFrom(string $body): ?string
    {
        $at = strpos($body, '/queues/');
        if ($at === false || $at < 2 || $body[$at - 2] !== "\xa1") {
            return null;
        }
        $len = ord($body[$at - 1]);
        $name = substr($body, $at + 8, $len - 8);
        return $name === '' ? null : $name;
    }

    /** The payload of a data section, if the body has one. */
    private static function dataSection(string $body): ?string
    {
        $at = strpos($body, "\x00\x53\x75\xa0");
        if ($at === false || !isset($body[$at + 4])) {
            return null;
        }
        $len = ord($body[$at + 4]);
        return substr($body, $at + 5, $len);
    }

    /** A performative with an empty described list. */
    private function performative(int $code): string
    {
        return $this->frame(0, "\x00\x53" . chr($code) . "\x45");
    }

    /** A transfer carrying one message body. */
    private function transfer(string $payload): string
    {
        $fields = "\x52\x00"        // handle
            . "\x43"                // delivery-id, zero
            . "\xa0" . chr(1) . chr(1) // delivery-tag
            . "\x43"                // message-format
            . "\x41";               // settled
        $list = "\xc0" . chr(strlen($fields) + 1) . chr(5) . $fields;
        $data = "\x00\x53\x75\xa0" . chr(strlen($payload)) . $payload;
        return $this->frame(0, "\x00\x53\x14" . $list . $data);
    }

    /** An AMQP 1.0 frame: size, DOFF 2, type, then a zero channel. */
    private function frame(int $type, string $body): string
    {
        return pack('N', strlen($body) + 8) . chr(2) . chr($type) . "\x00\x00" . $body;
    }
}
