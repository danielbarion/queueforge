<?php
declare(strict_types=1);

/**
 * Passes a connected TCP socket to another process, once.
 *
 * Unix datagrams keep each message whole, so the file descriptor arrives
 * with the JSON that describes it.
 */
final class Handoff
{
    /** Receive buffer for one datagram. */
    public const RECV_BYTES = 262144;
    /** Largest base64 `bytes` a migrate may carry, leaving room for the JSON around it. */
    public const MAX_BYTES = 200000;

    /** @var resource|null */
    public $stream = null;

    /** @param Socket $sock */
    public function __construct(public $sock, public string $peer)
    {
    }

    public static function bind(string $path): self
    {
        $dir = dirname($path);
        if (!is_dir($dir) && !mkdir($dir, 0777, true) && !is_dir($dir)) {
            throw new RuntimeException("cannot create $dir");
        }
        if (is_file($path) || is_link($path)) {
            @unlink($path);
        }
        $sock = socket_create(AF_UNIX, SOCK_DGRAM, 0);
        if ($sock === false || !socket_bind($sock, $path)) {
            throw new RuntimeException("bind $path failed");
        }
        socket_set_nonblock($sock);
        @socket_set_option($sock, SOL_SOCKET, SO_RCVBUF, 4 * 1024 * 1024);
        @socket_set_option($sock, SOL_SOCKET, SO_SNDBUF, 4 * 1024 * 1024);
        return new self($sock, '');
    }

    /**
     * @param array<string, mixed> $msg
     * @param resource|null $tcp
     */
    public function send(array $msg, $tcp = null): bool
    {
        if ($this->peer === '') {
            return false;
        }
        $json = json_encode($msg);
        if (!is_string($json)) {
            return false;
        }
        $packet = [
            'name' => ['family' => AF_UNIX, 'path' => $this->peer],
            'iov' => [$json],
        ];
        if (is_resource($tcp)) {
            $imported = socket_import_stream($tcp);
            if ($imported === false) {
                return false;
            }
            $packet['control'] = [[
                'level' => SOL_SOCKET,
                'type' => SCM_RIGHTS,
                'data' => [$imported],
            ]];
        }
        $sent = @socket_sendmsg($this->sock, $packet, 0);
        return $sent !== false;
    }

    /**
     * @return array{msg:array<string, mixed>,fp:mixed}|null
     */
    public function recv(): ?array
    {
        $packet = [
            // socket_recvmsg sizes its buffer from buffer_size, not from an
            // iov passed in; without it every datagram is cut to 8192 bytes,
            // which dropped any move carrying more buffered frames than that.
            'buffer_size' => self::RECV_BYTES,
            'controllen' => socket_cmsg_space(SOL_SOCKET, SCM_RIGHTS, 1),
        ];
        $n = @socket_recvmsg($this->sock, $packet, MSG_DONTWAIT);
        if ($n === false || $n <= 0) {
            return null;
        }
        $decoded = json_decode(substr($packet['iov'][0], 0, $n), true);
        if (!is_array($decoded)) {
            return null;
        }
        $fp = null;
        $fd = $packet['control'][0]['data'][0] ?? null;
        if ($fd) {
            $exported = socket_export_stream($fd);
            if (is_resource($exported)) {
                stream_set_blocking($exported, false);
                $fp = $exported;
            }
        }
        return ['msg' => $decoded, 'fp' => $fp];
    }

    /**
     * The queue a method frame names, or null when this frame does not decide a home.
     *
     * A publish on the default exchange names its queue in the routing key.
     * A consume names it directly. Any other frame stays on this process.
     */
    public static function namedQueue(string $buf, int $at): ?string
    {
        if (strlen($buf) - $at < 11 || ($buf[$at] ?? '') !== "\x01") {
            return null;
        }
        $size = unpack('N', substr($buf, $at + 3, 4))[1];
        if (strlen($buf) - $at < 8 + $size) {
            return null;
        }
        $class = unpack('n', substr($buf, $at + 7, 2))[1];
        $method = unpack('n', substr($buf, $at + 9, 2))[1];
        $o = $at + 11;
        if ($class === 60 && $method === 40) {
            $o += 2;
            $exchange = self::short($buf, $o);
            if ($exchange === null || $exchange !== '') {
                return null;
            }
            $queue = self::short($buf, $o);
            return $queue === null || $queue === '' ? null : $queue;
        }
        if ($class === 60 && $method === 20) {
            $o += 2;
            $queue = self::short($buf, $o);
            return $queue === null || $queue === '' ? null : $queue;
        }
        return null;
    }

    private static function short(string $buf, int &$o): ?string
    {
        if (!isset($buf[$o])) {
            return null;
        }
        $n = ord($buf[$o]);
        $o++;
        if (strlen($buf) < $o + $n) {
            return null;
        }
        $s = substr($buf, $o, $n);
        $o += $n;
        return $s;
    }
}
