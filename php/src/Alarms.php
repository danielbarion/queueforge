<?php

declare(strict_types=1);

/**
 * Memory and disk alarms, as RabbitMQ raises them.
 *
 * `check()` samples this process's memory and the data directory's free
 * disk. While either is past its limit, the server stops reading from
 * connections that have published, and tells clients that advertised
 * `connection.blocked`. Consumers keep running so the backlog can drain.
 * Bun does the same (bun/src/broker/alarms.ts).
 */
final class Alarms
{
    /** RabbitMQ's defaults: 0.6 of RAM, 50 MB of free disk. */
    public const MEM_RELATIVE = 0.6;
    public const DISK_FREE_LIMIT = 50_000_000;

    public bool $memAlarm = false;
    public bool $diskAlarm = false;
    public int $memUsed = 0;
    public ?int $diskFree = null;
    public readonly int $memLimit;

    public function __construct(private string $dataDir)
    {
        $this->memLimit = self::memLimit();
    }

    public function blocked(): bool
    {
        return $this->memAlarm || $this->diskAlarm;
    }

    /** The `connection.blocked` reason, as RabbitMQ words it. */
    public function reason(): string
    {
        return $this->memAlarm ? 'low on memory' : ($this->diskAlarm ? 'low on disk' : '');
    }

    /** Sample both resources. Returns true when the blocked state changed. */
    public function check(): bool
    {
        $before = $this->blocked();
        $this->memUsed = memory_get_usage(true);
        $free = @disk_free_space($this->dataDir);
        $this->diskFree = $free === false ? null : (int) $free;
        $this->memAlarm = $this->memUsed >= $this->memLimit;
        $this->diskAlarm = $this->diskFree !== null && $this->diskFree < self::DISK_FREE_LIMIT;
        return $this->blocked() !== $before;
    }

    /**
     * The memory limit in bytes. `QUEUEFORGE_MEM_HIGH_WATERMARK` overrides the
     * fraction of RAM, or gives an absolute byte count when it is above 1. RAM
     * is the cgroup limit in a container, else the machine's.
     */
    public static function memLimit(): int
    {
        $raw = getenv('QUEUEFORGE_MEM_HIGH_WATERMARK');
        $value = is_string($raw) && is_numeric($raw) ? (float) $raw : 0.0;
        if ($value > 1) return (int) $value;
        $fraction = $value > 0 ? $value : self::MEM_RELATIVE;
        $total = self::totalMemory();
        return $total === null ? PHP_INT_MAX : (int) ($total * $fraction);
    }

    private static function totalMemory(): ?int
    {
        foreach (['/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/memory/memory.limit_in_bytes'] as $path) {
            $text = @file_get_contents($path);
            if (is_string($text) && ctype_digit(trim($text)) && (int) trim($text) < (1 << 60)) return (int) trim($text);
        }
        $info = @file_get_contents('/proc/meminfo');
        if (is_string($info) && preg_match('/^MemTotal:\s+(\d+) kB/m', $info, $m)) return (int) $m[1] * 1024;
        if (PHP_OS_FAMILY === 'Darwin') {
            $out = @shell_exec('/usr/sbin/sysctl -n hw.memsize 2>/dev/null');
            if (is_string($out) && ctype_digit(trim($out))) return (int) trim($out);
        }
        return null;
    }
}
