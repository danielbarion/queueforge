<?php

declare(strict_types=1);

/**
 * Socket readiness for the event loops.
 *
 * On Linux `stream_select` refuses any descriptor at or above FD_SETSIZE
 * (1024), so one process could not hold more than about 1,000 connections.
 * With the `ev` extension loaded this uses its loop (epoll on Linux) instead,
 * keeping one watcher per stream between calls. Without it, `stream_select`.
 *
 * Level-triggered, like select. Callers read TLS streams until they are empty:
 * epoll cannot see records OpenSSL has already decrypted.
 */
final class Poller
{
    /** @var array<string, self> */
    private static array $ev = [];
    private int $pid;
    private \EvLoop $loop;
    private \EvTimer $timer;
    /** @var array<int, \EvIo> */
    private array $reads = [];
    /** @var array<int, \EvIo> */
    private array $writes = [];
    /** @var array<int, true> */
    private array $readable = [];
    /** @var array<int, true> */
    private array $writable = [];

    public static function backend(): string
    {
        return extension_loaded('ev') ? 'ev' : 'select';
    }

    /**
     * Same contract as `stream_select` with an empty except set: keeps the
     * ready streams in $read and $write and returns their count, or false.
     * Each $slot keeps its own watchers, so loops with different stream sets
     * do not rebuild each other's.
     *
     * @param list<resource> $read
     * @param list<resource> $write
     */
    public static function select(array &$read, array &$write, float $wait, string $slot = 'main'): int|false
    {
        if (!extension_loaded('ev')) {
            $except = [];
            $sec = (int) $wait;
            return @stream_select($read, $write, $except, $sec, (int) (($wait - $sec) * 1000000));
        }
        // A forked child must not share its parent's epoll set.
        $ev = self::$ev[$slot] ?? null;
        if ($ev === null || $ev->pid !== getmypid()) $ev = self::$ev[$slot] = new self();
        return $ev->poll($read, $write, $wait);
    }

    /** Waits up to $wait seconds for one stream to become readable. */
    public static function one($fp, float $wait): void
    {
        $read = [$fp];
        $write = [];
        $except = [];
        $sec = (int) $wait;
        // A descriptor past FD_SETSIZE cannot be selected; sleeping instead
        // keeps the caller's retry loop going.
        if (@stream_select($read, $write, $except, $sec, (int) (($wait - $sec) * 1000000)) === false) usleep((int) ($wait * 1000000));
    }

    private function __construct()
    {
        $this->pid = getmypid();
        $this->loop = new \EvLoop();
        $this->timer = $this->loop->timer(0, 0, static function (): void {});
        $this->timer->stop();
    }

    private function poll(array &$read, array &$write, float $wait): int
    {
        $wantRead = [];
        foreach ($read as $fp) $wantRead[(int) $fp] = $fp;
        $wantWrite = [];
        foreach ($write as $fp) $wantWrite[(int) $fp] = $fp;
        // Stop watchers first: a closed stream's descriptor number can already
        // belong to a stream that is about to be watched.
        foreach ($this->reads as $id => $watcher) if (!isset($wantRead[$id])) { $watcher->stop(); unset($this->reads[$id]); }
        foreach ($this->writes as $id => $watcher) if (!isset($wantWrite[$id])) { $watcher->stop(); unset($this->writes[$id]); }
        foreach ($wantRead as $id => $fp) if (!isset($this->reads[$id])) $this->reads[$id] = $this->loop->io($fp, \Ev::READ, function () use ($id): void { $this->readable[$id] = true; });
        foreach ($wantWrite as $id => $fp) if (!isset($this->writes[$id])) $this->writes[$id] = $this->loop->io($fp, \Ev::WRITE, function () use ($id): void { $this->writable[$id] = true; });
        $this->readable = [];
        $this->writable = [];
        if ($wait <= 0) {
            $this->loop->run(\Ev::RUN_NOWAIT);
        } else {
            $this->timer->set($wait, 0);
            $this->timer->start();
            $this->loop->run(\Ev::RUN_ONCE);
            $this->timer->stop();
        }
        $read = [];
        foreach ($this->readable as $id => $_) if (isset($wantRead[$id]) && is_resource($wantRead[$id])) $read[] = $wantRead[$id];
        $write = [];
        foreach ($this->writable as $id => $_) if (isset($wantWrite[$id]) && is_resource($wantWrite[$id])) $write[] = $wantWrite[$id];
        return count($read) + count($write);
    }
}
