<?php
declare(strict_types=1);

require_once __DIR__ . '/Amqp.php';

/**
 * Shared test scaffolding: spawns a broker on a free port, waits for it to
 * listen, and collects assertion results. Each test file is a standalone
 * script that exits 0 on success, so the runner needs no framework.
 */
final class Harness
{
    private static int $passed = 0;
    /** @var list<string> */
    private static array $failures = [];
    /** @var list<array{proc:mixed,dir:string}> */
    private static array $running = [];

    /** Picks a free localhost port by binding one and letting it go. */
    public static function freePort(): int
    {
        $fp = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if ($fp === false) {
            throw new RuntimeException("cannot reserve a port: $errstr");
        }
        $name = stream_socket_get_name($fp, false);
        fclose($fp);
        $pos = strrpos((string) $name, ':');
        return $pos === false ? 0 : (int) substr((string) $name, $pos + 1);
    }

    /**
     * Starts a broker and returns its handle. Extra config lines are appended
     * verbatim, so a test can turn on management, cluster, or MQTT listeners.
     *
     * @param list<string> $extraListeners
     * @param array{listen?:string,members?:list<array{id:string,addr:string}>} $cluster
     * @return array{proc:mixed,dir:string,port:int}
     */
    public static function broker(array $extraListeners = [], int $fsyncMs = 10, string $nodeId = '', array $cluster = []): array
    {
        $port = self::freePort();
        $dir = sys_get_temp_dir() . '/qf-test-' . bin2hex(random_bytes(6));
        if (!mkdir($dir, 0777, true) && !is_dir($dir)) {
            throw new RuntimeException("cannot create $dir");
        }
        $lines = ['[listeners]', "amqp = \"127.0.0.1:$port\""];
        foreach ($extraListeners as $line) {
            $lines[] = $line;
        }
        $lines[] = '';
        $lines[] = '[data]';
        $lines[] = "dir = \"$dir/data\"";
        $lines[] = 'fsync_policy = "every_n_ms"';
        $lines[] = "fsync_interval_ms = $fsyncMs";
        if ($nodeId !== '' || $cluster !== []) {
            $lines[] = '';
            $lines[] = '[cluster]';
            if ($nodeId !== '') {
                $lines[] = "node_id = \"$nodeId\"";
            }
            if (isset($cluster['listen'])) {
                $lines[] = 'listen = "' . $cluster['listen'] . '"';
            }
            if (isset($cluster['members']) && $cluster['members'] !== []) {
                $lines[] = 'members = [';
                foreach ($cluster['members'] as $member) {
                    $lines[] = '  { id = "' . $member['id'] . '", addr = "' . $member['addr'] . '" },';
                }
                $lines[] = ']';
            }
        }
        file_put_contents("$dir/config.toml", implode("\n", $lines) . "\n");

        $root = dirname(__DIR__, 2);
        $cmd = ['php', "$root/bin/queueforge", '--config', "$dir/config.toml", '--dev-bootstrap'];
        $proc = proc_open($cmd, [1 => ['file', "$dir/out.log", 'w'], 2 => ['file', "$dir/err.log", 'w']], $pipes);
        if (!is_resource($proc)) {
            throw new RuntimeException('cannot start the broker');
        }
        $handle = ['proc' => $proc, 'dir' => $dir, 'port' => $port];
        self::$running[] = ['proc' => $proc, 'dir' => $dir];
        self::waitForPort($port, $dir);
        return $handle;
    }

    private static function waitForPort(int $port, string $dir): void
    {
        $deadline = microtime(true) + 10.0;
        while (microtime(true) < $deadline) {
            $fp = @stream_socket_client("tcp://127.0.0.1:$port", $errno, $errstr, 0.1);
            if ($fp !== false) {
                fclose($fp);
                return;
            }
            usleep(50000);
        }
        $err = @file_get_contents("$dir/err.log");
        throw new RuntimeException("broker did not listen on $port: " . trim((string) $err));
    }

    /** @param array{proc:mixed,dir:string,port:int} $handle */
    public static function stop(array $handle, bool $kill = false): void
    {
        if (is_resource($handle['proc'])) {
            proc_terminate($handle['proc'], $kill ? 9 : 15);
            proc_close($handle['proc']);
        }
        foreach (self::$running as $i => $row) {
            if ($row['proc'] === $handle['proc']) {
                unset(self::$running[$i]);
            }
        }
        self::$running = array_values(self::$running);
    }

    /** Reads the broker's stderr, for diagnosing a failed check. */
    public static function stderr(array $handle): string
    {
        return trim((string) @file_get_contents($handle['dir'] . '/err.log'));
    }

    public static function ok(string $label, bool $condition, string $detail = ''): void
    {
        if ($condition) {
            self::$passed++;
            fwrite(STDOUT, "  ok   $label\n");
            return;
        }
        self::$failures[] = $label . ($detail === '' ? '' : " ($detail)");
        fwrite(STDOUT, "  FAIL $label" . ($detail === '' ? '' : " -- $detail") . "\n");
    }

    public static function eq(string $label, mixed $expected, mixed $actual): void
    {
        $same = $expected === $actual;
        self::ok($label, $same, $same ? '' : 'expected ' . self::show($expected) . ', got ' . self::show($actual));
    }

    private static function show(mixed $value): string
    {
        if (is_string($value)) {
            return strlen($value) > 60 ? '"' . substr($value, 0, 57) . '..."' : '"' . $value . '"';
        }
        return var_export($value, true);
    }

    /** Ends the test file, reporting via the exit code. */
    public static function done(): never
    {
        foreach (self::$running as $row) {
            if (is_resource($row['proc'])) {
                proc_terminate($row['proc'], 9);
                proc_close($row['proc']);
            }
        }
        if (self::$failures === []) {
            fwrite(STDOUT, '  ' . self::$passed . " checks passed\n");
            exit(0);
        }
        fwrite(STDERR, '  ' . count(self::$failures) . " of " . (self::$passed + count(self::$failures)) . " checks failed\n");
        exit(1);
    }

    /** Runs a closure, turning an exception into a failed check. */
    public static function guard(string $label, callable $fn): void
    {
        try {
            $fn();
        } catch (Throwable $e) {
            self::ok($label, false, $e->getMessage());
        }
    }
}
