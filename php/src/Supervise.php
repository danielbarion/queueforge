<?php
declare(strict_types=1);

/**
 * One PHP process per core the cgroup grants.
 *
 * This process accepts AMQP and hands each socket to a child. A child that
 * does not own the queue hands the socket on, once, to the child that does.
 * After that the message is not forwarded.
 */
final class Supervise
{
    /** @param array<string, mixed> $cfg */
    public static function run(string $script, string $configPath, bool $dev, array $cfg): void
    {
        $cores = Cores::granted();
        if (function_exists('posix_setpgid')) {
            posix_setpgid(0, 0);
        }
        $plans = Cores::plans($cores, (string) $cfg['dir']);
        $members = [];
        foreach ($plans as $plan) {
            $members[] = ['id' => $plan['id'], 'addr' => $plan['cluster']];
        }
        $kids = [];
        foreach ($plans as $plan) {
            if (!is_dir($plan['dir']) && !mkdir($plan['dir'], 0777, true) && !is_dir($plan['dir'])) {
                throw new RuntimeException('cannot create ' . $plan['dir']);
            }
            $up = Handoff::bind($plan['up']);
            $up->peer = $plan['down'];
            $cmd = [PHP_BINARY, $script, '--config', $configPath];
            if ($dev) {
                $cmd[] = '--dev-bootstrap';
            }
            $env = getenv();
            if (!is_array($env)) {
                $env = [];
            }
            $env['QUEUEFORGE_CHILD'] = '1';
            $env['QUEUEFORGE_NODE_ID'] = $plan['id'];
            $env['QUEUEFORGE_DATA_DIR'] = $plan['dir'];
            $env['QUEUEFORGE_CLUSTER'] = $plan['cluster'];
            $env['QUEUEFORGE_MEMBERS'] = (string) json_encode($members);
            $env['QUEUEFORGE_HANDOFF_UP'] = $plan['up'];
            $env['QUEUEFORGE_HANDOFF_DOWN'] = $plan['down'];
            $env['QUEUEFORGE_BIND_MGMT'] = $plan['mgmt'] ? '1' : '0';
            $pipes = [];
            $proc = proc_open($cmd, [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes, null, $env);
            if (!is_resource($proc)) {
                throw new RuntimeException('cannot start ' . $plan['id']);
            }
            fclose($pipes[0]);
            stream_set_blocking($pipes[1], false);
            stream_set_blocking($pipes[2], false);
            $kids[$plan['id']] = ['handoff' => $up, 'proc' => $proc, 'out' => $pipes[1], 'err' => $pipes[2], 'ready' => false];
        }

        $readyDeadline = microtime(true) + 15;
        while (count(array_filter($kids, static fn (array $kid): bool => $kid['ready'])) < count($kids) && microtime(true) < $readyDeadline) {
            self::pump($kids);
            usleep(20000);
        }
        $ready = count(array_filter($kids, static fn (array $kid): bool => $kid['ready']));
        if ($ready < count($kids)) {
            fwrite(STDERR, "queueforge-php parent ready $ready/" . count($kids) . "\n");
        }

        $listen = stream_socket_server('tcp://' . $cfg['amqp'], $errno, $errstr, STREAM_SERVER_BIND | STREAM_SERVER_LISTEN);
        if ($listen === false) {
            fwrite(STDERR, "listen {$cfg['amqp']}: $errstr\n");
            exit(1);
        }
        stream_set_blocking($listen, false);
        fwrite(STDOUT, 'queueforge-php parent cores=' . $cores . "\n");
        $ids = array_keys($kids);
        $next = 0;
        while (true) {
            self::pump($kids);
            $read = [$listen];
            $write = [];
            $except = [];
            if (@stream_select($read, $write, $except, 0, 1000) === false) {
                continue;
            }
            if (!in_array($listen, $read, true)) {
                continue;
            }
            $client = @stream_socket_accept($listen, 0);
            if ($client === false) {
                continue;
            }
            stream_set_blocking($client, false);
            $id = $ids[$next % count($ids)];
            $next++;
            $ok = false;
            for ($try = 0; $try < 100 && !$ok; $try++) {
                $ok = $kids[$id]['handoff']->send(['type' => 'conn'], $client);
                if (!$ok) {
                    self::pump($kids);
                    usleep(2000);
                }
            }
            fclose($client);
            if (!$ok) {
                fwrite(STDERR, "queueforge-php handoff to $id failed\n");
            }
        }
    }

    /** @param array<string, array{handoff:Handoff,proc:mixed,out:mixed,err:mixed,ready:bool}> $kids */
    private static function pump(array &$kids): void
    {
        foreach ($kids as $id => $kid) {
            $out = stream_get_contents($kid['out']);
            $err = stream_get_contents($kid['err']);
            if (is_string($out) && $out !== '') {
                fwrite(STDOUT, $out);
            }
            if (is_string($err) && $err !== '') {
                fwrite(STDERR, $err);
            }
            while ($packet = $kid['handoff']->recv()) {
                $msg = $packet['msg'];
                if (($msg['type'] ?? '') === 'ready') {
                    $kids[$id]['ready'] = true;
                    continue;
                }
                if (($msg['type'] ?? '') !== 'migrate' || !is_resource($packet['fp'])) {
                    if (is_resource($packet['fp'])) {
                        fclose($packet['fp']);
                    }
                    continue;
                }
                $home = (string) ($msg['home'] ?? '');
                if (!isset($kids[$home])) {
                    fclose($packet['fp']);
                    continue;
                }
                $sent = false;
                for ($try = 0; $try < 100 && !$sent; $try++) {
                    $sent = $kids[$home]['handoff']->send([
                        'type' => 'conn',
                        'state' => $msg['state'] ?? [],
                        'bytes' => $msg['bytes'] ?? '',
                    ], $packet['fp']);
                    if (!$sent) {
                        usleep(1000);
                    }
                }
                fclose($packet['fp']);
                if (!$sent) {
                    fwrite(STDERR, "queueforge-php migrate to $home failed\n");
                }
            }
        }
    }
}
