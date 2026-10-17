<?php
declare(strict_types=1);

/**
 * Cloud discovery HTTP calls run off the event loop: with a Consul agent
 * that takes 3 seconds per request, AMQP round trips on the broker never
 * stall for more than 200 ms.
 */

require_once __DIR__ . '/lib/Harness.php';

$mockPort = Harness::freePort();
$dir = sys_get_temp_dir() . '/qf-discovery-slow-' . bin2hex(random_bytes(4));
mkdir($dir);
$state = "$dir/state";
$mock = proc_open([PHP_BINARY, '-S', "127.0.0.1:$mockPort", __DIR__ . '/lib/slow-discovery-mock.php'], [1 => ['file', "$dir/mock.out", 'w'], 2 => ['file', "$dir/mock.err", 'w']], $pipes, null, ['MOCK_STATE' => $state] + getenv());
Harness::waitListening($mockPort, $dir);
$requests = static fn (): array => array_map(static fn ($l) => json_decode($l, true), array_filter(explode("\n", (string) @file_get_contents("$state.log"))));

Harness::guard('a slow discovery endpoint does not stall AMQP traffic', static function () use ($mockPort, $state, $requests): void {
    $cluster = Harness::freePort();
    $broker = Harness::broker();
    // Declared before clustering is configured, so no metadata quorum is needed.
    $c = new Amqp('127.0.0.1', $broker['port']); $c->channel(); $c->declareQueue('slow-disc'); $c->close();
    Harness::stop($broker);
    $config = (string) file_get_contents($broker['dir'] . '/config.toml');
    $config .= "\n[cluster]\nlisten = \"127.0.0.1:$cluster\"\ndiscovery = \"consul\"\nconsul_host = \"127.0.0.1\"\nconsul_port = $mockPort\nconsul_svc = \"qf\"\nconsul_svc_ttl = 2\ndiscovery_interval = 1\n";
    file_put_contents($broker['dir'] . '/config.toml', $config);
    $broker = Harness::launch($broker['dir'], $broker['port']);
    try {
        Harness::ok('startup registered with the agent', Harness::until(static fn (): bool => in_array('/v1/agent/service/register', array_column($requests(), 'path'), true), 5.0));
        // From now on every discovery request takes 3 seconds.
        touch("$state.slow");
        $c = new Amqp('127.0.0.1', $broker['port']); $c->channel();
        $from = microtime(true); $last = $from; $worst = 0.0; $trips = 0;
        while (microtime(true) - $from < 8.0) {
            $c->publish('', 'slow-disc', 'tick');
            $m = $c->get('slow-disc', true);
            $now = microtime(true);
            if ($m['delivered']) $trips++;
            $worst = max($worst, $now - $last); $last = $now;
            usleep(10000);
        }
        $c->close();
        $slow = array_filter($requests(), static fn (array $r): bool => $r['slow'] && $r['start'] < $last && $r['end'] > $from);
        Harness::ok('slow discovery requests ran during the traffic', count($slow) >= 1, (string) count($slow));
        Harness::ok('round trips kept flowing', $trips > 100, (string) $trips);
        Harness::ok('no AMQP stall above 200 ms', $worst < 0.2, sprintf('worst gap %.3f s', $worst));
    } finally { Harness::stop($broker); @unlink("$state.slow"); }
});

proc_terminate($mock);
proc_close($mock);
Harness::done();
