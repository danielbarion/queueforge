<?php
declare(strict_types=1);

/**
 * Cores mode with TLS: the parent hands raw TCP to a child, which runs the
 * handshake. TLS connections are not moved between children, so a publisher
 * and a consumer on different children exchange messages over the cluster
 * links, as Bun's children do.
 */

require_once __DIR__ . '/lib/Harness.php';

$pki = sys_get_temp_dir() . '/qf-tls-cores-' . bin2hex(random_bytes(4));
mkdir($pki);
$run = static function (array $cmd): void {
    $p = proc_open($cmd, [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
    $err = stream_get_contents($pipes[2]);
    foreach ($pipes as $fp) fclose($fp);
    if (proc_close($p) !== 0) throw new RuntimeException($err);
};
$run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', "$pki/ca.key", '-out', "$pki/ca.pem", '-subj', '/CN=QF Test CA', '-days', '1']);
foreach (['server' => 'localhost', 'client' => 'admin'] as $name => $cn) {
    $run(['openssl', 'req', '-newkey', 'rsa:2048', '-nodes', '-keyout', "$pki/$name.key", '-out', "$pki/$name.csr", '-subj', "/CN=$cn"]);
    file_put_contents("$pki/$name.ext", $name === 'server' ? "subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n" : "extendedKeyUsage=clientAuth\n");
    $run(['openssl', 'x509', '-req', '-in', "$pki/$name.csr", '-CA', "$pki/ca.pem", '-CAkey', "$pki/ca.key", '-CAcreateserial', '-out', "$pki/$name.pem", '-days', '1', '-extfile', "$pki/$name.ext"]);
}

$broker = Harness::broker(['', '[tls]', 'enabled = true', "cert_path = \"$pki/server.pem\"", "key_path = \"$pki/server.key\"", "ca_path = \"$pki/ca.pem\""], 10, '', [], ['QUEUEFORGE_CORES' => '2']);
$log = '';
$deadline = microtime(true) + 12.0;
while (microtime(true) < $deadline && !str_contains($log, 'parent cores=2')) {
    usleep(50000);
    $log = (string) @file_get_contents($broker['dir'] . '/out.log');
}
Harness::ok('two children behind a TLS listener', str_contains($log, 'parent cores=2'), trim($log . "\n" . Harness::stderr($broker)));

$ssl = ['cafile' => "$pki/ca.pem", 'verify_peer' => true, 'verify_peer_name' => true, 'peer_name' => 'localhost'];
$open = static fn (): Amqp => new Amqp('127.0.0.1', $broker['port'], 'admin', 'devpassword12', true, 5.0, false, $ssl);

Harness::guard('TLS connections land on both children', static function () use ($open): void {
    $conns = [];
    for ($i = 0; $i < 4; $i++) $conns[] = $open();
    Harness::eq('four TLS connections open with PLAIN and no certificate', 4, count($conns));
    foreach ($conns as $c) $c->close();
});

Harness::guard('messages cross children over TLS', static function () use ($open): void {
    foreach ([true, false] as $consumerFirst) {
        $queue = 'tls-cores-' . ($consumerFirst ? 'c' : 'p');
        $bodies = ['one', 'two', 'three'];
        $c = $open();
        $c->channel();
        $c->declareQueue($queue);
        if ($consumerFirst) { $c->qos(10); $c->consume($queue, ''); }
        $p = $open();
        $p->channel();
        $p->confirmSelect();
        foreach ($bodies as $body) {
            $p->publish('', $queue, $body, ['deliveryMode' => 2]);
            $p->expect(60, 80, 3.0);
        }
        if (!$consumerFirst) { $c->qos(10); $c->consume($queue, ''); }
        $got = [];
        $until = microtime(true) + 5.0;
        while (count($got) < count($bodies) && microtime(true) < $until) {
            $frame = $c->read(max(0.05, $until - microtime(true)));
            if ($frame !== null && $frame['class'] === 60 && $frame['method'] === 60) $got[] = (string) $frame['body'];
        }
        Harness::eq(($consumerFirst ? 'consumer first' : 'publisher first') . ': every body arrives', $bodies, $got);
        $p->close();
        $c->close();
    }
});

// Settling travels back to the home: a nack requeues there, an ack removes it.
Harness::guard('remote settlement reaches the home', static function () use ($open): void {
    $take = static function (Amqp $c): ?array {
        $until = microtime(true) + 3.0;
        while (microtime(true) < $until) {
            $frame = $c->read(max(0.05, $until - microtime(true)));
            if ($frame !== null && $frame['class'] === 60 && $frame['method'] === 60) return $frame;
        }
        return null;
    };
    $tagOf = static fn (array $frame): int => Codec::readU64($frame['args'], 1 + ord($frame['args'][0]));
    $p = $open();
    $p->channel();
    $p->confirmSelect();
    $queues = [];
    for ($i = 0; $i < 4; $i++) {
        $queue = "tls-settle-$i";
        $p->declareQueue($queue);
        $p->publish('', $queue, "settle-$i", ['deliveryMode' => 2]);
        $p->expect(60, 80, 3.0);
        $queues[] = $queue;
    }
    $redelivered = 0;
    $acked = 0;
    foreach ($queues as $queue) {
        $c = $open();
        $c->channel();
        $c->consume($queue, '');
        $first = $take($c);
        if ($first === null) continue;
        $c->nack($tagOf($first), true);
        $again = $take($c);
        if ($again !== null && $again['body'] === $first['body']) $redelivered++;
        if ($again !== null) { $c->ack($tagOf($again)); $acked++; }
        $c->close();
    }
    Harness::eq('a nack requeues at the home and is delivered again', 4, $redelivered);
    usleep(300000);
    $left = 0;
    foreach ($queues as $queue) $left += $p->declareQueue($queue, true, [], 1, true)['messages'] ?? 0;
    Harness::eq('acked messages are gone from every home', 0, $left);
    $p->close();
});

Harness::guard('EXTERNAL with a client certificate in a child', static function () use ($broker, $pki, $ssl): void {
    $fp = @stream_socket_client('tls://127.0.0.1:' . $broker['port'], $errno, $errstr, 5, STREAM_CLIENT_CONNECT,
        stream_context_create(['ssl' => $ssl + ['local_cert' => "$pki/client.pem", 'local_pk' => "$pki/client.key"]]));
    Harness::ok('handshake with a client certificate', is_resource($fp), (string) $errstr);
    if (!is_resource($fp)) return;
    stream_set_timeout($fp, 5);
    fwrite($fp, "AMQP\0\0\x09\x01");
    $head = (string) fread($fp, 7);
    $len = strlen($head) === 7 ? unpack('N', substr($head, 3, 4))[1] : 0;
    $start = '';
    while (strlen($start) < $len + 1 && !feof($fp)) $start .= (string) fread($fp, $len + 1 - strlen($start));
    Harness::ok('EXTERNAL is offered', str_contains($start, 'EXTERNAL'));
    fclose($fp);
});

Harness::stop($broker, true);

// A separate amqps port beside plain AMQP: the first child serves it, and its
// clients reach queues on the other child over the cluster links.
$amqps = Harness::freePort();
$second = Harness::broker(["amqps = \"127.0.0.1:$amqps\"", '', '[tls]', 'enabled = false', "cert_path = \"$pki/server.pem\"", "key_path = \"$pki/server.key\"", "ca_path = \"$pki/ca.pem\""], 10, '', [], ['QUEUEFORGE_CORES' => '2']);
$log = '';
$deadline = microtime(true) + 12.0;
while (microtime(true) < $deadline && !str_contains($log, 'parent cores=2')) {
    usleep(50000);
    $log = (string) @file_get_contents($second['dir'] . '/out.log');
}
Harness::ok('cores mode stays on with an amqps port', str_contains($log, 'parent cores=2'), trim($log . "\n" . Harness::stderr($second)));
Harness::guard('plain publishers and amqps consumers share queues', static function () use ($second, $amqps, $ssl): void {
    $got = [];
    $want = [];
    for ($i = 0; $i < 4; $i++) {
        $queue = "amqps-cores-$i";
        $p = new Amqp('127.0.0.1', $second['port']);
        $p->channel();
        $p->declareQueue($queue);
        $p->confirmSelect();
        $p->publish('', $queue, "via-plain-$i", ['deliveryMode' => 2]);
        $p->expect(60, 80, 3.0);
        $want[] = "via-plain-$i";
        $c = new Amqp('127.0.0.1', $amqps, 'admin', 'devpassword12', true, 5.0, false, $ssl);
        $c->channel();
        $c->consume($queue, '', true);
        $until = microtime(true) + 3.0;
        while (microtime(true) < $until) {
            $frame = $c->read(max(0.05, $until - microtime(true)));
            if ($frame !== null && $frame['class'] === 60 && $frame['method'] === 60) { $got[] = (string) $frame['body']; break; }
        }
        $c->close();
        $p->close();
    }
    Harness::eq('every queue delivers to its amqps consumer', $want, $got);
    Harness::ok('only one child binds amqps', !str_contains(Harness::stderr($second), 'listen amqps'), Harness::stderr($second));
});
Harness::stop($second, true);
exec('rm -rf ' . escapeshellarg($pki));
Harness::done();
