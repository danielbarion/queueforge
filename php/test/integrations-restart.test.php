<?php
declare(strict_types=1);

/**
 * Federation (exchange and queue links), dynamic shovels (AMQP 0-9-1, AMQP
 * 1.0 and stream sources) and delayed messages, each checked again after a
 * broker restart: upstreams, policies, shovels and pending delayed messages
 * are stored, and the links come back.
 */

require_once __DIR__ . '/lib/Harness.php';

$mgmt = Harness::freePort();
$broker = Harness::broker(["management = \"127.0.0.1:$mgmt\""]);
Harness::waitListening($mgmt, $broker['dir']);
$port = $broker['port'];
$remote = "amqp://admin:devpassword12@127.0.0.1:$port";

$client = static function () use ($port): Amqp { $c = new Amqp('127.0.0.1', $port); $c->channel(); return $c; };
/** Bodies in `$queue` within `$seconds`, taken with basic.get. */
$drain = static function (string $queue, int $want, float $seconds = 5.0) use ($client): array {
    $c = $client(); $got = [];
    Harness::until(static function () use ($c, $queue, $want, &$got): bool {
        while (($m = $c->get($queue, true))['delivered']) $got[] = $m['body'];
        return count($got) >= $want;
    }, $seconds);
    $c->close();
    return $got;
};
$links = static function (string $path) use ($mgmt): array { $r = Harness::http($mgmt, 'GET', $path); return is_array($r['json']) ? $r['json'] : []; };

Harness::guard('federation links', static function () use ($mgmt, $client, $links): void {
    $c = $client();
    $c->declareExchange('fx-up', 'topic'); $c->declareExchange('fx-down', 'topic');
    $c->declareQueue('fx-q'); $c->bind('fx-q', 'fx-down', 'orders.#');
    $c->declareQueue('fq-up'); $c->declareQueue('fq-down');
    Harness::eq('upstream stored', 201, Harness::http($mgmt, 'PUT', '/api/parameters/federation-upstream/%2f/up-x', ['value' => ['uri' => 'amqp://', 'exchange' => 'fx-up']])['status']);
    Harness::http($mgmt, 'PUT', '/api/parameters/federation-upstream/%2f/up-q', ['value' => ['uri' => 'amqp://', 'queue' => 'fq-up']]);
    Harness::http($mgmt, 'PUT', '/api/policies/%2f/fed-x', ['pattern' => '^fx-down$', 'apply-to' => 'exchanges', 'definition' => ['federation-upstream' => 'up-x'], 'priority' => 1]);
    Harness::http($mgmt, 'PUT', '/api/policies/%2f/fed-q', ['pattern' => '^fq-down$', 'apply-to' => 'queues', 'definition' => ['federation-upstream-set' => 'all'], 'priority' => 1]);
    $running = Harness::until(static function () use ($links): bool {
        $rows = $links('/api/federation-links/%2f');
        $x = array_filter($rows, static fn ($r) => ($r['type'] ?? '') === 'exchange' && ($r['exchange'] ?? '') === 'fx-down' && ($r['upstream_exchange'] ?? '') === 'fx-up' && $r['status'] === 'running');
        $q = array_filter($rows, static fn ($r) => ($r['type'] ?? '') === 'queue' && ($r['queue'] ?? '') === 'fq-down' && ($r['upstream_queue'] ?? '') === 'fq-up' && $r['upstream'] === 'up-q' && $r['status'] === 'running');
        return $x !== [] && $q !== [];
    }, 5.0);
    Harness::ok('GET /api/federation-links lists the exchange and queue links as running', $running, json_encode($links('/api/federation-links')));
    $bindings = Harness::http($mgmt, 'GET', '/api/bindings/%2f')['json']['items'] ?? [];
    $internal = array_values(array_map(static fn ($b) => $b['routing_key'], array_filter($bindings, static fn ($b) => str_starts_with((string) ($b['destination'] ?? ''), 'qf-fed-') && ($b['source'] ?? '') === 'fx-up')));
    Harness::eq('the link queue is bound with the downstream binding keys', ['orders.#'], $internal);
    $c->publish('fx-up', 'orders.new', 'federated');
    $c->publish('fx-up', 'other.key', 'not-bound');
    Harness::ok('a federated exchange receives what is published upstream', Harness::until(static fn () => ($m = $c->get('fx-q', true))['delivered'] && $m['body'] === 'federated', 5.0));
    usleep(300000);
    Harness::eq('a key the downstream does not bind stays upstream', false, $c->get('fx-q', true)['delivered']);
    $c->publish('', 'fq-up', 'sitting-upstream');
    usleep(600000);
    Harness::eq('a queue link does not pull without a consumer', 1, $c->declareQueue('fq-up', true, [], 1, true)['messages']);
    $c->channel(2);
    $c->consume('fq-down', 'fq-consumer', true, 2);
    $got = [];
    Harness::until(static function () use ($c, &$got): bool { $f = $c->read(0.2); if ($f !== null && $f['class'] === 60 && $f['method'] === 60) $got[] = $f['body']; return $got !== []; }, 8.0);
    Harness::eq('a federated queue pulls from its upstream for its consumer', ['sitting-upstream'], $got);
    $c->close();
});

Harness::guard('shovels', static function () use ($mgmt, $client, $drain, $links, $remote): void {
    $c = $client();
    foreach (['sh-src', 'sh-dst', 's10-src', 's10-dst', 's10-local', 'ss-dst', 'ssr-dst'] as $q) $c->declareQueue($q);
    $c->declareQueue('sh-stream', true, ['x-queue-type' => 'stream']);
    $c->confirmSelect();
    foreach (['one', 'two', 'three'] as $b) $c->publish('', 'sh-stream', "stream-$b");
    $put = static fn (string $name, array $value) => Harness::http($mgmt, 'PUT', "/api/parameters/shovel/%2f/$name", ['value' => $value])['status'];
    Harness::eq('an amqp091 shovel is stored', 201, $put('sh-091', ['src-protocol' => 'amqp091', 'src-uri' => 'amqp://', 'src-queue' => 'sh-src', 'dest-protocol' => 'amqp091', 'dest-uri' => 'amqp://', 'dest-queue' => 'sh-dst']));
    $put('sh-10', ['src-protocol' => 'amqp10', 'src-uri' => $remote, 'src-address' => '/queues/s10-src', 'dest-protocol' => 'amqp10', 'dest-uri' => $remote, 'dest-address' => '/queues/s10-dst']);
    $put('sh-10-local', ['src-protocol' => 'amqp10', 'src-uri' => 'amqp://', 'src-address' => 's10-local', 'dest-protocol' => 'amqp091', 'dest-uri' => 'amqp://', 'dest-queue' => 's10-dst']);
    $put('sh-stream', ['src-protocol' => 'amqp091', 'src-uri' => 'amqp://', 'src-queue' => 'sh-stream', 'src-consumer-args' => ['x-stream-offset' => 'first'], 'dest-uri' => 'amqp://', 'dest-queue' => 'ss-dst']);
    $put('sh-stream-remote', ['src-protocol' => 'amqp091', 'src-uri' => $remote, 'src-queue' => 'sh-stream', 'src-consumer-args' => ['x-stream-offset' => 'first'], 'dest-uri' => 'amqp://', 'dest-queue' => 'ssr-dst']);
    $c->publish('', 'sh-src', 'via-091');
    $c->publish('', 's10-src', 'via-amqp10');
    $c->publish('', 's10-local', 'via-local-amqp10');
    Harness::eq('an amqp091 shovel moves messages', ['via-091'], $drain('sh-dst', 1));
    Harness::eq('amqp10 source and destination over AMQP 1.0, and a local amqp10 address', ['via-amqp10', 'via-local-amqp10'], (static function (array $got): array { sort($got); return $got; })($drain('s10-dst', 2, 8.0)));
    Harness::eq('a stream source reads from x-stream-offset', ['stream-one', 'stream-two', 'stream-three'], $drain('ss-dst', 3));
    Harness::eq('a remote stream source consumes with x-stream-offset', ['stream-one', 'stream-two', 'stream-three'], $drain('ssr-dst', 3, 8.0));
    $rows = $links('/api/shovels/%2f');
    $running = array_column(array_filter($rows, static fn ($r) => $r['type'] === 'dynamic' && $r['state'] === 'running'), 'name');
    sort($running);
    Harness::eq('GET /api/shovels reports each dynamic shovel running', ['sh-091', 'sh-10', 'sh-10-local', 'sh-stream', 'sh-stream-remote'], $running);
    Harness::ok('shovel URIs are listed without the password', !str_contains(json_encode($rows), 'devpassword12'));
    $c->close();
});

Harness::guard('delayed messages before a restart', static function () use ($client): void {
    $c = $client();
    $c->declareExchange('dl-x', 'x-delayed-message', 1, ['x-delayed-type' => 'direct']);
    $c->declareQueue('dl-q'); $c->bind('dl-q', 'dl-x', 'k');
    $c->publish('dl-x', 'k', 'transient-delayed', ['deliveryMode' => 1, 'headers' => ['x-delay' => 2500]]);
    $c->publish('dl-x', 'k', 'persistent-delayed', ['deliveryMode' => 2, 'headers' => ['x-delay' => 2500]]);
    usleep(300000);
    Harness::eq('not delivered before the delay', false, $c->get('dl-q', true)['delivered']);
    $c->close();
});

usleep(300000);
$broker = Harness::restart($broker);
Harness::waitListening($mgmt, $broker['dir']);

Harness::guard('after a restart', static function () use ($mgmt, $client, $drain, $links): void {
    $c = $client();
    $early = $c->get('dl-q', true)['delivered'];
    Harness::ok('a delayed message is still held right after the restart', !$early);
    $delayed = $drain('dl-q', 2, 6.0);
    sort($delayed);
    Harness::eq('pending delayed messages, transient and persistent, survive and are delivered', ['persistent-delayed', 'transient-delayed'], $delayed);
    $params = array_column(Harness::http($mgmt, 'GET', '/api/parameters')['json'] ?? [], 'name');
    Harness::ok('upstreams and shovels are stored', !array_diff(['up-x', 'up-q', 'sh-091', 'sh-10', 'sh-stream'], $params));
    $up = Harness::until(static function () use ($links): bool {
        $rows = $links('/api/federation-links');
        return count(array_filter($rows, static fn ($r) => $r['status'] === 'running')) >= 2;
    }, 6.0);
    Harness::ok('federation links come back', $up);
    $c->publish('fx-up', 'orders.again', 'federated-after-restart');
    Harness::eq('exchange federation works after the restart', ['federated-after-restart'], $drain('fx-q', 1));
    $c->publish('', 'sh-src', 'shoveled-after-restart');
    Harness::eq('a stored shovel restarts', ['shoveled-after-restart'], $drain('sh-dst', 1));
    $c->channel(2);
    $c->consume('fq-down', 'fq-again', true, 2);
    $c->publish('', 'fq-up', 'pulled-after-restart');
    $got = [];
    Harness::until(static function () use ($c, &$got): bool { $f = $c->read(0.2); if ($f !== null && $f['class'] === 60 && $f['method'] === 60) $got[] = $f['body']; return $got !== []; }, 8.0);
    Harness::eq('queue federation works after the restart', ['pulled-after-restart'], $got);
    Harness::eq('a shovel is deleted', 204, Harness::http($mgmt, 'DELETE', '/api/parameters/shovel/%2f/sh-091')['status']);
    Harness::ok('and stops', Harness::until(static fn () => !in_array('sh-091', array_column($links('/api/shovels'), 'name'), true), 3.0));
    $c->close();
});

Harness::stop($broker);
Harness::done();
