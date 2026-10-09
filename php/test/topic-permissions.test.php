<?php
declare(strict_types=1);

require_once __DIR__ . '/lib/Harness.php';
$root = dirname(__DIR__);
foreach (['Routing', 'Features', 'Policy', 'Codec', 'Auth', 'Store', 'Cluster', 'Broker', 'Amqp10', 'Server'] as $class) {
    require_once "$root/src/$class.php";
}

$dir = sys_get_temp_dir() . '/qf-topic-' . bin2hex(random_bytes(6));
$store = new Store("$dir/messages.log");
$broker = new Broker($store, "$dir/users.json");
$broker->putUser("alice", "test-password", []);
$broker->setPermissions("alice", "/", ".*", ".*", ".*");

Harness::guard('topic permission patterns', static function () use ($broker): void {
    Harness::eq('no rule allows publishing', true, $broker->topicWriteAllowed('alice', '/', 'logs', 'private.entry'));
    $broker->topicPermissions['alice']['logs'] = ['write' => '^public\\.', 'read' => '^public\\.'];
    Harness::eq('matching routing key allowed', true, $broker->topicWriteAllowed('alice', '/', 'logs', 'public.entry'));
    Harness::eq('nonmatching routing key refused', false, $broker->topicWriteAllowed('alice', '/', 'logs', 'private.entry'));
    Harness::eq('rule does not affect another user', true, $broker->topicWriteAllowed('bob', '/', 'logs', 'private.entry'));
    Harness::eq('rule does not affect another exchange', true, $broker->topicWriteAllowed('alice', '/', 'other', 'private.entry'));
    $broker->topicPermissions['alice']['empty'] = ['write' => '', 'read' => ''];
    Harness::eq('empty pattern refuses publication', false, $broker->topicWriteAllowed('alice', '/', 'empty', 'anything'));
    $broker->topicPermissions['alice']['invalid'] = ['write' => '[', 'read' => ''];
    Harness::eq('invalid pattern refuses publication', false, $broker->topicWriteAllowed('alice', '/', 'invalid', 'anything'));
});

// Exercise the actual AMQP publication handler without opening a listener.
Harness::guard('topic permission enforcement', static function () use ($broker): void {
    $broker->declareQueue('topic-q');
    $broker->declareExchange('logs', 'topic');
    $broker->bind('topic-q', 'logs', '#');
    $server = new Server(null, $broker, 10);
    $finish = new ReflectionMethod(Server::class, 'finishPublish');
    foreach ([['public.entry', true], ['private.entry', false]] as [$key, $allowed]) {
        $sock = new Sock(null);
        $sock->user = 'alice';
        $ch = new Chan();
        $ch->pub = ['exchange' => 'logs', 'key' => $key, 'got' => $key, 'mode' => 1, 'mandatory' => false];
        $sock->channels[1] = $ch;
        $server->conns[1] = $sock;
        $finish->invoke($server, 1, 1);
        if ($allowed) {
            Harness::eq('allowed publish queues its message', 1, count($broker->queues['topic-q']['ready']));
            Harness::eq('allowed publish leaves channel open', '', $sock->out);
        } else {
            Harness::eq('refused publish adds no message', 1, count($broker->queues['topic-q']['ready']));
            Harness::eq('refused publish closes channel with access refused', Codec::channelClose(1, 403, 'ACCESS_REFUSED - write access to topic refused', 60, 40), $sock->out);
        }
    }
});

// Exercise binding through the authenticated AMQP connection, using different
// read/write patterns so the test cannot pass with the publication check.
Harness::guard('authenticated topic binding', static function () use ($broker): void {
    $broker->declareQueue('bind-q');
    $broker->declareExchange('bind-logs', 'topic');
    $broker->declareExchange('bind-direct', 'direct');
    $broker->topicPermissions['alice']['bind-logs'] = ['write' => '^public\\.', 'read' => '^allowed\\.'];
    $broker->topicPermissions['alice']['bind-direct'] = ['write' => '', 'read' => ''];
    $server = new Server(null, $broker, 10);
    $bind = new ReflectionMethod(Server::class, 'queueBind');
    foreach ([['bind-logs', 'allowed.entry', true], ['bind-logs', 'public.entry', false], ['bind-direct', 'anything', true]] as [$exchange, $key, $allowed]) {
        $sock = new Sock(null);
        $sock->user = 'alice';
        $sock->channels[1] = new Chan();
        $server->conns[1] = $sock;
        $before = count($broker->bindings);
        $payload = pack('n', 0) . Codec::shortstr('bind-q') . Codec::shortstr($exchange) . Codec::shortstr($key) . chr(0) . Codec::writeTable([]);
        $bind->invoke($server, 1, 1, $payload, 0);
        Harness::eq("$exchange $key binding count", $before + ($allowed ? 1 : 0), count($broker->bindings));
        $expected = $allowed ? Codec::method(1, 50, 21) : Codec::channelClose(1, 403, "ACCESS_REFUSED - cannot bind queue without topic read permission for '$exchange' key '$key'", 50, 20);
        Harness::eq("$exchange $key response", $expected, $sock->out);
    }
});

fclose($store->fp);
foreach (glob("$dir/*") ?: [] as $file) {
    unlink($file);
}
rmdir($dir);
Harness::done();
