<?php
declare(strict_types=1);
require_once __DIR__ . '/lib/Harness.php';
foreach (['Routing', 'Features', 'Policy', 'Codec', 'Auth', 'Store', 'Cluster', 'Broker', 'Streams', 'Amqp10'] as $file) require_once __DIR__ . "/../src/$file.php";

function a10Fixture(): array {
    $dir = sys_get_temp_dir() . '/qf-a10-' . bin2hex(random_bytes(6));
    $store = new Store("$dir/messages.log"); $broker = new Broker($store, "$dir/users.json"); $broker->bootstrap('devpassword12'); $broker->declareQueue('q');
    return [$broker, new Amqp10($broker), ['conn' => 91], $store, $dir];
}
function a10Finish(array $fixture): void {
    foreach ($fixture[0]->allBrokers() as $scope) if (is_resource($scope->store->fp)) fclose($scope->store->fp);
    $paths = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($fixture[4], FilesystemIterator::SKIP_DOTS), RecursiveIteratorIterator::CHILD_FIRST);
    foreach ($paths as $path) $path->isDir() ? rmdir($path->getPathname()) : unlink($path->getPathname());
    rmdir($fixture[4]);
}
function a10Frame(int $code, array $fields = [], string $payload = '', int $channel = 0, int $type = 0): string {
    $body = Amqp10Codec::encode(new Amqp10Described($code, $fields)) . $payload; return pack('NCCn', strlen($body) + 8, 2, $type, $channel) . $body;
}
function a10Drive(Amqp10 $a, array &$state, string $bytes): string { return $a->drive($bytes, $state); }
function a10Open(Amqp10 $a, array &$state, string $password = 'devpassword12', string $user = 'admin', string $vhost = '/'): string {
    a10Drive($a, $state, "AMQP\x03\x01\x00\x00");
    $out = a10Drive($a, $state, a10Frame(0x41, [Amqp10Codec::symbol('PLAIN'), Amqp10Codec::binary("\0$user\0$password")], '', 0, 1));
    if (!($state['closing'] ?? false)) $out .= a10Drive($a, $state, "AMQP\x00\x01\x00\x00" . a10Frame(0x10, ['unit-client', 'vhost:' . $vhost, Amqp10Codec::uint(512)]) . a10Frame(0x11, [null, Amqp10Codec::uint(0), Amqp10Codec::uint(10000), Amqp10Codec::uint(10000)]));
    return $out;
}
function a10Frames(string $bytes): array {
    $out = []; $at = 0;
    while ($at < strlen($bytes)) {
        if (substr($bytes, $at, 4) === 'AMQP') { $at += 8; continue; }
        $size = unpack('N', substr($bytes, $at, 4))[1]; $offset = ord($bytes[$at + 4]) * 4;
        $body = substr($bytes, $at + $offset, $size - $offset); $at += $size;
        if ($body === '') continue;
        $d = new Amqp10Codec($body); $p = $d->value(); $out[] = ['code' => $p->descriptor, 'fields' => $p->value, 'payload' => substr($body, $d->at), 'size' => $size];
    }
    return $out;
}
function a10Attach(Amqp10 $a, array &$state, int $handle, bool $receiver, ?string $address = '/queues/q'): string {
    $source = $receiver ? new Amqp10Described(0x28, [$address]) : null; $target = $receiver ? null : new Amqp10Described(0x29, [$address]);
    return a10Drive($a, $state, a10Frame(0x12, ['link-' . $handle, Amqp10Codec::uint($handle), $receiver, new Amqp10Value('ubyte', 0), new Amqp10Value('ubyte', 0), $source, $target, null, null, Amqp10Codec::uint(0)]));
}
function a10Publish(Amqp10 $a, array &$state, int $id, string $body): string {
    $payload = Amqp10Codec::encode(new Amqp10Described(0x75, Amqp10Codec::binary($body)));
    return a10Drive($a, $state, a10Frame(0x14, [Amqp10Codec::uint(0), Amqp10Codec::uint($id), Amqp10Codec::binary("tag"), Amqp10Codec::uint(0), false, false], $payload));
}

Harness::guard('AMQP10 codec preserves typed values and validates bounds', static function (): void {
    $value = new Amqp10Described(0x74, new Amqp10Value('map', [['nested', [true, 9, 'text', Amqp10Codec::binary("\0\xff")]]]));
    $encoded = Amqp10Codec::encode($value); $d = new Amqp10Codec($encoded); $decoded = $d->value();
    Harness::eq('typed map roundtrip', $encoded, Amqp10Codec::encode($decoded));
    Harness::eq('decoder consumed exactly the value', strlen($encoded), $d->at);
    foreach (["\xd0" . pack('NN', 1000, 1), "\xc1\x02\x01\x40", "\xff", "\xa1\x05abc"] as $invalid) {
        try { (new Amqp10Codec($invalid))->value(); Harness::ok('malformed value refused', false); }
        catch (RuntimeException) { Harness::ok('malformed value refused', true); }
    }
    $big = str_repeat('a', 1000000); $binary = Amqp10Codec::binary($big);
    Harness::eq('bin32 preserves million bytes', $big, (new Amqp10Codec(Amqp10Codec::encode($binary)))->value()->value);
});

Harness::guard('AMQP10 authentication and permissions', static function (): void {
    $f = a10Fixture(); [$broker, $a, $state] = $f;
    try {
        $frames = a10Frames(a10Open($a, $state, 'wrong'));
        Harness::eq('wrong PLAIN password rejected', 1, $frames[0]['fields'][0]); Harness::ok('auth failure closes', $state['closing']);
        $state = ['conn' => 92]; a10Open($a, $state); Harness::ok('valid PLAIN opens', !($state['closing'] ?? false));
        $frames = a10Frames(a10Attach($a, $state, 0, true, '/queues/missing'));
        Harness::eq('missing queue attach ends in detach', 0x16, $frames[1]['code']); Harness::eq('missing queue condition', 'amqp:not-found', $frames[1]['fields'][2]->value[0]->value);
        Harness::ok('external attach does not auto-declare', !isset($broker->queues['missing']));
        $broker->putUser('limited', 'password12', ['management']); $broker->setPermissions('limited', '/', '.*', '^allowed$', '^allowed$');
        $limited = ['conn' => 93]; a10Open($a, $limited, 'password12', 'limited');
        $frames = a10Frames(a10Attach($a, $limited, 0, true)); Harness::eq('read restricted attach refused', 'amqp:unauthorized-access', $frames[1]['fields'][2]->value[0]->value);
        $frames = a10Frames(a10Attach($a, $limited, 1, false)); Harness::eq('write restricted attach refused', 'amqp:unauthorized-access', $frames[1]['fields'][2]->value[0]->value);
    } finally { $a->closed($state); a10Finish($f); }
});

Harness::guard('AMQP10 confirmed publish maps properties across protocols', static function (): void {
    $f = a10Fixture(); [$broker, $a, $state] = $f;
    try {
        a10Open($a, $state); a10Attach($a, $state, 0, false, '/exchanges/amq.direct/key'); $broker->bind('q', 'amq.direct', 'key');
        $payload = Amqp10Codec::encode(new Amqp10Described(0x70, [true]))
            . Amqp10Codec::encode(new Amqp10Described(0x73, ['message-id', null, null, null, null, 'correlation', Amqp10Codec::symbol('text/plain')]))
            . Amqp10Codec::encode(new Amqp10Described(0x74, new Amqp10Value('map', [['color', 'blue'], ['n', 3]])))
            . Amqp10Codec::encode(new Amqp10Described(0x75, Amqp10Codec::binary('routed')));
        $out = a10Drive($a, $state, a10Frame(0x14, [Amqp10Codec::uint(0), Amqp10Codec::uint(7), Amqp10Codec::binary('tag'), Amqp10Codec::uint(0), false, false], $payload));
        Harness::eq('publish waits for real durability confirmation', '', $out); Harness::eq('one confirmation pending', 1, count($state['pending']));
        $confirms = $broker->flush(); $out = $a->confirmed($state, $confirms[0]['tag'], $confirms[0]['nack']); $frame = a10Frames($out)[0];
        Harness::eq('confirmed delivery accepted', 0x24, $frame['fields'][4]->descriptor);
        $msg = $broker->msgs[$broker->queues['q']['ready'][0]]; $props = Amqp10::readProps($msg['propRaw']);
        Harness::eq('routing body shared with 091', 'routed', $msg['body']); Harness::eq('message id mapped', 'message-id', $props['messageId']); Harness::eq('content type mapped', 'text/plain', $props['contentType']); Harness::eq('application header mapped', 'blue', $props['headers']['color']); Harness::eq('numeric header mapped', 3, $props['headers']['n']);
    } finally { $a->closed($state); a10Finish($f); }
});

Harness::guard('AMQP10 credit, dispositions and close cleanup', static function (): void {
    $f = a10Fixture(); [$broker, $a, $state] = $f;
    try {
        a10Open($a, $state); a10Attach($a, $state, 0, false);
        foreach (['one', 'two', 'three'] as $i => $body) a10Publish($a, $state, $i, $body);
        foreach ($broker->flush() as $confirm) $a->confirmed($state, $confirm['tag'], $confirm['nack']);
        a10Attach($a, $state, 1, true);
        $flow = [Amqp10Codec::uint(0), Amqp10Codec::uint(10000), Amqp10Codec::uint(0), Amqp10Codec::uint(10000), Amqp10Codec::uint(1), Amqp10Codec::uint(0), Amqp10Codec::uint(2)];
        $frames = a10Frames(a10Drive($a, $state, a10Frame(0x13, $flow))); Harness::eq('credit limits delivery count', 2, count($frames)); Harness::eq('one message remains ready', 1, $broker->readyCount('q')); Harness::eq('two deliveries unsettled', 2, count($state['sessions'][0]['unsettled']));
        a10Drive($a, $state, a10Frame(0x15, [true, Amqp10Codec::uint(0), null, true, new Amqp10Described(0x24, [])]));
        Harness::eq('accepted removes broker message', 2, count($broker->msgs));
        a10Drive($a, $state, a10Frame(0x15, [true, Amqp10Codec::uint(1), null, true, new Amqp10Described(0x26, [])])); Harness::eq('released returns message', 2, $broker->readyCount('q'));
        $flow[5] = Amqp10Codec::uint(2); $flow[6] = Amqp10Codec::uint(1); a10Drive($a, $state, a10Frame(0x13, $flow));
        Harness::eq('new credit consumes only one', 1, $broker->readyCount('q')); $a->closed($state); Harness::eq('closed returns unsettled', 2, $broker->readyCount('q'));
    } finally { $a->closed($state); a10Finish($f); }
});

Harness::guard('AMQP10 segmented messages and non-data body preservation', static function (): void {
    $f = a10Fixture(); [$broker, $a, $state] = $f;
    try {
        a10Open($a, $state); a10Attach($a, $state, 0, false);
        $big = str_repeat("\x07", 1000000); $payload = Amqp10Codec::encode(new Amqp10Described(0x75, Amqp10Codec::binary($big)));
        foreach (str_split($payload, 64000) as $i => $chunk) a10Drive($a, $state, a10Frame(0x14, [Amqp10Codec::uint(0), $i === 0 ? Amqp10Codec::uint(0) : null, $i === 0 ? Amqp10Codec::binary('tag') : null, $i === 0 ? Amqp10Codec::uint(0) : null, false, ($i + 1) * 64000 < strlen($payload)], $chunk));
        $broker->flush(); Harness::eq('segmented inbound binary preserved', $big, $broker->msgs[$broker->queues['q']['ready'][0]]['body']);
        a10Attach($a, $state, 1, true); $frames = a10Frames(a10Drive($a, $state, a10Frame(0x13, [Amqp10Codec::uint(0), Amqp10Codec::uint(10000), Amqp10Codec::uint(0), Amqp10Codec::uint(10000), Amqp10Codec::uint(1), Amqp10Codec::uint(0), Amqp10Codec::uint(1)])));
        Harness::ok('outbound segmented to negotiated frame size', count($frames) > 1 && max(array_column($frames, 'size')) <= 512);
        $joined = implode('', array_column($frames, 'payload')); Harness::eq('segmented outbound body preserved', $big, Amqp10::inbound($joined)['body']);
        foreach ([new Amqp10Described(0x77, new Amqp10Value('map', [['greeting', 'hi'], ['n', 2]])), new Amqp10Described(0x76, ['a', 2])] as $section) {
            $raw = Amqp10Codec::encode($section); $mapped = Amqp10::inbound($raw); $roundtrip = Amqp10::outbound(['mode' => 1, 'body' => $mapped['body'], 'propRaw' => Amqp10::writeProps($mapped['props'], $mapped['headers'])]);
            Harness::eq('value/sequence body kept as exact encoded sections', $raw, Amqp10::inbound($roundtrip)['body']);
        }
    } finally { $a->closed($state); a10Finish($f); }
});

Harness::guard('AMQP10 drain and malformed frames', static function (): void {
    $f = a10Fixture(); [$broker, $a, $state] = $f;
    try {
        a10Open($a, $state); a10Attach($a, $state, 0, true); $frames = a10Frames(a10Drive($a, $state, a10Frame(0x13, [Amqp10Codec::uint(0), Amqp10Codec::uint(10000), Amqp10Codec::uint(0), Amqp10Codec::uint(10000), Amqp10Codec::uint(0), Amqp10Codec::uint(0), Amqp10Codec::uint(10), null, true])));
        Harness::eq('drain reports zero remaining credit', 0, $frames[0]['fields'][6]); Harness::eq('drain advances delivery count', 10, $frames[0]['fields'][5]);
        a10Drive($a, $state, pack('NCCn', 9, 1, 0, 0) . "\x45"); Harness::ok('invalid data offset closes connection', $state['closing']);
    } finally { $a->closed($state); a10Finish($f); }
});
Harness::guard('AMQP10 vhost topology is isolated', static function (): void {
    $f = a10Fixture(); [$broker, $a, $state] = $f;
    try {
        $broker->vhosts[] = '/isolated'; $broker->setPermissions('admin', '/isolated', '.*', '.*', '.*');
        $scope = $broker->forVhost('/isolated'); $scope->declareQueue('q');
        a10Open($a, $state, 'devpassword12', 'admin', '/isolated'); a10Attach($a, $state, 0, false); a10Publish($a, $state, 1, 'isolated-body'); $scope->flush();
        Harness::eq('root queue remains empty', 0, $broker->readyCount('q'));
        Harness::eq('selected vhost queue receives publication', 1, $scope->readyCount('q'));
        a10Attach($a, $state, 1, true); a10Drive($a, $state, a10Frame(0x13, [Amqp10Codec::uint(0), Amqp10Codec::uint(100), Amqp10Codec::uint(0), Amqp10Codec::uint(100), Amqp10Codec::uint(1), Amqp10Codec::uint(0), Amqp10Codec::uint(1)]));
        $a->closed($state); Harness::eq('close requeues into original vhost', 1, $scope->readyCount('q')); Harness::eq('close does not leak into root', 0, $broker->readyCount('q'));
    } finally { $a->closed($state); a10Finish($f); }
});
Harness::guard('AMQP10 stream delivery retains immutable records', static function (): void {
    $f = a10Fixture(); [$broker, $a, $state] = $f;
    try {
        $broker->declareQueue('stream-q', ['x-queue-type' => 'stream']);
        $broker->streamAppend('stream-q', 'one', [['color', 'blue']], Amqp10::writeProps(['contentType' => 'text/plain'], [['color', 'blue']]));
        $broker->streamAppend('stream-q', 'two');
        a10Open($a, $state); a10Attach($a, $state, 0, true, '/queues/stream-q');
        $frames = a10Frames(a10Drive($a, $state, a10Frame(0x13, [Amqp10Codec::uint(0), Amqp10Codec::uint(10000), Amqp10Codec::uint(0), Amqp10Codec::uint(10000), Amqp10Codec::uint(0), Amqp10Codec::uint(0), Amqp10Codec::uint(2)])));
        Harness::eq('stream receiver sees both log records', ['one', 'two'], array_map(static fn (array $frame): string => Amqp10::inbound($frame['payload'])['body'], $frames));
        a10Drive($a, $state, a10Frame(0x15, [true, Amqp10Codec::uint(0), Amqp10Codec::uint(1), true, new Amqp10Described(0x24, [])]));
        Harness::eq('stream acknowledgments retain both records', 2, $broker->streamNext('stream-q'));
        Harness::eq('AMQP10 consumer appears in broker metadata', 1, $broker->consumerCount('stream-q'));
        $a->closed($state); Harness::eq('closed removes AMQP10 consumer', 0, $broker->consumerCount('stream-q'));
    } finally { $a->closed($state); a10Finish($f); }
});
Harness::guard('AMQP10 rejection dead-letters through the default exchange', static function (): void {
    $f = a10Fixture(); [$broker, $a, $state] = $f;
    try {
        $broker->deleteQueue('q'); $broker->declareQueue('dead'); $broker->declareQueue('q', ['x-dead-letter-exchange' => '', 'x-dead-letter-routing-key' => 'dead']);
        a10Open($a, $state); a10Attach($a, $state, 0, false); a10Publish($a, $state, 0, 'rejected'); $broker->flush();
        a10Attach($a, $state, 1, true); a10Drive($a, $state, a10Frame(0x13, [Amqp10Codec::uint(0), Amqp10Codec::uint(100), Amqp10Codec::uint(0), Amqp10Codec::uint(100), Amqp10Codec::uint(1), Amqp10Codec::uint(0), Amqp10Codec::uint(1)]));
        a10Drive($a, $state, a10Frame(0x15, [true, Amqp10Codec::uint(0), null, true, new Amqp10Described(0x25, [])])); $broker->flush();
        Harness::eq('rejected message routed to default DLX queue', 1, $broker->readyCount('dead'));
        Harness::eq('rejected body retained', 'rejected', $broker->msgs[$broker->queues['dead']['ready'][0]]['body']);
        Harness::eq('source message removed', 0, $broker->readyCount('q'));
    } finally { $a->closed($state); a10Finish($f); }
});
Harness::guard('AMQP10 typed application headers survive stored properties', static function (): void {
    $headers = [['nil', null], ['flag', true], ['fraction', 1.25], ['bytes', Amqp10Codec::binary("\xff\0")], ['nested', new Amqp10Value('map', [['nil', null], ['flag', false]])]];
    $raw = Amqp10Codec::encode(new Amqp10Described(0x73, [new Amqp10Value('ulong-raw', str_repeat("\xff", 8))]))
        . Amqp10Codec::encode(new Amqp10Described(0x74, new Amqp10Value('map', $headers)))
        . Amqp10Codec::encode(new Amqp10Described(0x75, Amqp10Codec::binary('body')));
    $mapped = Amqp10::inbound($raw);
    Harness::eq('unsigned 64-bit identifier keeps exact decimal value', '18446744073709551615', $mapped['props']['messageId']);
    $roundtrip = Amqp10::inbound(Amqp10::outbound(['body' => $mapped['body'], 'propRaw' => Amqp10::writeProps($mapped['props'], $mapped['typedHeaders'])]));
    Harness::eq('stored application headers retain AMQP value constructors', Amqp10Codec::encode(new Amqp10Value('map', $headers)), Amqp10Codec::encode(new Amqp10Value('map', $roundtrip['typedHeaders'])));
    Harness::eq('stored unsigned identifier remains exact', '18446744073709551615', $roundtrip['props']['messageId']);
});
Harness::done();
