<?php
declare(strict_types=1);

/**
 * Stream protocol filtering, as RabbitMQ 3.13+: PublishV2 carries a filter
 * value per message (stored as x-stream-filter-value, shared with AMQP 0-9-1),
 * Subscribe takes filter.N and match-unfiltered, and the command versions
 * exchange advertises PublishV2.
 */
require_once __DIR__ . '/lib/Harness.php';
foreach (['Routing','Features','Policy','Codec','Auth','Store','Cluster','Broker','Amqp10','Protocols'] as $class) require_once dirname(__DIR__) . '/src/' . $class . '.php';

function sfStr(?string $s): string { return $s === null ? pack('n', 0xffff) : pack('n', strlen($s)) . $s; }
function sfFrames(string $bytes): array { $out = []; while (strlen($bytes) >= 4) { $n = unpack('N', substr($bytes, 0, 4))[1]; $out[] = ['key' => unpack('n', substr($bytes, 4, 2))[1], 'version' => unpack('n', substr($bytes, 6, 2))[1], 'body' => substr($bytes, 8, $n - 4)]; $bytes = substr($bytes, $n + 4); } return $out; }
function sfSend(Protocols $p, array &$state, int $key, string $body, int $version = 1): array { $p = [$p]; $frame = pack('nn', $key, $version) . $body; $buf = pack('N', strlen($frame)) . $frame; return sfFrames($p[0]->stream($buf, $state)); }
function sfCode(array $frames): int { return unpack('n', substr($frames[0]['body'], 4, 2))[1]; }
function sfData(string $body): string { return "\0\x53\x75\xa0" . chr(strlen($body)) . $body; }
/** Bodies delivered in Deliver frames, in order, from the known set. */
function sfDelivered(array $frames, array $known): array
{
    $got = [];
    foreach ($frames as $f) if ($f['key'] === 8) foreach ($known as $body) if (str_contains($f['body'], sfData($body))) $got[] = $body;
    return $got;
}
function sfMap(array $m): string { $b = pack('N', count($m)); foreach ($m as $k => $v) $b .= sfStr((string) $k) . sfStr((string) $v); return $b; }

Harness::guard('stream protocol filtering', static function (): void {
    $dir = sys_get_temp_dir() . '/qf-stream-filter-' . bin2hex(random_bytes(6));
    $b = new Broker(new Store($dir . '/messages.log'), $dir . '/users.json'); $b->bootstrap('devpassword12');
    $p = new Protocols($b); $s = [];
    $auth = "\0admin\0devpassword12";
    sfSend($p, $s, 19, pack('N', 1) . sfStr('PLAIN') . pack('N', strlen($auth)) . $auth);
    Harness::eq('open', 1, sfCode(sfSend($p, $s, 21, pack('N', 2) . sfStr('/'))));

    $versions = sfSend($p, $s, 27, pack('N', 3) . pack('N', 1) . pack('nnn', 2, 1, 2));
    $body = $versions[0]['body']; $n = unpack('N', substr($body, 6, 4))[1]; $publishMax = null;
    for ($i = 0; $i < $n; $i++) { [, $k, $min, $max] = unpack('n3', substr($body, 10 + 6 * $i, 6)); if ($k === 2) $publishMax = $max; }
    Harness::eq('command versions advertise PublishV2', 2, $publishMax);

    Harness::eq('create stream', 1, sfCode(sfSend($p, $s, 13, pack('N', 4) . sfStr('filt') . pack('N', 0))));
    Harness::eq('declare publisher', 1, sfCode(sfSend($p, $s, 1, pack('NC', 5, 1) . sfStr('') . sfStr('filt'))));
    $messages = [['red-one', 'red'], ['blue-one', 'blue'], ['plain-one', null], ['red-two', 'red']];
    $publish = chr(1) . pack('N', count($messages));
    foreach ($messages as $i => [$text, $filter]) $publish .= pack('J', $i + 1) . sfStr($filter) . pack('N', strlen(sfData($text))) . sfData($text);
    $confirm = sfSend($p, $s, 2, $publish, 2);
    $b->flushDurable(); $confirm = [...$confirm, ...sfSend($p, $s, 23, '')];
    Harness::ok('PublishV2 is confirmed', in_array(3, array_column($confirm, 'key'), true), json_encode(array_column($confirm, 'key')));
    Harness::eq('four records stored', 4, $b->streamNext('filt'));
    Harness::eq('filter value stored as x-stream-filter-value', 'red', array_column($b->streamRead('filt', 0, 1)[0]['headers'], 1, 0)['x-stream-filter-value'] ?? null);
    Harness::eq('no filter value, no header', null, array_column($b->streamRead('filt', 2, 1)[0]['headers'], 1, 0)['x-stream-filter-value'] ?? null);

    $known = ['red-one', 'blue-one', 'plain-one', 'red-two', 'amqp-red', 'amqp-green'];
    $sub = static fn (int $id, array $props) => sfSend($p, $s, 7, pack('NC', 10 + $id, $id) . sfStr('filt') . pack('n', 1) . pack('n', 100) . sfMap($props));
    $red = $sub(1, ['filter.0' => 'red']);
    Harness::eq('filter.0 delivers only matching values', ['red-one', 'red-two'], sfDelivered($red, $known));
    $blue = $sub(2, ['filter.0' => 'blue', 'match-unfiltered' => 'true']);
    Harness::eq('match-unfiltered adds messages without a value', ['blue-one', 'plain-one'], sfDelivered($blue, $known));
    $many = $sub(3, ['filter.0' => 'blue', 'filter.1' => 'red']);
    Harness::eq('several filters are a set', ['red-one', 'blue-one', 'red-two'], sfDelivered($many, $known));
    $all = $sub(4, []);
    Harness::eq('no filter delivers everything', ['red-one', 'blue-one', 'plain-one', 'red-two'], sfDelivered($all, $known));

    // AMQP 0-9-1 publishers set the same header; stream filters see it.
    $raw = Amqp10::writeProps(['deliveryMode' => 2], [['x-stream-filter-value', 'red']]);
    $b->publish(0, 0, 0, '', 'filt', 'amqp-red', 2, 0, [['x-stream-filter-value', 'red']], null, $raw);
    $raw = Amqp10::writeProps(['deliveryMode' => 2], [['x-stream-filter-value', 'green']]);
    $b->publish(0, 0, 0, '', 'filt', 'amqp-green', 2, 0, [['x-stream-filter-value', 'green']], null, $raw);
    $b->flushDurable();
    $later = sfSend($p, $s, 23, '');
    $bySub = [];
    foreach ($later as $f) if ($f['key'] === 8) foreach ($known as $text) if (str_contains($f['body'], sfData($text))) $bySub[ord($f['body'][0])][] = $text;
    Harness::eq('an AMQP 0-9-1 x-stream-filter-value reaches the matching stream subscriber', ['amqp-red'], $bySub[1] ?? []);
    Harness::eq('and not the non-matching one', [], $bySub[2] ?? []);
    Harness::eq('an unfiltered subscriber gets both', ['amqp-red', 'amqp-green'], $bySub[4] ?? []);
});
Harness::done();
