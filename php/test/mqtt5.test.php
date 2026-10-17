<?php
declare(strict_types=1);

/**
 * MQTT 5.0: topic aliases, subscription options (no-local,
 * retain-as-published, retain handling), subscription identifiers, message
 * and session expiry, will delay, and retained messages across a restart.
 */

require_once __DIR__ . '/lib/Harness.php';

$mqttPort = Harness::freePort();
$broker = Harness::broker(["mqtt = \"127.0.0.1:$mqttPort\""]);
Harness::waitListening($mqttPort, $broker['dir']);

final class Mqtt
{
    /** @var resource */
    public $fp;
    public string $buf = '';
    public bool $sessionPresent = false;
    public string $connackProps = '';
    private int $pid = 1;
    /** Packets read while waiting for an ack, handed out by next() first. */
    private array $pending = [];

    public static function len(int $n): string { $o = ''; do { $b = $n % 128; $n = intdiv($n, 128); $o .= chr($n ? $b | 128 : $b); } while ($n); return $o; }
    public static function str(string $s): string { return pack('n', strlen($s)) . $s; }
    public static function packet(int $first, string $body): string { return chr($first) . self::len(strlen($body)) . $body; }
    public static function props(array $p): string { $raw = implode('', $p); return self::len(strlen($raw)) . $raw; }
    public static function u32(int $id, int $v): string { return chr($id) . pack('N', $v); }
    public static function u16(int $id, int $v): string { return chr($id) . pack('n', $v); }

    /** @param array{clean?:bool,props?:list<string>,will?:array{topic:string,body:string,qos?:int,retain?:bool,props?:list<string>}} $o */
    public function __construct(int $port, public string $client, array $o = [])
    {
        $fp = stream_socket_client("tcp://127.0.0.1:$port", $errno, $errstr, 3);
        if ($fp === false) throw new RuntimeException("mqtt connect: $errstr");
        $this->fp = $fp;
        stream_set_blocking($fp, false);
        $flags = 0xC0 | (($o['clean'] ?? true) ? 2 : 0);
        $will = '';
        if (isset($o['will'])) {
            $w = $o['will'];
            $flags |= 4 | (($w['qos'] ?? 0) << 3) | (($w['retain'] ?? false) ? 32 : 0);
            $will = self::props($w['props'] ?? []) . self::str($w['topic']) . self::str($w['body']);
        }
        $body = self::str('MQTT') . chr(5) . chr($flags) . pack('n', 30) . self::props($o['props'] ?? []) . self::str($client) . $will . self::str('admin') . self::str('devpassword12');
        $this->send(self::packet(0x10, $body));
        $ack = $this->next(3.0);
        if ($ack === null || $ack[0] !== 2 || ord($ack[2][1]) !== 0) throw new RuntimeException('CONNACK refused');
        $this->sessionPresent = (ord($ack[2][0]) & 1) === 1;
        $at = 2; $n = ord($ack[2][$at]); $this->connackProps = substr($ack[2], $at + 1, $n);
    }
    public function send(string $bytes): void { stream_set_blocking($this->fp, true); fwrite($this->fp, $bytes); stream_set_blocking($this->fp, false); }
    /** @return array{0:int,1:int,2:string}|null type, flags, body */
    public function next(float $seconds = 2.0): ?array
    {
        if ($this->pending !== []) return array_shift($this->pending);
        $deadline = microtime(true) + $seconds;
        while (true) {
            if (strlen($this->buf) >= 2) {
                $n = 0; $m = 1; $at = 1;
                while ($at < strlen($this->buf)) { $b = ord($this->buf[$at++]); $n += ($b & 127) * $m; if (!($b & 128)) break; $m *= 128; }
                if (strlen($this->buf) >= $at + $n) {
                    $first = ord($this->buf[0]); $body = substr($this->buf, $at, $n); $this->buf = substr($this->buf, $at + $n);
                    return [$first >> 4, $first & 15, $body];
                }
            }
            $left = $deadline - microtime(true);
            if ($left <= 0) return null;
            $r = [$this->fp]; $w = $e = null;
            if (@stream_select($r, $w, $e, 0, (int) min(200000, $left * 1e6)) > 0) {
                $chunk = fread($this->fp, 65536);
                if ($chunk === '' || $chunk === false) { if (feof($this->fp)) return null; continue; }
                $this->buf .= $chunk;
            }
        }
    }
    public function subscribe(string $filter, int $options = 1, array $props = []): string
    {
        $pid = $this->pid++;
        $this->send(self::packet(0x82, pack('n', $pid) . self::props($props) . self::str($filter) . chr($options)));
        $held = [];
        while (($p = $this->next(3.0)) !== null) { if ($p[0] === 9) { $this->pending = [...$this->pending, ...$held]; return substr($p[2], 3); } $held[] = $p; }
        throw new RuntimeException('no SUBACK');
    }
    public function publish(string $topic, string $body, int $qos = 1, bool $retain = false, array $props = []): void
    {
        $pid = $this->pid++;
        $this->send(self::packet(0x30 | ($qos << 1) | ($retain ? 1 : 0), self::str($topic) . ($qos ? pack('n', $pid) : '') . self::props($props) . $body));
        if ($qos) { $held = []; $deadline = microtime(true) + 3; while (microtime(true) < $deadline && ($p = $this->next(0.5)) !== null) { if ($p[0] === 4) break; $held[] = $p; } $this->pending = [...$this->pending, ...$held]; }
    }
    /** PUBLISH packets for `$seconds`, each acked: topic, body, retain, props. */
    public function messages(float $seconds): array
    {
        $out = [];
        $deadline = microtime(true) + $seconds;
        while (($left = $deadline - microtime(true)) > 0 && ($p = $this->next($left)) !== null) {
            if ($p[0] !== 3) continue;
            $at = 0; $tn = unpack('n', substr($p[2], 0, 2))[1]; $topic = substr($p[2], 2, $tn); $at = 2 + $tn;
            $qos = ($p[1] >> 1) & 3; $pid = 0;
            if ($qos) { $pid = unpack('n', substr($p[2], $at, 2))[1]; $at += 2; }
            $n = 0; $m = 1; while (true) { $b = ord($p[2][$at++]); $n += ($b & 127) * $m; if (!($b & 128)) break; $m *= 128; }
            $props = Protocols::mqttPropList(substr($p[2], $at, $n)); $at += $n;
            $out[] = ['topic' => $topic, 'body' => substr($p[2], $at), 'retain' => ($p[1] & 1) === 1, 'props' => $props];
            if ($qos) $this->send(self::packet(0x40, pack('n', $pid)));
        }
        return $out;
    }
    public function drop(): void { if (is_resource($this->fp)) fclose($this->fp); }
    public function disconnect(): void { $this->send("\xe0\0"); $this->drop(); }
}
foreach (['Routing','Features','Policy','Codec','Auth','Store','Broker','Amqp10','Protocols'] as $file) require_once dirname(__DIR__) . '/src/' . $file . '.php';
$bodies = static fn (array $ms): array => array_column($ms, 'body');
$prop = static function (array $m, int $id): array { $out = []; foreach ($m['props'] as [$k, $v]) if ($k === $id) $out[] = $v; return $out; };

Harness::guard('topic alias', static function () use ($mqttPort): void {
    $sub = new Mqtt($mqttPort, 'alias-sub'); $sub->subscribe('alias/t');
    $pub = new Mqtt($mqttPort, 'alias-pub');
    Harness::ok('CONNACK advertises topic-alias-maximum', str_contains($pub->connackProps, "\x22\x00\x10"));
    $pub->publish('alias/t', 'first', 1, false, [Mqtt::u16(0x23, 1)]);
    $pub->publish('', 'second', 1, false, [Mqtt::u16(0x23, 1)]);
    $got = $sub->messages(1.0);
    Harness::eq('an empty topic resolves through the alias', [['alias/t', 'first'], ['alias/t', 'second']], array_map(static fn ($m) => [$m['topic'], $m['body']], $got));
    $sub->disconnect(); $pub->disconnect();
});

Harness::guard('no-local and retain-as-published', static function () use ($mqttPort, $bodies): void {
    $c = new Mqtt($mqttPort, 'nl-self');
    $c->subscribe('nl/t', 1 | 4);
    $c->publish('nl/t', 'mine');
    $other = new Mqtt($mqttPort, 'nl-other'); $other->publish('nl/t', 'theirs');
    Harness::eq('own publishes do not come back', ['theirs'], $bodies($c->messages(0.8)));
    $keep = new Mqtt($mqttPort, 'rap-keep'); $keep->subscribe('rap/t', 1 | 8);
    $drop = new Mqtt($mqttPort, 'rap-drop'); $drop->subscribe('rap/t', 1);
    $other->publish('rap/t', 'flagged', 1, true);
    Harness::eq('rap keeps the retain flag', [true], array_column($keep->messages(0.8), 'retain'));
    Harness::eq('without rap the retain flag is cleared', [false], array_column($drop->messages(0.3), 'retain'));
    $other->publish('rap/t', '', 1, true);
    foreach ([$c, $other, $keep, $drop] as $x) $x->disconnect();
});

Harness::guard('retain handling and subscription identifiers', static function () use ($mqttPort, $bodies, $prop): void {
    $pub = new Mqtt($mqttPort, 'rh-pub'); $pub->publish('rh/t', 'stored', 1, true);
    $none = new Mqtt($mqttPort, 'rh-2'); $none->subscribe('rh/t', 1 | (2 << 4));
    Harness::eq('retain handling 2 sends nothing on subscribe', [], $bodies($none->messages(0.5)));
    $once = new Mqtt($mqttPort, 'rh-1'); $once->subscribe('rh/t', 1 | (1 << 4));
    Harness::eq('retain handling 1 sends on a new subscription', ['stored'], $bodies($once->messages(0.5)));
    $once->subscribe('rh/t', 1 | (1 << 4));
    Harness::eq('retain handling 1 skips an existing subscription', [], $bodies($once->messages(0.5)));
    $all = new Mqtt($mqttPort, 'rh-0'); $all->subscribe('rh/t', 1);
    $first = $all->messages(0.5);
    Harness::eq('retain handling 0 sends with retain set', [true], array_column($first, 'retain'));
    $pub->publish('rh/t', '', 1, true);

    $s = new Mqtt($mqttPort, 'subid'); $s->subscribe('subid/+', 1, ["\x0B" . Mqtt::len(42)]); $s->subscribe('subid/x', 1, ["\x0B" . Mqtt::len(7)]);
    $pub->publish('subid/x', 'tagged');
    $got = $s->messages(0.8);
    $ids = array_map(static function ($v) { $at = 0; $n = 0; $m = 1; do { $b = ord($v[$at++]); $n += ($b & 127) * $m; $m *= 128; } while ($b & 128); return $n; }, $prop($got[0] ?? ['props' => []], 0x0B));
    sort($ids);
    Harness::eq('every matching subscription identifier is included', [7, 42], $ids);
    foreach ([$pub, $none, $once, $all, $s] as $x) $x->disconnect();
});

Harness::guard('message expiry', static function () use ($mqttPort, $bodies, $prop): void {
    $pub = new Mqtt($mqttPort, 'exp-pub');
    $pub->publish('exp/short', 'short', 1, true, [Mqtt::u32(0x02, 1)]);
    $pub->publish('exp/long', 'long', 1, true, [Mqtt::u32(0x02, 60)]);
    // A queued message of an offline session also expires.
    $away = new Mqtt($mqttPort, 'exp-away', ['props' => [Mqtt::u32(0x11, 60)]]); $away->subscribe('exp/queued'); $away->drop();
    usleep(300000);
    $pub->publish('exp/queued', 'gone-soon', 1, false, [Mqtt::u32(0x02, 1)]);
    $pub->publish('exp/queued', 'kept', 1, false, [Mqtt::u32(0x02, 60)]);
    usleep(2200000);
    $sub = new Mqtt($mqttPort, 'exp-sub'); $sub->subscribe('exp/long'); $sub->subscribe('exp/short');
    $got = $sub->messages(0.8);
    Harness::eq('an expired retained message is not delivered', ['long'], $bodies($got));
    $left = isset($got[0]) ? unpack('N', $prop($got[0], 0x02)[0] ?? "\0\0\0\0")[1] : 0;
    Harness::ok('the remaining interval is forwarded', $left > 0 && $left < 60, "left=$left");
    $back = new Mqtt($mqttPort, 'exp-away', ['clean' => false, 'props' => [Mqtt::u32(0x11, 60)]]);
    Harness::eq('an expired queued message is dropped', ['kept'], $bodies($back->messages(1.0)));
    $pub->publish('exp/long', '', 1, true);
    foreach ([$pub, $sub, $back] as $x) $x->disconnect();
});

Harness::guard('session expiry', static function () use ($mqttPort, $bodies): void {
    $first = new Mqtt($mqttPort, 'sess-keep', ['props' => [Mqtt::u32(0x11, 60)]]);
    Harness::ok('a clean start has no session', !$first->sessionPresent);
    $first->subscribe('sess/t'); $first->drop();
    usleep(300000);
    $pub = new Mqtt($mqttPort, 'sess-pub'); $pub->publish('sess/t', 'while-away');
    $again = new Mqtt($mqttPort, 'sess-keep', ['clean' => false, 'props' => [Mqtt::u32(0x11, 60)]]);
    Harness::ok('the session is present on reconnect', $again->sessionPresent);
    Harness::eq('queued QoS 1 messages are delivered', ['while-away'], $bodies($again->messages(1.0)));
    $again->drop();

    $short = new Mqtt($mqttPort, 'sess-short', ['props' => [Mqtt::u32(0x11, 1)]]); $short->subscribe('sess/short'); $short->drop();
    usleep(2500000);
    $late = new Mqtt($mqttPort, 'sess-short', ['clean' => false, 'props' => [Mqtt::u32(0x11, 1)]]);
    Harness::ok('the session ends after its expiry interval', !$late->sessionPresent);
    $zero = new Mqtt($mqttPort, 'sess-zero'); $zero->subscribe('sess/zero'); $zero->drop();
    usleep(300000);
    $zero2 = new Mqtt($mqttPort, 'sess-zero', ['clean' => false]);
    Harness::ok('expiry 0 ends the session at disconnect', !$zero2->sessionPresent);
    foreach ([$pub, $late, $zero2] as $x) $x->disconnect();
});

Harness::guard('will delay interval', static function () use ($mqttPort, $bodies): void {
    $watch = new Mqtt($mqttPort, 'will-watch'); $watch->subscribe('will/+');
    $dying = new Mqtt($mqttPort, 'will-dying', ['props' => [Mqtt::u32(0x11, 10)], 'will' => ['topic' => 'will/a', 'body' => 'gone', 'qos' => 1, 'props' => [Mqtt::u32(0x18, 2)]]]);
    $dying->drop();
    Harness::eq('the will waits for its delay', [], $bodies($watch->messages(1.0)));
    $got = $watch->messages(2.5);
    Harness::eq('then it is published', ['gone'], $bodies($got));
    Harness::ok('the will delay property is not forwarded', !in_array(0x18, array_column($got[0]['props'] ?? [], 0), true));
    $back = new Mqtt($mqttPort, 'will-back', ['props' => [Mqtt::u32(0x11, 10)], 'will' => ['topic' => 'will/b', 'body' => 'never', 'props' => [Mqtt::u32(0x18, 2)]]]);
    $back->drop();
    usleep(500000);
    $again = new Mqtt($mqttPort, 'will-back', ['clean' => false, 'props' => [Mqtt::u32(0x11, 10)]]);
    Harness::eq('reconnecting in time cancels the will', [], $bodies($watch->messages(2.5)));
    // The session expiry caps the will delay.
    $capped = new Mqtt($mqttPort, 'will-capped', ['props' => [Mqtt::u32(0x11, 1)], 'will' => ['topic' => 'will/c', 'body' => 'capped', 'props' => [Mqtt::u32(0x18, 30)]]]);
    $capped->drop();
    Harness::eq('a shorter session expiry sends the will sooner', ['capped'], $bodies($watch->messages(2.5)));
    $again->disconnect(); $watch->disconnect();
});

Harness::guard('retained messages survive a restart', static function () use (&$broker, $mqttPort, $bodies): void {
    $pub = new Mqtt($mqttPort, 'ret-pub');
    $pub->publish('keep/me', 'retained-across-restart', 1, true);
    $pub->disconnect();
    $long = new Mqtt($mqttPort, 'sess-restart', ['props' => [Mqtt::u32(0x11, 60)]]); $long->subscribe('sr/t', 1, ["\x0B" . Mqtt::len(9)]); $long->drop();
    $short = new Mqtt($mqttPort, 'sess-restart-short', ['props' => [Mqtt::u32(0x11, 2)]]); $short->subscribe('sr/short'); $short->drop();
    usleep(200000);
    $broker = Harness::restart($broker);
    Harness::waitListening($mqttPort, $broker['dir']);
    $back = new Mqtt($mqttPort, 'sess-restart', ['clean' => false, 'props' => [Mqtt::u32(0x11, 60)]]);
    Harness::ok('a session with an expiry interval survives a restart', $back->sessionPresent);
    $other = new Mqtt($mqttPort, 'sess-restart-pub'); $other->publish('sr/t', 'after-restart');
    $got = $back->messages(1.0);
    $ids = array_map(static fn ($p) => ord($p[1]), array_filter($got[0]['props'] ?? [], static fn ($p) => $p[0] === 0x0B));
    Harness::eq('its subscription identifier is kept', [9], array_values($ids));
    $back->disconnect(); $other->disconnect();
    usleep(2500000);
    $late = new Mqtt($mqttPort, 'sess-restart-short', ['clean' => false, 'props' => [Mqtt::u32(0x11, 2)]]);
    Harness::ok('a parked session still expires after a restart', !$late->sessionPresent);
    $late->disconnect();
    $sub = new Mqtt($mqttPort, 'ret-sub'); $sub->subscribe('keep/me');
    Harness::eq('the retained message is delivered after restart', ['retained-across-restart'], $bodies($sub->messages(1.0)));
    $sub->publish('keep/me', '', 1, true);
    $sub->disconnect();
});

Harness::stop($broker);
Harness::done();
