<?php
declare(strict_types=1);

/**
 * LDAP resource access queries ([auth.ldap.resource_access]): constant and
 * in_group entries for vhost, configure, write, read and topic, the member
 * attribute, group lookups bound as bind_dn, and one lookup per group per
 * login.
 */

require_once __DIR__ . '/lib/Harness.php';
foreach (['Routing','Features','Policy','Codec','Auth','Store','Broker','Security'] as $file) require_once dirname(__DIR__) . '/src/' . $file . '.php';

$dir = sys_get_temp_dir() . '/qf-ldap-access-' . bin2hex(random_bytes(5));
mkdir($dir);
$port = Harness::freePort();
// A BER LDAP peer: binds succeed for the right passwords, and a base search
// on a group matches when "<attribute>=<user dn>" is listed for it.
$mock = <<<'MOCK'
<?php
[$script, $port, $dir] = $argv;
$groups = ['cn=admins,dc=qf' => ['member' => ['uid=alice,ou=people,dc=qf']], 'cn=writers,dc=qf' => ['uniqueMember' => ['uid=bob,ou=people,dc=qf']]];
$listener = stream_socket_server("tcp://127.0.0.1:$port", $errno, $error);
echo "ready\n"; fflush(STDOUT);
function readExact($c, $n) { $s = ''; while (strlen($s) < $n) { $b = fread($c, $n - strlen($s)); if ($b === false || $b === '') throw new Exception('closed'); $s .= $b; } return $s; }
function frame($c) { $h = readExact($c, 2); $n = ord($h[1]); $more = ''; if ($n & 128) { $k = $n & 127; $more = readExact($c, $k); $n = 0; for ($i = 0; $i < $k; $i++) $n = ($n << 8) | ord($more[$i]); } return $h . $more . readExact($c, $n); }
function tlv(string $b, int &$at): array { $tag = ord($b[$at++]); $n = ord($b[$at++]); if ($n & 128) { $k = $n & 127; $len = 0; for ($i = 0; $i < $k; $i++) $len = ($len << 8) | ord($b[$at++]); $n = $len; } $v = substr($b, $at, $n); $at += $n; return [$tag, $v]; }
function response($id, $tag, $body) { return "\x30" . chr(3 + 2 + strlen($body)) . "\x02\x01" . chr($id) . chr($tag) . chr(strlen($body)) . $body; }
while ($c = @stream_socket_accept($listener, 30)) {
    stream_set_timeout($c, 2);
    try {
        for ($step = 0; $step < 16; $step++) {
            $wire = frame($c); file_put_contents("$dir/ldap.log", base64_encode($wire) . "\n", FILE_APPEND);
            $at = 0; [, $msg] = tlv($wire, $at); $in = 0; [, $idRaw] = tlv($msg, $in); $id = ord($idRaw[strlen($idRaw) - 1]); [$op, $body] = tlv($msg, $in);
            if ($op === 0x42) break;
            if ($op === 0x60) { $ok = str_contains($body, 'secret'); fwrite($c, response($id, 0x61, "\x0a\x01" . chr($ok ? 0 : 49) . "\x04\x00\x04\x00")); continue; }
            if ($op === 0x63) {
                $b = 0; [, $base] = tlv($body, $b); for ($i = 0; $i < 5; $i++) tlv($body, $b); [, $filter] = tlv($body, $b);
                $f = 0; [, $attr] = tlv($filter, $f); [, $dn] = tlv($filter, $f);
                if (in_array($dn, $groups[$base][$attr] ?? [], true)) fwrite($c, response($id, 0x64, "\x04\x01x\x30\x00"));
                fwrite($c, response($id, 0x65, "\x0a\x01\x00\x04\x00\x04\x00"));
            }
        }
    } catch (Throwable $e) {}
    fclose($c);
}
MOCK;
file_put_contents("$dir/mock.php", $mock);
$proc = proc_open([PHP_BINARY, "$dir/mock.php", (string) $port, $dir], [1 => ['pipe', 'w'], 2 => ['file', "$dir/mock.err", 'a']], $pipes);
if (!is_resource($proc) || fgets($pipes[1]) !== "ready\n") throw new RuntimeException('LDAP mock did not start');

$broker = new Broker(new Store("$dir/messages.log"), "$dir/users.json");
$ldap = ['server' => '127.0.0.1', 'port' => $port, 'user_dn_pattern' => 'uid=${username},ou=people,dc=qf', 'admin_group' => 'cn=admins,dc=qf', 'bind_dn' => 'cn=svc,dc=qf', 'bind_password' => 'svc-secret'];
$searches = static function () use ($dir): int {
    $n = 0;
    foreach (array_filter(explode("\n", (string) @file_get_contents("$dir/ldap.log"))) as $line) if (str_contains(base64_decode($line), "\x63") && str_contains(base64_decode($line), 'cn=') && str_contains(base64_decode($line), '1.1')) $n++;
    return $n;
};

Harness::guard('access query parsing', static function (): void {
    Harness::eq('in_group', ['in_group' => 'cn=admins,dc=qf'], Security::accessQuery('{ in_group = "cn=admins,dc=qf" }'));
    Harness::eq('in_group with attribute', ['in_group' => 'cn=w,dc=qf', 'attribute' => 'uniqueMember'], Security::accessQuery('{ in_group = "cn=w,dc=qf", attribute = "uniqueMember" }'));
    Harness::eq('constant', ['constant' => false], Security::accessQuery('{ constant = false }'));
});

Harness::guard('configure limited to a group', static function () use ($broker, $ldap, $searches): void {
    $cfg = ['ldap' => $ldap, 'ldap.resource_access' => ['configure' => '{ in_group = "cn=admins,dc=qf" }', 'write' => '{ constant = true }', 'read' => '{ constant = true }']];
    $before = $searches();
    $alice = Security::verify($broker, $cfg, 'alice', 'alice-secret');
    Harness::ok('a group member configures', $alice !== null && Security::allows($alice, '/', 'configure', 'q'));
    Harness::eq('admin_group and the access query share one lookup', 1, $searches() - $before);
    $bob = Security::verify($broker, $cfg, 'bob', 'bob-secret');
    Harness::ok('a non-member cannot configure', $bob !== null && !Security::allows($bob, '/', 'configure', 'q'));
    Harness::ok('but writes and reads', Security::allows($bob, '/', 'write', 'q') && Security::allows($bob, '/', 'read', 'q'));
    Harness::ok('and keeps vhost access', Security::hasVhost($bob, '/'));
    Harness::ok('a wrong password still fails', Security::verify($broker, $cfg, 'bob', 'nope') === null);
});

Harness::guard('member attribute, vhost and topic queries', static function () use ($broker, $ldap): void {
    $cfg = ['ldap' => $ldap, 'ldap.resource_access' => ['write' => ['in_group' => 'cn=writers,dc=qf', 'attribute' => 'uniqueMember']]];
    $bob = Security::verify($broker, $cfg, 'bob', 'bob-secret');
    Harness::ok('uniqueMember grants write', $bob !== null && Security::allows($bob, '/', 'write', 'x'));
    $alice = Security::verify($broker, $cfg, 'alice', 'alice-secret');
    Harness::ok('a missing key defaults to constant true', $alice !== null && Security::allows($alice, '/', 'configure', 'x') && !Security::allows($alice, '/', 'write', 'x'));
    $none = Security::verify($broker, ['ldap' => $ldap, 'ldap.resource_access' => ['vhost' => '{ constant = false }']], 'alice', 'alice-secret');
    Harness::ok('a refused vhost query denies the vhost', $none !== null && !Security::hasVhost($none, '/'));
    $topic = Security::verify($broker, ['ldap' => $ldap, 'ldap.resource_access' => ['topic' => '{ constant = false }']], 'alice', 'alice-secret');
    Harness::ok('a refused topic query denies routing keys but not the resource', $topic !== null && Security::allows($topic, '/', 'write', 'x') && !Security::allows($topic, '/', 'write', 'amq.topic', 'a.b'));
});

proc_terminate($proc); foreach ($pipes as $p) fclose($p); proc_close($proc);
Harness::done();
