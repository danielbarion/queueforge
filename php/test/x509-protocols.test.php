<?php
declare(strict_types=1);

/**
 * x.509 client certificates on every TLS listener: MQTT (mqtts) and STOMP
 * (stomps) with no user name log in as the verified certificate's CN, and
 * AMQP 1.0 offers and accepts SASL EXTERNAL on the amqps port.
 */

require_once __DIR__ . '/lib/Harness.php';
foreach (['Codec', 'Auth', 'Amqp10'] as $file) require_once dirname(__DIR__) . '/src/' . $file . '.php';

$pki = sys_get_temp_dir() . '/qf-x509-' . bin2hex(random_bytes(4));
mkdir($pki);
$run = static function (array $cmd): void {
    $p = proc_open($cmd, [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
    $err = stream_get_contents($pipes[2]); foreach ($pipes as $fp) fclose($fp);
    if (proc_close($p) !== 0) throw new RuntimeException($err);
};
$run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', "$pki/ca.key", '-out', "$pki/ca.pem", '-subj', '/CN=QF x509 CA', '-days', '1']);
foreach (['server' => 'localhost', 'client' => 'cert-user', 'nobody' => 'missing-user'] as $name => $cn) {
    $run(['openssl', 'req', '-newkey', 'rsa:2048', '-nodes', '-keyout', "$pki/$name.key", '-out', "$pki/$name.csr", '-subj', "/CN=$cn"]);
    file_put_contents("$pki/$name.ext", $name === 'server' ? "subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n" : "extendedKeyUsage=clientAuth\n");
    $run(['openssl', 'x509', '-req', '-in', "$pki/$name.csr", '-CA', "$pki/ca.pem", '-CAkey', "$pki/ca.key", '-CAcreateserial', '-out', "$pki/$name.pem", '-days', '1', '-extfile', "$pki/$name.ext"]);
}

$amqps = Harness::freePort(); $mqtts = Harness::freePort(); $stomps = Harness::freePort(); $mgmt = Harness::freePort();
$broker = Harness::broker([
    "amqps = \"127.0.0.1:$amqps\"", "mqtts = \"127.0.0.1:$mqtts\"", "stomps = \"127.0.0.1:$stomps\"", "management = \"127.0.0.1:$mgmt\"",
    '', '[tls]', 'enabled = false', "cert_path = \"$pki/server.pem\"", "key_path = \"$pki/server.key\"", "ca_path = \"$pki/ca.pem\"",
]);
foreach ([$amqps, $mqtts, $stomps, $mgmt] as $p) Harness::waitListening($p, $broker['dir']);
Harness::http($mgmt, 'PUT', '/api/users/cert-user', ['password' => 'unused-password-1', 'tags' => '']);
Harness::http($mgmt, 'PUT', '/api/permissions/%2f/cert-user', ['configure' => '.*', 'write' => '.*', 'read' => '.*']);

/** A TLS client socket, optionally presenting `$cert`. */
$tls = static function (int $port, ?string $cert) use ($pki) {
    $ssl = ['cafile' => "$pki/ca.pem", 'verify_peer' => true, 'peer_name' => 'localhost'];
    if ($cert !== null) { $ssl['local_cert'] = "$pki/$cert.pem"; $ssl['local_pk'] = "$pki/$cert.key"; }
    $fp = @stream_socket_client("tls://127.0.0.1:$port", $errno, $errstr, 3, STREAM_CLIENT_CONNECT, stream_context_create(['ssl' => $ssl]));
    if ($fp === false) throw new RuntimeException("TLS connect: $errstr");
    stream_set_timeout($fp, 3);
    return $fp;
};
$readN = static function ($fp, int $n): string { $b = ''; while (strlen($b) < $n) { $c = fread($fp, $n - strlen($b)); if ($c === false || $c === '') break; $b .= $c; } return $b; };

$mqttConnect = static function (?string $cert, string $user = '') use ($tls, $mqtts, $readN): int {
    $fp = $tls($mqtts, $cert);
    $flags = 0x02 | ($user !== '' ? 0xC0 : 0);
    $body = pack('n', 4) . 'MQTT' . chr(4) . chr($flags) . pack('n', 30) . pack('n', 6) . 'x509-c' . ($user !== '' ? pack('n', strlen($user)) . $user . pack('n', 8) . 'wrong-pw' : '');
    fwrite($fp, chr(0x10) . chr(strlen($body)) . $body);
    $ack = $readN($fp, 4); fclose($fp);
    return strlen($ack) === 4 && ord($ack[0]) === 0x20 ? ord($ack[3]) : -1;
};
Harness::guard('MQTT over TLS', static function () use ($mqttConnect): void {
    Harness::eq('a client certificate and no user name log in as the CN', 0, $mqttConnect('client'));
    Harness::ok('no certificate and no user name is refused', $mqttConnect(null) !== 0);
    Harness::ok('a certificate for an unknown user is refused', $mqttConnect('nobody') !== 0);
    Harness::ok('a user name still needs its password', $mqttConnect('client', 'cert-user') !== 0);
});

$stompConnect = static function (?string $cert) use ($tls, $stomps): string {
    $fp = $tls($stomps, $cert);
    fwrite($fp, "CONNECT\naccept-version:1.2\nhost:/\n\n\0");
    $out = '';
    while (!str_contains($out, "\0")) { $c = fread($fp, 4096); if ($c === false || $c === '') break; $out .= $c; }
    fclose($fp);
    return explode("\n", $out)[0];
};
Harness::guard('STOMP over TLS', static function () use ($stompConnect): void {
    Harness::eq('CONNECT without login and with a certificate', 'CONNECTED', $stompConnect('client'));
    Harness::eq('without login and without a certificate', 'ERROR', $stompConnect(null));
    Harness::eq('a certificate for an unknown user', 'ERROR', $stompConnect('nobody'));
});

$saslExternal = static function (?string $cert) use ($tls, $amqps, $readN): array {
    $fp = $tls($amqps, $cert);
    fwrite($fp, "AMQP\x03\x01\x00\x00");
    $readN($fp, 8);
    $frame = static function () use ($fp, $readN): mixed { $h = $readN($fp, 8); if (strlen($h) < 8) return null; $size = unpack('N', $h)[1]; $body = $readN($fp, $size - 8); return (new Amqp10Codec(substr($body, ord($h[4]) * 4 - 8)))->value(); };
    $mechs = $frame();
    $offered = array_map(static fn ($s) => Amqp10Codec::scalar($s), (array) Amqp10Codec::scalar($mechs->value[0] ?? []));
    $init = Amqp10Codec::encode(new Amqp10Described(0x41, [Amqp10Codec::symbol('EXTERNAL'), Amqp10Codec::binary('')]));
    fwrite($fp, pack('NCCn', strlen($init) + 8, 2, 1, 0) . $init);
    $outcome = $frame();
    fclose($fp);
    return [$offered, $outcome instanceof Amqp10Described ? (int) Amqp10Codec::scalar($outcome->value[0] ?? 1) : -1];
};
Harness::guard('AMQP 1.0 SASL EXTERNAL over TLS', static function () use ($saslExternal): void {
    [$offered, $code] = $saslExternal('client');
    Harness::ok('EXTERNAL is offered with a verified certificate', in_array('EXTERNAL', $offered, true));
    Harness::eq('EXTERNAL logs in as the CN', 0, $code);
    [$offered] = $saslExternal(null);
    Harness::ok('EXTERNAL is not offered without a certificate', !in_array('EXTERNAL', $offered, true));
    [, $code] = $saslExternal('nobody');
    Harness::ok('a certificate for an unknown user is refused', $code !== 0);
});

Harness::stop($broker);
Harness::done();
