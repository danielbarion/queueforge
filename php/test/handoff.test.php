<?php
declare(strict_types=1);

/** The frame that names a queue is what decides which process keeps the connection. */

require_once __DIR__ . '/lib/Harness.php';
require_once dirname(__DIR__) . '/src/Codec.php';
require_once dirname(__DIR__) . '/src/Handoff.php';

Harness::guard('queue named by a frame', static function (): void {
    $publish = Codec::method(1, 60, 40, pack('n', 0) . Codec::shortstr('') . Codec::shortstr('q3') . chr(0));
    Harness::eq('default exchange publish names the queue', 'q3', Handoff::namedQueue($publish, 0));
    $other = Codec::method(1, 60, 40, pack('n', 0) . Codec::shortstr('ex') . Codec::shortstr('q3') . chr(0));
    Harness::eq('a named exchange is not a queue home', null, Handoff::namedQueue($other, 0));
    $consume = Codec::method(1, 60, 20, pack('n', 0) . Codec::shortstr('q7') . Codec::shortstr('ctag') . chr(0));
    Harness::eq('consume names the queue', 'q7', Handoff::namedQueue($consume, 0));
    Harness::eq('a short buffer waits', null, Handoff::namedQueue(substr($publish, 0, 6), 0));
});

Harness::done();
