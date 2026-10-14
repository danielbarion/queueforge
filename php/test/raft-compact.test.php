<?php
// A group of large bodies compacts by bytes, not only after 10,000 entries.
require_once dirname(__DIR__) . '/src/RaftNode.php';
function check($ok, $why) { if (!$ok) throw new Exception($why); echo "ok $why\n"; }
$dir = sys_get_temp_dir() . '/qf-compact-' . bin2hex(random_bytes(4));
try {
    $applied = 0;
    $n = new RaftNode('solo', $dir, ['solo'], function () {}, function () use (&$applied) { $applied++; }, function () {}, fn () => ['messages' => []]);
    $groups = new ReflectionProperty(RaftNode::class, 'groups');
    foreach ($groups->getValue($n) as $row) $row['core']->startTimer(0);
    $n->tick();
    $body = base64_encode(str_repeat('x', 4 * 1024 * 1024));
    $count = intdiv(RaftNode::COMPACT_BYTES, strlen($body)) + 2;
    $done = 0;
    for ($i = 0; $i < $count; $i++) {
        $n->propose('quorum', 'enq', ['message_id' => "m$i", 'body_b64' => $body], function ($ok) use (&$done) { if ($ok) $done++; });
        $n->tick();
    }
    for ($i = 0; $i < 20 && $done < $count; $i++) { usleep(10000); $n->tick(); }
    check($done === $count && $applied === $count, "all $count large entries confirmed and applied");
    $core = $groups->getValue($n)['quorum']['core'];
    check(count($core->logEntries()) < $count, 'the in-memory log was compacted by bytes');
    check(is_file("$dir/quorum/snapshot.json"), 'the compaction wrote a snapshot');
    check(filesize("$dir/quorum/log.jsonl") < RaftNode::COMPACT_BYTES, 'the log file was rewritten short');
} finally {
    if (is_dir($dir)) {
        $it = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($dir, FilesystemIterator::SKIP_DOTS), RecursiveIteratorIterator::CHILD_FIRST);
        foreach ($it as $p) $p->isDir() ? rmdir($p->getPathname()) : unlink($p->getPathname());
        rmdir($dir);
    }
}
