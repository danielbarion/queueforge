<?php
declare(strict_types=1);

/**
 * Parses every PHP file in the project. The broker has no composer autoloader
 * and no build step, so a syntax error would otherwise only surface when the
 * failing line actually ran.
 */

require_once __DIR__ . '/lib/Harness.php';

$root = dirname(__DIR__);
$targets = array_merge(
    glob($root . '/src/*.php') ?: [],
    glob($root . '/test/*.php') ?: [],
    glob($root . '/test/lib/*.php') ?: [],
    [$root . '/bin/queueforge'],
);
sort($targets);

foreach ($targets as $file) {
    if (!is_file($file)) {
        continue;
    }
    $proc = proc_open(['php', '-l', $file], [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
    if (!is_resource($proc)) {
        Harness::ok('lint ' . basename($file), false, 'could not run php -l');
        continue;
    }
    $out = (string) stream_get_contents($pipes[1]);
    $err = (string) stream_get_contents($pipes[2]);
    fclose($pipes[1]);
    fclose($pipes[2]);
    $code = proc_close($proc);
    $label = 'lint ' . str_replace($root . '/', '', $file);
    Harness::ok($label, $code === 0, trim($out . ' ' . $err));
}

Harness::done();
