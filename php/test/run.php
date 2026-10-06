<?php
declare(strict_types=1);

/**
 * Runs every check in this directory. Each test file is a separate process so
 * one crash cannot take the rest down, and the exit code is the overall
 * result. Designed to run inside the bench container with this directory
 * mounted:
 *
 *   docker compose -f docker-compose.bench.yml run --rm --no-deps \
 *     --entrypoint php -v ./php/test:/opt/queueforge/test \
 *     php test/run.php
 */

$root = dirname(__DIR__);
$only = $argv[1] ?? '';

$files = glob(__DIR__ . '/*.test.php') ?: [];
sort($files);
// The two original scripts predate the *.test.php convention.
foreach (['roundtrip.php', 'parity.php'] as $legacy) {
    if (is_file(__DIR__ . '/' . $legacy)) {
        $files[] = __DIR__ . '/' . $legacy;
    }
}

if ($only !== '') {
    $files = array_values(array_filter(
        $files,
        static fn (string $f): bool => str_contains(basename($f), $only),
    ));
}

if ($files === []) {
    fwrite(STDERR, "no test files matched\n");
    exit(2);
}

fwrite(STDOUT, 'php ' . PHP_VERSION . ', ' . count($files) . " test files\n\n");

$failed = [];
$started = microtime(true);
foreach ($files as $file) {
    $name = basename($file);
    fwrite(STDOUT, "$name\n");
    $began = microtime(true);
    $proc = proc_open(['php', $file], [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes, $root);
    if (!is_resource($proc)) {
        $failed[] = $name;
        fwrite(STDOUT, "  FAIL could not start\n\n");
        continue;
    }
    $out = stream_get_contents($pipes[1]);
    $err = stream_get_contents($pipes[2]);
    fclose($pipes[1]);
    fclose($pipes[2]);
    $code = proc_close($proc);
    $took = sprintf('%.1fs', microtime(true) - $began);

    $body = trim((string) $out);
    if ($body !== '') {
        foreach (explode("\n", $body) as $line) {
            fwrite(STDOUT, (str_starts_with($line, '  ') ? $line : '  ' . $line) . "\n");
        }
    }
    if ($code !== 0) {
        $failed[] = $name;
        $detail = trim((string) $err);
        if ($detail !== '') {
            foreach (explode("\n", $detail) as $line) {
                fwrite(STDOUT, "  $line\n");
            }
        }
        fwrite(STDOUT, "  FAILED in $took\n\n");
        continue;
    }
    fwrite(STDOUT, "  passed in $took\n\n");
}

$elapsed = sprintf('%.1fs', microtime(true) - $started);
if ($failed !== []) {
    fwrite(STDOUT, count($failed) . ' of ' . count($files) . " files failed in $elapsed: " . implode(', ', $failed) . "\n");
    exit(1);
}
fwrite(STDOUT, count($files) . " files passed in $elapsed\n");
exit(0);
