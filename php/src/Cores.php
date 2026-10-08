<?php
declare(strict_types=1);

/**
 * How many broker processes a container may run.
 *
 * The count comes from the cgroup, not the machine. One CPU stays one
 * process. On a Mac, where there is no cgroup, the answer is one.
 */
final class Cores
{
    public static function countCpuset(string $text): int
    {
        $n = 0;
        foreach (explode(',', $text) as $part) {
            $part = trim($part);
            if (preg_match('/^(\d+)(?:-(\d+))?$/', $part, $m) !== 1) {
                continue;
            }
            $start = (int) $m[1];
            $end = isset($m[2]) && $m[2] !== '' ? (int) $m[2] : $start;
            if ($end >= $start) {
                $n += $end - $start + 1;
            }
        }
        return $n;
    }

    public static function quotaCpus(string $text): ?int
    {
        $parts = preg_split('/\s+/', trim($text)) ?: [];
        if (count($parts) < 2 || $parts[0] === 'max') {
            return null;
        }
        $quota = (int) $parts[0];
        $period = (int) $parts[1];
        if ($period <= 0) {
            return null;
        }
        return max(1, (int) round($quota / $period));
    }

    public static function fromCgroup(?string $cpuMax, ?string $cpuset): int
    {
        $quota = $cpuMax !== null && $cpuMax !== '' ? self::quotaCpus($cpuMax) : null;
        $set = $cpuset !== null && trim($cpuset) !== '' ? self::countCpuset($cpuset) : 0;
        if ($quota !== null && $set > 0) {
            return min($quota, $set);
        }
        if ($quota !== null) {
            return $quota;
        }
        if ($set > 0) {
            return $set;
        }
        return 1;
    }

    public static function granted(): int
    {
        $forced = getenv('QUEUEFORGE_CORES');
        if (is_string($forced) && preg_match('/^[1-9]\\d*$/', $forced) === 1) {
            return (int) $forced;
        }
        $max = @file_get_contents('/sys/fs/cgroup/cpu.max');
        $set = @file_get_contents('/sys/fs/cgroup/cpuset.cpus.effective');
        if (!is_string($set) || trim($set) === '') {
            $set = @file_get_contents('/sys/fs/cgroup/cpuset.cpus');
        }
        return self::fromCgroup(is_string($max) ? $max : null, is_string($set) ? $set : null);
    }

    /**
     * One plan per core. Cluster ports start at 25672.
     *
     * @return list<array{id:string,dir:string,cluster:string,up:string,down:string,mgmt:bool}>
     */
    public static function plans(int $cores, string $dataDir): array
    {
        $root = rtrim($dataDir, '/');
        $plans = [];
        for ($i = 0; $i < $cores; $i++) {
            $id = 'n' . $i;
            $plans[] = [
                'id' => $id,
                'dir' => $root . '/' . $id,
                'cluster' => '127.0.0.1:' . (25672 + $i),
                'up' => $root . '/handoff/' . $id . '.up',
                'down' => $root . '/handoff/' . $id . '.down',
                'mgmt' => $i === 0,
            ];
        }
        return $plans;
    }
}
