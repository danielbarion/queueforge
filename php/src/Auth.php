<?php
declare(strict_types=1);

/** RabbitMQ SHA-256 password hash: base64(salt || SHA-256(salt || password)). */
final class Auth
{
    public static function hash(string $password): string
    {
        $salt = random_bytes(4);
        return base64_encode($salt . hash('sha256', $salt . $password, true));
    }

    /**
     * Hashes with a caller-supplied salt. Only used to reproduce a known
     * cross-language fixture in the tests; real hashes get a random salt.
     */
    public static function hashWithSalt(string $password, string $salt): string
    {
        if (strlen($salt) !== 4) {
            throw new RuntimeException('the salt must be 4 bytes');
        }
        return base64_encode($salt . hash('sha256', $salt . $password, true));
    }

    public static function matches(string $password, string $encoded): bool
    {
        $raw = base64_decode($encoded, true);
        if ($raw === false || (strlen($raw) !== 36 && strlen($raw) !== 68)) {
            return false;
        }
        $salt = substr($raw, 0, 4);
        $digest = substr($raw, 4);
        $algo = strlen($digest) === 32 ? 'sha256' : 'sha512';
        return hash_equals($digest, hash($algo, $salt . $password, true));
    }

    /**
     * The password rules Bun enforces: non-empty, at most 1024 bytes, and at
     * least 8 characters, counted as code points rather than bytes so an
     * accented or astral character counts as one.
     *
     * @throws RuntimeException when the password is unacceptable
     */
    public static function check(string $password): void
    {
        if ($password === '') {
            throw new RuntimeException('the password cannot be empty');
        }
        if (strlen($password) > 1024) {
            throw new RuntimeException('the password cannot exceed 1024 bytes');
        }
        // Counted with PCRE rather than mb_strlen so the broker keeps needing
        // only ext-sockets beyond the bundled extensions.
        $points = preg_match_all('/./us', $password);
        if ($points === false || $points < 8) {
            throw new RuntimeException('the password needs at least 8 characters');
        }
    }
}
