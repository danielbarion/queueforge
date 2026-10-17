<?php
declare(strict_types=1);

/** RabbitMQ password hash: base64(salt || digest(salt || password)). New hashes are SHA-256. MD5 and SHA-512 still verify. */
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
        if ($raw === false || !in_array(strlen($raw), [20, 36, 68], true)) {
            return false;
        }
        $salt = substr($raw, 0, 4);
        $digest = substr($raw, 4);
        $algo = match (strlen($digest)) {
            16 => 'md5',
            32 => 'sha256',
            default => 'sha512',
        };
        return hash_equals($digest, hash($algo, $salt . $password, true));
    }

    /** RabbitMQ `hashing_algorithm` for a stored hash, or null when the length is not one RabbitMQ ships. */
    public static function algorithm(string $encoded): ?string
    {
        $raw = base64_decode($encoded, true);
        if ($raw === false) {
            return null;
        }
        return match (strlen($raw)) {
            20 => 'rabbit_password_hashing_md5',
            36 => 'rabbit_password_hashing_sha256',
            68 => 'rabbit_password_hashing_sha512',
            default => null,
        };
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
