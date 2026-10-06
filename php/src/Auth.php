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
}
