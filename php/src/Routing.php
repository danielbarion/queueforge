<?php
declare(strict_types=1);

/** Topic and header binding match, same rules as the Bun broker. */
final class Routing
{
    public static function topic(string $pattern, string $key): bool
    {
        $pat = $pattern === '' ? [] : explode('.', $pattern);
        $words = $key === '' ? [] : explode('.', $key);
        return self::topicRec($pat, $words);
    }

    /** @param list<string> $pat @param list<string> $words */
    private static function topicRec(array $pat, array $words): bool
    {
        if ($pat === []) {
            return $words === [];
        }
        if ($pat[0] === '#') {
            if (count($pat) === 1) {
                return true;
            }
            $rest = array_slice($pat, 1);
            for ($i = 0; $i <= count($words); $i++) {
                if (self::topicRec($rest, array_slice($words, $i))) {
                    return true;
                }
            }
            return false;
        }
        if ($words === []) {
            return false;
        }
        if ($pat[0] === '*' || $pat[0] === $words[0]) {
            return self::topicRec(array_slice($pat, 1), array_slice($words, 1));
        }
        return false;
    }
}
