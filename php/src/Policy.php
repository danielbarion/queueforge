<?php
declare(strict_types=1);

/**
 * Policy matching and resolution. A policy supplies queue arguments the client
 * did not declare, so a declared value always wins. Mirrors Bun's
 * bun/src/broker/policy-data.ts and policy.ts.
 */
final class Policy
{
    /** Definition keys a policy may carry. Anything else is a 400. */
    public const KEYS = [
        'message-ttl',
        'expires',
        'max-length',
        'max-length-bytes',
        'overflow',
        'dead-letter-exchange',
        'dead-letter-routing-key',
        'dead-letter-strategy',
        'delivery-limit',
        'alternate-exchange',
        'max-priority',
        'queue-mode',
    ];

    /** Policy definition key to the queue argument it fills. */
    private const ARG_OF = [
        'message-ttl' => 'x-message-ttl',
        'expires' => 'x-expires',
        'max-length' => 'x-max-length',
        'max-length-bytes' => 'x-max-length-bytes',
        'overflow' => 'x-overflow',
        'dead-letter-exchange' => 'x-dead-letter-exchange',
        'dead-letter-routing-key' => 'x-dead-letter-routing-key',
        'dead-letter-strategy' => 'x-dead-letter-strategy',
        'delivery-limit' => 'x-delivery-limit',
        'max-priority' => 'x-max-priority',
    ];

    /**
     * Picks the policy that applies to a resource: highest priority wins,
     * then the lexicographically smaller name. An invalid pattern is skipped
     * rather than raised.
     *
     * @param array<string, array<string, mixed>> $table name to policy body
     * @return ?array<string, mixed>
     */
    public static function match(array $table, string $name, string $kind): ?array
    {
        $best = null;
        $bestName = '';
        foreach ($table as $policyName => $policy) {
            $applyTo = (string) ($policy['apply-to'] ?? 'all');
            if ($applyTo !== 'all' && $applyTo !== $kind) {
                continue;
            }
            $pattern = (string) ($policy['pattern'] ?? '');
            if (@preg_match('/' . str_replace('/', '\/', $pattern) . '/', $name) !== 1) {
                continue;
            }
            $priority = (int) ($policy['priority'] ?? 0);
            $bestPriority = $best === null ? PHP_INT_MIN : (int) ($best['priority'] ?? 0);
            if ($best === null
                || $priority > $bestPriority
                || ($priority === $bestPriority && strcmp((string) $policyName, $bestName) < 0)) {
                $best = $policy;
                $bestName = (string) $policyName;
            }
        }
        return $best;
    }

    /**
     * Fills arguments from a policy, never overriding a declared value. The
     * operator policy is applied after the user policy, so it wins between
     * the two but still loses to anything the client declared.
     *
     * @param array<string, string|int> $declared the client's arguments
     * @param ?array<string, mixed> $user
     * @param ?array<string, mixed> $operator
     * @return array<string, string|int>
     */
    public static function resolve(array $declared, ?array $user, ?array $operator): array
    {
        $out = $declared;
        foreach ([$user, $operator] as $policy) {
            if ($policy === null) {
                continue;
            }
            $definition = is_array($policy['definition'] ?? null) ? $policy['definition'] : [];
            foreach (self::ARG_OF as $key => $arg) {
                if (!array_key_exists($key, $definition)) {
                    continue;
                }
                // A value the client declared is never replaced.
                if (array_key_exists($arg, $declared) && $declared[$arg] !== '') {
                    continue;
                }
                $value = $definition[$key];
                if (is_bool($value)) {
                    $value = $value ? 1 : 0;
                }
                if (is_scalar($value)) {
                    $out[$arg] = $value;
                }
            }
        }
        return $out;
    }

    /**
     * The alternate exchange for an exchange, in Bun's precedence order: the
     * exchange row, then the operator policy, then the user policy
     * (bun/src/broker/topology.ts:234).
     *
     * @param array<string, array<string, mixed>> $userTable
     * @param array<string, array<string, mixed>> $operatorTable
     */
    public static function alternate(?string $onRow, array $userTable, array $operatorTable, string $exchange): ?string
    {
        if ($onRow !== null && $onRow !== '') {
            return $onRow;
        }
        foreach ([$operatorTable, $userTable] as $table) {
            $policy = self::match($table, $exchange, 'exchanges');
            $definition = is_array($policy['definition'] ?? null) ? $policy['definition'] : [];
            $alternate = $definition['alternate-exchange'] ?? null;
            if (is_string($alternate) && $alternate !== '') {
                return $alternate;
            }
        }
        return null;
    }

    /**
     * Validates a policy body from the management API. Returns an error
     * string, or null when the body is acceptable.
     *
     * @param array<string, mixed> $body
     */
    public static function validate(array $body): ?string
    {
        $applyTo = (string) ($body['apply-to'] ?? 'all');
        if (!in_array($applyTo, ['all', 'queues', 'exchanges'], true)) {
            return "apply-to must be one of all, queues or exchanges";
        }
        if (!isset($body['pattern']) || !is_string($body['pattern'])) {
            return 'a policy needs a pattern';
        }
        $definition = $body['definition'] ?? null;
        if (!is_array($definition)) {
            return 'a policy needs a definition';
        }
        $unknown = array_values(array_diff(array_keys($definition), self::KEYS));
        if ($unknown !== []) {
            return implode(', ', $unknown) . ' are not recognised policy settings';
        }
        return null;
    }
}
