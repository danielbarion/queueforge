<?php
declare(strict_types=1);

/** Resolve configured DNS peers at startup, using the same address IDs as Rust/Bun. */
final class Discovery
{
    public static function resolve(array $config): array
    {
        if (($config['discovery'] ?? '') !== 'dns') return $config;
        $listen = (string)($config['cluster'] ?? '');
        $name = (string)($config['dns_name'] ?? '');
        if ($listen === '' || $name === '') throw new RuntimeException('DNS discovery requires cluster.listen and cluster.dns_name');
        $colon = strrpos($listen, ':');
        $port = (int)($config['dns_port'] ?? ($colon === false ? 0 : substr($listen, $colon + 1)));
        if ($port < 1 || $port > 65535) throw new RuntimeException('Invalid DNS discovery port');
        $addresses = [];
        if (function_exists('socket_addrinfo_lookup')) {
            foreach (@socket_addrinfo_lookup($name, (string)$port, ['ai_socktype'=>SOCK_STREAM]) ?: [] as $info) {
                $row = socket_addrinfo_explain($info)['ai_addr'];
                $addresses[] = $row['sin_addr'] ?? $row['sin6_addr'] ?? '';
            }
        } else {
            if (function_exists('gethostbynamelist')) $addresses = @gethostbynamelist($name) ?: [];
            foreach (@dns_get_record($name, DNS_A | DNS_AAAA) ?: [] as $row) {
                if (isset($row['ip'])) $addresses[] = $row['ip'];
                if (isset($row['ipv6'])) $addresses[] = $row['ipv6'];
            }
        }
        if ($addresses === []) throw new RuntimeException('DNS discovery resolved no peer addresses');
        $members = [];
        foreach (array_unique($addresses) as $address) {
            if (filter_var($address, FILTER_VALIDATE_IP) === false) continue;
            $endpoint = (str_contains($address, ':') ? '['.$address.']' : $address).':'.$port;
            $members[$endpoint] = ['id'=>$endpoint, 'addr'=>$endpoint];
        }
        $id = (string)($config['node_id'] ?? '');
        if ($id === '') $id = $listen;
        $members[$id] = ['id'=>$id, 'addr'=>$listen];
        ksort($members, SORT_STRING);
        $config['node_id'] = $id; $config['members'] = array_values($members);
        return $config;
    }
}
