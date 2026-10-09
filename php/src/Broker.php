<?php
declare(strict_types=1);

/**
 * Single-node and clustered queues. A classic confirm is not released until the
 * covering fsync. A quorum confirm also waits for a durable majority.
 */
final class Broker
{
    /** @var array<string, array{ready: list<int>, consumers: list<array{conn:int,ch:int,tag:string,credit:int}>}> */
    public array $queues = [];
    /** @var array<int, array{queue:string,body:string,mode:int}> */
    public array $msgs = [];
    /** @var list<array{conn:int,ch:int,tag:int,end:int}> */
    public array $waiting = [];
    /** @var array<string, string> */
    public array $users = [];
    /**
     * Tags per user. An untagged user loaded from the original file format is
     * treated as an administrator so an existing deployment keeps working.
     *
     * @var array<string, list<string>>
     */
    public array $tags = [];
    /**
     * Regex permissions per user and vhost.
     *
     * @var array<string, array<string, array{configure:string,write:string,read:string}>>
     */
    public array $permissions = [];
    /** @var array<string, array<string, array<string, mixed>>> vhost to name to body */
    public array $policies = [];
    /** @var array<string, array<string, array<string, mixed>>> */
    public array $operatorPolicies = [];
    /** @var array<string, array<string, array{write:string,read:string}>> user to exchange */
    public array $topicPermissions = [];
    /** @var array<string, array<string, int>> */
    public array $userLimits = [];
    /** @var array<string, array<string, int>> */
    public array $vhostLimits = [];
    /**
     * Shovel and federation-upstream parameters from the management API.
     *
     * @var array<string, array<string, array<string, array<string, mixed>>>>
     */
    public array $parameters = [];
    /** @var array<string, string> federation upstream name to uri */
    public array $fedUpstreams = [];
    /** @var list<string> */
    public array $vhosts = ['/'];
    /** @var array<int, string> Connection ID to authenticated username. */
    public array $userByConn = [];
    public array $currentUsers = [];
    /**
     * Feature flags. Named after Bun's set so the management UI sees the same
     * shape; this broker has no code paths keyed off them.
     *
     * @var array<string, bool>
     */
    public array $featureFlags = [
        // Named as Bun names them, so a client reading the flag list sees the
        // same identifiers from either broker.
        'quorum_queues' => true,
        'publisher_confirms' => true,
        'classic_queue_type' => true,
    ];
    /** @var array<string, string> */
    public array $exchanges = [
        '' => 'direct',
        'amq.direct' => 'direct',
        'amq.fanout' => 'fanout',
        'amq.topic' => 'topic',
        'amq.headers' => 'headers',
        'amq.match' => 'headers',
        'amq.rabbitmq.event' => 'topic',
    ];
    /** True while the broker itself publishes, which may use an internal exchange. */
    public bool $internalPublish = false;
    /**
     * Exchange rows beyond the kind: durability, auto-delete, internal, and
     * the alternate exchange.
     *
     * @var array<string, array{durable:bool,autoDelete:bool,internal:bool,alternate:?string}>
     */
    public array $exchangeRows = [
        'amq.rabbitmq.event' => ['durable' => true, 'autoDelete' => false, 'internal' => true, 'alternate' => null],
    ];
    /**
     * Lets a transient non-exclusive queue be declared. Off by default, which
     * is the RabbitMQ deprecation behaviour Bun also implements.
     */
    public bool $transientNonexcl = false;
    /** @var list<array{exchange:string,queue:string,key:string,args:list<array{0:string,1:string}>}> */
    public array $bindings = [];
    /**
     * Exchange-to-exchange links. In memory only, so they do not survive a
     * restart. Bun keeps them the same way (bun/src/broker/topology.ts:255).
     *
     * @var list<array{source:string,destination:string,key:string}>
     */
    public array $e2e = [];
    public int $nextId = 1;
    public int $cursor = 0;
    public string $nodeId = 'queueforge';
    /** @var list<array{id:string,addr:string}> */
    public array $members = [];
    public ?Cluster $cluster = null;
    public bool $ready = true;
    /** @var array<string, int> */
    public array $prom = [
        'connections' => 0,
        'connectionsOpened' => 0,
        'connectionsClosed' => 0,
        'channels' => 0,
        'channelsOpened' => 0,
        'channelsClosed' => 0,
        'queuesDeclared' => 0,
        'queuesCreated' => 0,
        'queuesDeleted' => 0,
        'consumers' => 0,
        'received' => 0,
        'receivedConfirm' => 0,
        'confirmed' => 0,
        'routed' => 0,
        'unroutableDropped' => 0,
        'unroutableReturned' => 0,
        'delivered' => 0,
        'deliveredConsumeManual' => 0,
        'deliveredConsumeAuto' => 0,
        'deliveredGetManual' => 0,
        'deliveredGetAuto' => 0,
        'getEmpty' => 0,
        'acknowledged' => 0,
        'redelivered' => 0,
        'dlxExpired' => 0,
        'dlxRejected' => 0,
        'dlxMaxlen' => 0,
        'dlxDeliveryLimit' => 0,
    ];

    public function __construct(public Store $store, public string $userFile)
    {
        if (is_file($userFile)) {
            $decoded = json_decode((string) file_get_contents($userFile), true);
            if (is_array($decoded)) {
                foreach ($decoded as $name => $row) {
                    if (!is_string($name)) {
                        continue;
                    }
                    // A bare string is the original format: a hash with no
                    // tags, which is treated as an administrator.
                    if (is_string($row)) {
                        $this->users[$name] = $row;
                        $this->tags[$name] = ['administrator'];
                        continue;
                    }
                    if (is_array($row) && is_string($row['hash'] ?? null)) {
                        $this->users[$name] = $row['hash'];
                        $tags = [];
                        foreach ((array) ($row['tags'] ?? []) as $tag) {
                            if (is_string($tag)) {
                                $tags[] = $tag;
                            }
                        }
                        $this->tags[$name] = $tags === [] ? ['administrator'] : $tags;
                        if (is_array($row['permissions'] ?? null)) {
                            $this->permissions[$name] = $row['permissions'];
                        }
                    }
                }
            }
        }
        foreach ($store->replay() as $msg) {
            $meta = is_array($msg['meta'] ?? null) ? $msg['meta'] : [];
            $headers = [];
            foreach ((array) ($meta['headers'] ?? []) as $pair) {
                if (is_array($pair) && array_key_exists(0, $pair) && array_key_exists(1, $pair)) {
                    $headers[] = [(string) $pair[0], $pair[1]];
                }
            }
            $this->msgs[$msg['id']] = [
                'queue' => $msg['queue'],
                'body' => $msg['body'],
                'mode' => $msg['mode'],
                'redelivered' => false,
                // Recovered from the metadata field, so priority ordering,
                // per-message TTL and the originating exchange survive.
                'exchange' => (string) ($meta['exchange'] ?? ''),
                'key' => (string) ($meta['key'] ?? $msg['queue']),
                'priority' => (int) ($meta['priority'] ?? 0),
                'expires' => isset($meta['expires']) && $meta['expires'] !== null ? (int) $meta['expires'] : null,
                'headers' => $headers,
                'deliveries' => (int) ($meta['deliveries'] ?? 0),
                'qid' => (string) ($meta['qid'] ?? ''),
                'propRaw' => $msg['propRaw'] ?? null,
            ];
            $this->declareRecovered($msg['queue']);
            $this->pushReady($msg['queue'], $msg['id']);
            if ($msg['id'] >= $this->nextId) {
                $this->nextId = $msg['id'] + 1;
            }
        }
    }

    /**
     * Creates a queue during replay. Recovery must not run the declare rules,
     * since a persisted queue already passed them and its declared arguments
     * are not in the log.
     */
    private function declareRecovered(string $name): void
    {
        if (isset($this->queues[$name])) {
            return;
        }
        $this->queues[$name] = [
            'ready' => [],
            'consumers' => [],
            'replicas' => [],
            'declaredArgs' => [],
            'args' => Features::parseArgs([]),
            'durable' => true,
            'exclusive' => false,
            'autoDelete' => false,
            'lastUsed' => microtime(true),
            'nextExpiry' => PHP_INT_MAX,
            'home' => '',
        ];
    }

    public function bootstrap(string $password): void
    {
        if ($this->users !== []) {
            return;
        }
        $this->users['admin'] = Auth::hash($password);
        $this->tags['admin'] = ['administrator'];
        $this->permissions['admin'] = ['/' => ['configure' => '.*', 'write' => '.*', 'read' => '.*']];
        $this->saveUsers();
    }

    /** Writes users, tags and permissions back to the user file. */
    public function saveUsers(): void
    {
        $out = [];
        foreach ($this->users as $name => $hash) {
            $out[$name] = [
                'hash' => $hash,
                'tags' => $this->tags[$name] ?? ['administrator'],
                'permissions' => $this->permissions[$name] ?? [],
            ];
        }
        @file_put_contents($this->userFile, json_encode($out));
    }

    /**
     * Creates or replaces a user. A password hash can be supplied directly,
     * which is how the management API accepts an already-hashed password.
     *
     * @param list<string> $tags
     */
    public function putUser(string $name, string $password, array $tags, string $hash = ''): void
    {
        if ($name === '') {
            throw new RuntimeException('a user needs a name');
        }
        if ($hash === '') {
            Auth::check($password);
            $hash = Auth::hash($password);
        }
        $this->users[$name] = $hash;
        $this->tags[$name] = $tags === [] ? ['management'] : $tags;
        $this->saveUsers();
    }

    public function deleteUser(string $name): bool
    {
        if (!isset($this->users[$name])) {
            return false;
        }
        unset($this->users[$name], $this->tags[$name], $this->permissions[$name], $this->topicPermissions[$name]);
        $this->saveUsers();
        return true;
    }

    /** True when the user carries a tag that may reach the management API. */
    public function canManage(string $name): bool
    {
        $tags = $this->tags[$name] ?? [];
        foreach (['administrator', 'management', 'monitoring'] as $tag) {
            if (in_array($tag, $tags, true)) {
                return true;
            }
        }
        return false;
    }

    public function isAdmin(string $name): bool
    {
        return in_array('administrator', $this->tags[$name] ?? [], true);
    }

    public function setPermissions(string $user, string $vhost, string $configure, string $write, string $read): void
    {
        $this->permissions[$user][$vhost] = [
            'configure' => $configure,
            'write' => $write,
            'read' => $read,
        ];
        $this->saveUsers();
    }

    public function clearPermissions(string $user, string $vhost): bool
    {
        if (!isset($this->permissions[$user][$vhost])) {
            return false;
        }
        unset($this->permissions[$user][$vhost]);
        $this->saveUsers();
        return true;
    }

    public function verify(string $user, string $pass): bool
    {
        return isset($this->users[$user]) && Auth::matches($pass, $this->users[$user]);
    }

    /**
     * Declares a queue. A quorum queue must be durable and non-exclusive, as
     * in Bun and RabbitMQ; asking for one any other way is a channel error.
     *
     * @param array<string, string|int> $rawArgs
     * @throws RuntimeException when a quorum queue is asked for transiently
     */
    /**
     * Declares a queue.
     *
     * A quorum queue must be durable and non-exclusive. A transient
     * non-exclusive classic queue is refused with 541, as RabbitMQ and Bun
     * do, unless transientNonexcl is set. Policies fill arguments the client
     * did not declare.
     *
     * @param array<string, string|int> $rawArgs
     * @return array{messages:int,consumers:int}
     * @throws RuntimeException with the reply code as the exception code
     */
    public function declareQueue(string $name, array $rawArgs = [], bool $durable = true, bool $exclusive = false, bool $passive = false, bool $autoDelete = false): array
    {
        $type = (string) ($rawArgs['x-queue-type'] ?? '');
        if ($passive) {
            if (!isset($this->queues[$name])) {
                throw new RuntimeException("NOT_FOUND - queue '$name'", 404);
            }
            // A passive declare reports state and must not touch arguments.
            return ['messages' => $this->readyCount($name), 'consumers' => $this->consumerCount($name)];
        }
        // An existing queue is settled before the declare rules run, so a
        // redeclare that disagrees about durability reports exactly that
        // rather than the transient-queue deprecation.
        if (isset($this->queues[$name])) {
            if (($this->queues[$name]['durable'] ?? true) !== $durable) {
                throw new RuntimeException("PRECONDITION_FAILED - inequivalent arg durable for queue '$name'", 406);
            }
            if (!isset($this->queues[$name]['replicas'])) {
                $this->queues[$name]['replicas'] = [];
            }
            // A redeclare reports state and leaves the stored arguments as
            // they are, so a later declare cannot silently retune the queue.
            return ['messages' => $this->readyCount($name), 'consumers' => $this->consumerCount($name)];
        }
        if (!$durable && !$exclusive && $type !== 'quorum' && !$this->transientNonexcl) {
            throw new RuntimeException(
                'INTERNAL_ERROR - Feature `transient_nonexcl_queues` is deprecated. '
                . 'By default, this feature is not permitted anymore.',
                541,
            );
        }
        if (!Features::knownQueueType($type)) {
            throw new RuntimeException("PRECONDITION_FAILED - unsupported x-queue-type '$type'", 406);
        }
        if ($type === 'quorum' && (!$durable || $exclusive)) {
            throw new RuntimeException('PRECONDITION_FAILED - quorum queue must be durable and non-exclusive', 406);
        }
        $this->queues[$name] = [
            'ready' => [],
            'consumers' => [],
            'replicas' => [],
            'declaredArgs' => $rawArgs,
            'args' => Features::parseArgs($this->withPolicy($rawArgs, $name)),
            'durable' => $durable,
            'exclusive' => $exclusive,
            'autoDelete' => $autoDelete,
            'lastUsed' => microtime(true),
            // The earliest TTL deadline among ready messages. PHP_INT_MAX
            // means nothing in this queue can expire.
            'nextExpiry' => PHP_INT_MAX,
            // A quorum queue is homed where it was declared rather than
            // by the classic hash, matching Bun.
            'home' => $type === 'quorum' ? $this->nodeId : '',
        ];
        $this->prom['queuesDeclared']++;
        $this->prom['queuesCreated']++;
        return ['messages' => 0, 'consumers' => 0];
    }

    /**
     * Applies the matching user and operator policies to declared arguments.
     *
     * @param array<string, string|int> $rawArgs
     * @return array<string, string|int>
     */
    private function withPolicy(array $rawArgs, string $name): array
    {
        return Policy::resolve(
            $rawArgs,
            Policy::match($this->policies['/'] ?? [], $name, 'queues'),
            Policy::match($this->operatorPolicies['/'] ?? [], $name, 'queues'),
        );
    }

    /**
     * Recomputes every queue's arguments from what was declared plus the
     * current policies. Called after a policy changes, so a policy edit
     * reaches queues that already exist.
     */
    public function applyPolicies(): void
    {
        foreach ($this->queues as $name => $queue) {
            $declared = is_array($queue['declaredArgs'] ?? null) ? $queue['declaredArgs'] : [];
            $this->queues[$name]['args'] = Features::parseArgs($this->withPolicy($declared, $name));
        }
    }

    /**
     * Declares an exchange. The amq.* namespace and the default exchange are
     * reserved, as they are in Bun (bun/src/broker/topology.ts:33).
     *
     * @throws RuntimeException with the reply code as the exception code
     */
    public function declareExchange(string $name, string $kind, bool $durable = true, bool $autoDelete = false, bool $internal = false, ?string $alternate = null, bool $passive = false): void
    {
        if ($name === '') {
            return;
        }
        if ($passive) {
            if (!isset($this->exchanges[$name])) {
                throw new RuntimeException("NOT_FOUND - exchange '$name'", 404);
            }
            return;
        }
        if (str_starts_with($name, 'amq.')) {
            throw new RuntimeException("ACCESS_REFUSED - exchange name '$name' is reserved", 403);
        }
        $this->exchanges[$name] = $kind === '' ? 'direct' : $kind;
        $this->exchangeRows[$name] = [
            'durable' => $durable,
            'autoDelete' => $autoDelete,
            'internal' => $internal,
            'alternate' => $alternate,
        ];
    }

    /**
     * Binds a queue to an exchange. Both have to exist, so a typo surfaces as
     * a 404 instead of creating a queue nobody asked for.
     *
     * @param list<array{0:string,1:mixed}> $args
     * @throws RuntimeException with the reply code as the exception code
     */
    public function bind(string $queue, string $exchange, string $key, array $args = [], ?string $user = null, string $vhost = '/'): void
    {
        if (!isset($this->queues[$queue])) {
            throw new RuntimeException("NOT_FOUND - no queue '$queue'", 404);
        }
        if ($exchange !== '' && !isset($this->exchanges[$exchange])) {
            throw new RuntimeException("NOT_FOUND - no exchange '$exchange'", 404);
        }
        
        // Protocol callers supply their authenticated identity; internal
        // topology restoration has no user. Only topic exchanges use keys
        // as an additional authorization boundary.
        if ($user !== null && ($this->exchanges[$exchange] ?? null) === 'topic' && !$this->topicReadAllowed($user, $vhost, $exchange, $key)) {
            throw new RuntimeException(
                "ACCESS_REFUSED - cannot bind queue without topic read permission for '$exchange' key '$key'",
                403
            );
        }
        
        foreach ($this->bindings as $row) {
            if ($row['queue'] === $queue && $row['exchange'] === $exchange && $row['key'] === $key && $row['args'] === $args) {
                return;
            }
        }
        $this->bindings[] = ['exchange' => $exchange, 'queue' => $queue, 'key' => $key, 'args' => $args];
    }

    /**
     * Publish one event to amq.rabbitmq.event, as RabbitMQ's event exchange
     * plugin does. A queue nobody bound costs one routing lookup. Errors are
     * dropped: an event never fails what caused it.
     *
     * @param list<array{0:string,1:mixed}> $headers
     */
    public function emitEvent(string $key, array $headers): void
    {
        $this->internalPublish = true;
        try {
            if ($this->route('amq.rabbitmq.event', $key, []) === []) {
                return;
            }
            $headers[] = ['vhost', '/'];
            $headers[] = ['timestamp_in_ms', (int) (microtime(true) * 1000)];
            $this->publish(0, 0, 0, 'amq.rabbitmq.event', $key, '', 1, 0, $headers);
        } catch (RuntimeException) {
            // An event is best effort.
        } finally {
            $this->internalPublish = false;
        }
    }

    /** @param list<array{0:string,1:string}> $headers
     *  @return list<string> */
    public function route(string $exchange, string $key, array $headers = []): array
    {
        return $this->routeFrom($exchange, $key, $headers, []);
    }

    /**
     * Routes through an exchange, following exchange-to-exchange links and
     * the alternate exchange. The seen set means a link or alternate cycle is
     * visited once instead of looping.
     *
     * @param list<array{0:string,1:mixed}> $headers
     * @param array<string, bool> $seen
     * @return list<string>
     * @throws RuntimeException when the exchange is internal
     */
    private function routeFrom(string $exchange, string $key, array $headers, array $seen): array
    {
        if (isset($seen[$exchange])) {
            return [];
        }
        // Only the exchange a client published to is checked; a hop into an
        // internal exchange is how it is meant to be used.
        if ($seen === [] && !$this->internalPublish && ($this->exchangeRows[$exchange]['internal'] ?? false) === true) {
            throw new RuntimeException("ACCESS_REFUSED - internal exchange '$exchange'", 403);
        }
        $seen[$exchange] = true;
        $out = $this->routeDirect($exchange, $key, $headers);
        foreach ($this->e2e as $link) {
            if ($link['source'] !== $exchange) {
                continue;
            }
            $kind = $this->exchanges[$exchange] ?? 'direct';
            $matches = match ($kind) {
                'fanout' => true,
                'topic' => Routing::topic($link['key'], $key),
                default => $link['key'] === $key,
            };
            if ($matches) {
                $out = array_merge($out, $this->routeFrom($link['destination'], $key, $headers, $seen));
            }
        }
        // Nothing matched here, so try the alternate exchange.
        if ($out === [] && $exchange !== '') {
            $alternate = Policy::alternate(
                $this->exchangeRows[$exchange]['alternate'] ?? null,
                $this->policies['/'] ?? [],
                $this->operatorPolicies['/'] ?? [],
                $exchange,
            );
            if ($alternate !== null && isset($this->exchanges[$alternate])) {
                $out = $this->routeFrom($alternate, $key, $headers, $seen);
            }
        }
        return array_values(array_unique($out));
    }

    /** @param list<array{0:string,1:string}> $headers
     *  @return list<string> */
    private function routeDirect(string $exchange, string $key, array $headers = []): array
    {
        if ($exchange === '') {
            return isset($this->queues[$key]) ? [$key] : [];
        }
        if (!isset($this->exchanges[$exchange])) {
            return [];
        }
        $rows = array_values(array_filter(
            $this->bindings,
            static fn (array $row): bool => $row['exchange'] === $exchange,
        ));
        $kind = $this->exchanges[$exchange];
        if ($kind === 'fanout') {
            return array_values(array_unique(array_column($rows, 'queue')));
        }
        if ($kind === 'topic') {
            $out = [];
            foreach ($rows as $row) {
                if (Routing::topic($row['key'], $key)) {
                    $out[] = $row['queue'];
                }
            }
            return array_values(array_unique($out));
        }
        if ($kind === 'headers') {
            $out = [];
            foreach ($rows as $row) {
                if (Features::headersMatch($row['args'] ?? [], $headers)) {
                    $out[] = $row['queue'];
                }
            }
            return array_values(array_unique($out));
        }
        $out = [];
        foreach ($rows as $row) {
            if ($row['key'] === $key) {
                $out[] = $row['queue'];
            }
        }
        return array_values(array_unique($out));
    }

    /**
     * Publishes one message.
     *
     * CC and BCC headers add destinations. BCC is stripped from the stored
     * headers so consumers never see it, as Bun does
     * (bun/src/broker/publish.ts:127-136).
     *
     * @param list<array{0:string,1:mixed}> $headers
     * @param ?string $propRaw raw publisher property bytes, replayed to consumers verbatim
     * @return 'wait'|'return'|'nack'
     * @throws RuntimeException when the default exchange has no such queue
     */
    public function publish(int $conn, int $ch, int $tag, string $exchange, string $key, string $body, int $mode, int $priority = 0, array $headers = [], ?int $expirationMs = null, ?string $propRaw = null): string
    {
        $this->prom['received']++;
        if ($tag > 0) {
            $this->prom['receivedConfirm']++;
        }
        // The default exchange addresses a queue by name, so a missing one is
        // a channel error rather than an unroutable publish.
        if ($exchange === '' && !isset($this->queues[$key])) {
            throw new RuntimeException("NOT_FOUND - no queue '$key'", 404);
        }
        
        // Enforce topic write permission for non-empty exchanges.
        if ($exchange !== '') {
            $user = $this->currentUsers[$conn] ?? 'guest';
            if (!$this->topicWriteAllowed($user, '/', $exchange, $key)) {
                throw new RuntimeException(
                    "ACCESS_REFUSED - topic permission denied for '$exchange' key '$key'",
                    403
                );
            }
        }
        
        $dests = $this->route($exchange, $key, $headers);
        foreach (['CC', 'BCC'] as $name) {
            foreach (self::headerList($headers, $name) as $extra) {
                if ($extra === $key) {
                    continue;
                }
                foreach ($this->route($exchange, $extra, $headers) as $queue) {
                    if (!in_array($queue, $dests, true)) {
                        $dests[] = $queue;
                    }
                }
            }
        }
        $hadBcc = self::headerList($headers, 'BCC') !== [];
        if ($hadBcc) {
            $headers = array_values(array_filter(
                $headers,
                static fn (array $pair): bool => $pair[0] !== 'BCC',
            ));
            // The property block carried BCC, so it can no longer be replayed
            // verbatim without leaking it.
            $propRaw = null;
        }
        if ($dests === []) {
            return 'return';
        }
        $this->prom['routed']++;
        $ids = [];
        $qids = [];
        $end = 0;
        $rejected = 0;
        $forwarded = 0;
        foreach ($dests as $queue) {
            if (!isset($this->queues[$queue])) {
                continue;
            }
            // A classic queue lives on one node. A publish that arrives
            // anywhere else is forwarded to its home, otherwise the message
            // sits here and a consumer attached at the home never sees it.
            $home = $this->remoteHomeOf($queue);
            if ($home !== null && $this->cluster !== null) {
                $this->cluster->request($home, 'enqueue', [
                    'vhost' => '/',
                    'queue' => $queue,
                    'exchange' => $exchange,
                    'routing_key' => $key,
                    'body_b64' => base64_encode($body),
                    'persistent' => $mode === 2,
                    'durable' => $mode === 2,
                ]);
                $forwarded++;
                continue;
            }
            $args = $this->queues[$queue]['args'];
            $this->expire($queue);
            $depth = $this->depth($queue);
            $over = ($args['maxLength'] !== null && $depth >= $args['maxLength'])
                || ($args['maxLengthBytes'] !== null && $this->bytes($queue) + strlen($body) > $args['maxLengthBytes']);
            if ($over && ($args['overflow'] === 'reject-publish' || $args['overflow'] === 'reject-publish-dlx')) {
                if ($args['overflow'] === 'reject-publish-dlx') {
                    $this->deadLetterBody($queue, $exchange, $key, $body, $headers);
                    $this->prom['dlxMaxlen']++;
                }
                $rejected++;
                continue;
            }
            if ($over) {
                while ($this->queues[$queue]['ready'] !== [] && (
                    ($args['maxLength'] !== null && $this->depth($queue) >= $args['maxLength'])
                    || ($args['maxLengthBytes'] !== null && $this->bytes($queue) + strlen($body) > $args['maxLengthBytes'])
                )) {
                    $dropped = array_shift($this->queues[$queue]['ready']);
                    if (is_int($dropped)) {
                        $this->prom['dlxMaxlen']++;
                        $this->deadLetter($dropped, 'maxlen');
                    }
                }
            }
            $id = $this->nextId++;
            $expires = null;
            $now = (int) (microtime(true) * 1000);
            if ($args['messageTtl'] !== null) {
                $expires = $now + $args['messageTtl'];
            }
            if ($expirationMs !== null) {
                $at = $now + $expirationMs;
                $expires = $expires === null ? $at : min($expires, $at);
            }
            // A quorum message carries the id its replicas are keyed by, in
            // the q-<node>-<millis>-<random> shape Bun uses, so a later
            // quorum_drop can name it. Classic messages keep the local id.
            $isQuorum = ($args['queueType'] ?? 'classic') === 'quorum';
            $qid = $isQuorum
                ? 'q-' . $this->nodeId . '-' . $now . '-' . bin2hex(random_bytes(4))
                : (string) $id;
            $this->msgs[$id] = [
                'queue' => $queue,
                'body' => $body,
                'mode' => $mode,
                'redelivered' => false,
                'exchange' => $exchange,
                'key' => $key,
                'priority' => $priority,
                'expires' => $expires,
                'headers' => $headers,
                'propRaw' => $propRaw,
                'qid' => $qid,
            ];
            $ids[] = $id;
            $qids[] = $qid;
            if ($mode === 2) {
                $end = $this->store->appendPublish($id, $queue, $body, $mode, $propRaw, self::meta($this->msgs[$id]));
            } else {
                $this->hold($queue, $id, true);
            }
        }
        // Any rejected destination nacks the publish, as Bun does
        // (bun/src/broker/publish.ts:166). Acking because one of three
        // queues accepted would tell the publisher the message is safe.
        if ($rejected > 0 || ($ids === [] && $forwarded === 0)) {
            return 'nack';
        }
        $need = 0;
        $copies = ['durable'];
        if ($this->quorumPublish($dests)) {
            $need = Features::majority(max(1, count($this->members)));
            if ($this->cluster !== null) {
                foreach ($qids as $i => $qid) {
                    $queue = $this->msgs[$ids[$i]]['queue'];
                    $this->cluster->replicate(Features::encodeQuorumAppend('/', $queue, $qid, $body, $exchange, $key, $mode === 2));
                }
            }
        }        $this->waiting[] = [
            'conn' => $conn,
            'ch' => $ch,
            'tag' => $tag,
            'end' => $end,
            'ids' => $ids,
            'qids' => $qids,
            'quorumNeed' => $need,
            'quorumHave' => 1,
            // One entry per durable copy. The local append is already fsynced
            // by the time the confirm is considered, so it counts as durable.
            'copies' => $copies,
        ];
        return 'wait';
    }

    /**
     * The message fields the body alone does not carry. Persisted alongside
     * the record so a restart keeps priority ordering, per-message TTL, the
     * originating exchange and routing key, and the headers a dead-letter
     * needs. Without this a recovered priority queue loses its ordering and
     * every message reports an empty exchange.
     *
     * @param array<string, mixed> $msg
     * @return array<string, mixed>
     */
    private static function meta(array $msg): array
    {
        $exchange = (string) ($msg['exchange'] ?? '');
        $key = (string) ($msg['key'] ?? '');
        $priority = (int) ($msg['priority'] ?? 0);
        $headers = $msg['headers'] ?? [];
        $qid = (string) ($msg['qid'] ?? '');
        $deliveries = (int) ($msg['deliveries'] ?? 0);
        // Nothing worth persisting: the replay defaults already reconstruct
        // an empty exchange, the queue name as the key, priority zero and no
        // expiry. Encoding that would cost a json_encode on every durable
        // publish for no gain.
        if ($exchange === '' && $key === (string) ($msg['queue'] ?? '')
            && $priority === 0 && ($msg['expires'] ?? null) === null
            && $headers === [] && $qid === '' && $deliveries === 0) {
            return [];
        }
        return [
            'exchange' => $exchange,
            'key' => $key,
            'priority' => $priority,
            'expires' => $msg['expires'] ?? null,
            'headers' => $headers,
            'qid' => $qid,
            'deliveries' => $deliveries,
        ];
    }

    /**
     * Reads a CC or BCC header as a list of routing keys. A single string is
     * treated as a one-element list, matching Bun's headerList
     * (bun/src/broker/routing.ts:68-74).
     *
     * @param list<array{0:string,1:mixed}> $headers
     * @return list<string>
     */
    private static function headerList(array $headers, string $name): array
    {
        $out = [];
        foreach ($headers as $pair) {
            if ($pair[0] !== $name) {
                continue;
            }
            if (is_array($pair[1])) {
                foreach ($pair[1] as $item) {
                    if (is_scalar($item)) {
                        $out[] = (string) $item;
                    }
                }
                continue;
            }
            if (is_scalar($pair[1])) {
                $out[] = (string) $pair[1];
            }
        }
        return $out;
    }

    /**
     * Whether a user has any permission entry for a vhost. This matches the
     * check Bun performs on connection.open: it tests only that a row
     * exists, not the configure/write/read patterns on it.
     */
    public function hasVhostAccess(string $user, string $vhost): bool
    {
        if ($this->isAdmin($user)) {
            return true;
        }
        return isset($this->permissions[$user][$vhost]);
    }

    /** The max-connections limit, from the user limit then the vhost limit. */
    public function connectionAllowed(string $user, string $vhost, int $open): bool
    {
        foreach ([$this->userLimits[$user] ?? [], $this->vhostLimits[$vhost] ?? []] as $limits) {
            $max = $limits['max-connections'] ?? null;
            if (is_int($max) && $max >= 0 && $open > $max) {
                return false;
            }
        }
        return true;
    }

    /** The max-channels limit for a user. */
    public function channelAllowed(string $user, int $open): bool
    {
        $max = $this->userLimits[$user]['max-channels'] ?? null;
        return !is_int($max) || $max < 0 || $open < $max;
    }

    /** The max-queues limit for a vhost. */
    public function queueAllowed(string $vhost): bool
    {
        $max = $this->vhostLimits[$vhost]['max-queues'] ?? null;
        return !is_int($max) || $max < 0 || count($this->queues) < $max;
    }

    /**
     * Whether a user may publish a routing key through an exchange. A user
     * with no topic-permission row for the exchange is allowed, which is how
     * RabbitMQ and Bun treat the absence of a rule.
     */
    public function topicWriteAllowed(string $user, string $vhost, string $exchange, string $key): bool
    {
        $row = $this->topicPermissions[$user][$exchange] ?? null;
        if ($row === null) {
            return true;
        }
        $pattern = (string) ($row['write'] ?? '');
        if ($pattern === '') {
            return false;
        }
        return @preg_match('/' . str_replace('/', '\/', $pattern) . '/', $key) === 1;
    }

    /** Binding a topic routing key requires its read permission. */
    public function topicReadAllowed(string $user, string $vhost, string $exchange, string $key): bool
    {
        $row = $this->topicPermissions[$user][$exchange] ?? null;
        if ($row === null) {
            return true;
        }
        $pattern = (string) ($row['read'] ?? '');
        if ($pattern === '') {
            return false;
        }
        return @preg_match('~' . str_replace('~', '\\~', $pattern) . '~', $key) === 1;
    }

    /**
     * Quorum message ids this node has already handed to a consumer.
     *
     * A peer that restarts replays its log, which would hand the same quorum
     * body out twice. The set travels in the hello payload so a recovering
     * node can drop what has already been consumed.
     *
     * @var array<string, true> keyed by "<queue>\0<qid>"
     */
    public array $consumed = [];

    /** Records a quorum id as consumed. */
    public function noteConsumed(string $queue, string $qid): void
    {
        if ($qid === '') {
            return;
        }
        // Classic ids are local and already acked out of the log. Keeping one
        // entry per delivery grows without bound and exhausts the process
        // around a million messages. Quorum ids are the ones a peer must not
        // hand out again after replaying its log.
        $kind = $this->queues[$queue]['args']['queueType'] ?? null;
        if ($kind !== null && $kind !== 'quorum') {
            return;
        }
        $this->consumed[$queue . "\0" . $qid] = true;
    }

    /** Whether a quorum id has already been consumed somewhere. */
    public function wasConsumed(string $queue, string $qid): bool
    {
        return $qid !== '' && isset($this->consumed[$queue . "\0" . $qid]);
    }

    /**
     * The consumed set in wire form: a list of [queue, qid] pairs.
     *
     * @return list<array{0:string,1:string}>
     */
    public function consumedList(): array
    {
        $out = [];
        foreach (array_keys($this->consumed) as $key) {
            $parts = explode("\0", (string) $key, 2);
            if (count($parts) === 2) {
                $out[] = [$parts[0], $parts[1]];
            }
        }
        return $out;
    }

    /**
     * Applies a peer's consumed set, dropping any local replica or ready
     * message it names.
     *
     * @param list<array{0:string,1:string}>|list<array<int, string>> $list
     */
    public function applyConsumed(array $list): void
    {
        foreach ($list as $pair) {
            if (!is_array($pair) || !isset($pair[0], $pair[1])) {
                continue;
            }
            $queue = (string) $pair[0];
            $qid = (string) $pair[1];
            $this->noteConsumed($queue, $qid);
            $this->dropByQid($queue, $qid);
        }
    }

    /** Removes a quorum message named by its cluster id. */
    public function dropByQid(string $queue, string $qid): void
    {
        if (!isset($this->queues[$queue]) || $qid === '') {
            return;
        }
        unset($this->queues[$queue]['replicas'][$qid]);
        foreach ($this->queues[$queue]['ready'] as $i => $id) {
            if (is_int($id) && ($this->msgs[$id]['qid'] ?? '') === $qid) {
                unset($this->queues[$queue]['ready'][$i]);
                $this->drop($id);
            }
        }
        $this->queues[$queue]['ready'] = array_values($this->queues[$queue]['ready']);
    }

    /**
     * Allocates a session id unique across the cluster: this node's slot in
     * the high 32 bits and a local counter in the low 32. Two nodes handing
     * out plain counters would otherwise issue the same id.
     */
    public function nextSession(): int
    {
        $n = $this->sessionsNext++;
        return $this->slot() * 0x100000000 + ($n & 0xffffffff);
    }

    private int $sessionsNext = 1;

    /** This node's 1-based index in the sorted member list. */
    private function slot(): int
    {
        if ($this->members === [] || $this->nodeId === '') {
            return 0;
        }
        $ids = array_map(static fn (array $m): string => (string) $m['id'], $this->members);
        sort($ids);
        $at = array_search($this->nodeId, $ids, true);
        return $at === false ? 1 : $at + 1;
    }

    /**
     * The peer that owns a classic queue, or null when this node does. A
     * quorum queue is served everywhere, so it never forwards.
     */
    public function remoteHomeOf(string $queue): ?string
    {
        if ($this->cluster === null || ($this->queues[$queue]['args']['queueType'] ?? 'classic') === 'quorum') {
            return null;
        }
        if (getenv('QUEUEFORGE_LOCAL') === '1') {
            return null;
        }
        $home = $this->home($queue);
        if ($home === '' || $home === $this->nodeId) {
            return null;
        }
        // Only a connected peer can take it; otherwise it is kept here
        // rather than dropped.
        return in_array($home, $this->cluster->peerIds(), true) ? $home : null;
    }

    /** @param list<string> $dests */
    private function quorumPublish(array $dests): bool
    {
        if (count($this->members) < 2) {
            return false;
        }
        foreach ($dests as $queue) {
            if (($this->queues[$queue]['args']['queueType'] ?? 'classic') === 'quorum') {
                return true;
            }
        }
        return false;
    }

    public function depth(string $queue): int
    {
        $n = count($this->queues[$queue]['ready']) + count($this->queues[$queue]['replicas']);
        return $n;
    }

    private function bytes(string $queue): int
    {
        $n = 0;
        foreach (array_merge($this->queues[$queue]['ready'], $this->queues[$queue]['replicas']) as $id) {
            $n += isset($this->msgs[$id]) ? strlen($this->msgs[$id]['body']) : 0;
        }
        return $n;
    }

    /**
     * Places a message. A transient quorum body is gated until its durable
     * majority lands; a persistent one reaches here only after flush() has
     * already confirmed the majority, so it is released immediately.
     */
    private function hold(string $queue, int $id, bool $gate = false): void
    {
        $type = $this->queues[$queue]['args']['queueType'] ?? 'classic';
        if ($type === 'quorum' && !$this->isLeader()) {
            $this->queues[$queue]['replicas'][] = $id;
            return;
        }
        if ($type === 'quorum' && $gate) {
            // A quorum body is gated until the durable majority lands, so a
            // consumer cannot be handed a message that a later rollback
            // would take back.
            $this->gated[$id] = true;
        }
        $this->pushReady($queue, $id);
    }

    /**
     * Quorum message ids not yet released by the confirm gate.
     *
     * @var array<int, true>
     */
    private array $gated = [];

    /** Whether a message is still waiting on its quorum majority. */
    public function isGated(int $id): bool
    {
        return isset($this->gated[$id]);
    }

    /** Releases the gate for a set of message ids. */
    public function release(array $ids): void
    {
        foreach ($ids as $id) {
            unset($this->gated[(int) $id]);
        }
    }

    private function pushReady(string $queue, int $id): void
    {
        if (!isset($this->msgs[$id])) {
            return;
        }
        // x-max-length 0 with drop-head keeps nothing: the new message is the
        // head and is dropped at once, dead-lettered as maxlen, as RabbitMQ does.
        if (($this->queues[$queue]['args']['maxLength'] ?? null) === 0
            && ($this->queues[$queue]['args']['overflow'] ?? 'drop-head') === 'drop-head') {
            $this->prom['dlxMaxlen']++;
            $this->deadLetter($id, 'maxlen');
            return;
        }
        $this->noteExpiry($queue, $id);
        $cap = $this->queues[$queue]['args']['maxPriority'] ?? null;
        $pri = $this->msgs[$id]['priority'] ?? 0;
        if ($cap !== null) {
            $pri = min($pri, $cap);
            $this->msgs[$id]['priority'] = $pri;
            $at = 0;
            $ready = $this->queues[$queue]['ready'];
            while ($at < count($ready) && ($this->msgs[$ready[$at]]['priority'] ?? 0) >= $pri) {
                $at++;
            }
            array_splice($this->queues[$queue]['ready'], $at, 0, [$id]);
            return;
        }
        $this->queues[$queue]['ready'][] = $id;
    }

    /**
     * Records a message's expiry as the queue's next deadline, so expire()
     * knows whether it has anything to do without walking the list.
     */
    private function noteExpiry(string $queue, int $id): void
    {
        $at = $this->msgs[$id]['expires'] ?? null;
        if (!is_int($at)) {
            return;
        }
        $this->queues[$queue]['nextExpiry'] = min($this->queues[$queue]['nextExpiry'] ?? PHP_INT_MAX, $at);
    }

    public function isLeader(): bool
    {
        $ids = [$this->nodeId];
        if ($this->cluster !== null) {
            foreach ($this->cluster->peerIds() as $id) {
                $ids[] = $id;
            }
        }
        if ($this->members === []) {
            return true;
        }
        return Features::leader($ids) === $this->nodeId;
    }

    public function refreshRole(): void
    {
        foreach ($this->queues as $name => $queue) {
            if (($queue['args']['queueType'] ?? 'classic') !== 'quorum') {
                continue;
            }
            if ($this->isLeader()) {
                foreach ($queue['replicas'] as $id) {
                    $this->pushReady($name, $id);
                }
                $this->queues[$name]['replicas'] = [];
            }
        }
    }

    public function noteCopy(string $messageId): void
    {
        foreach ($this->waiting as $i => $w) {
            if (in_array($messageId, $w['qids'] ?? [], true)) {
                $this->waiting[$i]['quorumHave']++;
                // A peer only replies ok once its own append is fsynced, so
                // an ok reply counts as a durable copy.
                $this->waiting[$i]['copies'][] = 'durable';
            }
        }
    }

    /**
     * Abandons a quorum publish whose replication did not reach a majority.
     * The replicas that did land are dropped on their peers and the local
     * copy goes too, so a nacked publish leaves nothing behind.
     */
    public function failQuorum(string $messageId): void
    {
        foreach ($this->waiting as $i => $w) {
            if (!in_array($messageId, $w['qids'] ?? [], true)) {
                continue;
            }
            if (($w['quorumNeed'] ?? 0) === 0) {
                return;
            }
            if ($this->cluster !== null) {
                foreach ($w['ids'] as $slot => $id) {
                    $queue = $this->msgs[$id]['queue'] ?? '';
                    $qid = $w['qids'][$slot] ?? '';
                    foreach ($this->cluster->peerIds() as $peer) {
                        $this->cluster->request($peer, 'quorum_drop', [
                            'vhost' => '/',
                            'queue' => $queue,
                            'id' => $qid,
                        ]);
                    }
                }
            }
            foreach ($w['ids'] as $id) {
                $queue = $this->msgs[$id]['queue'] ?? '';
                if ($queue !== '' && isset($this->queues[$queue])) {
                    foreach (['ready', 'replicas'] as $list) {
                        $this->queues[$queue][$list] = array_values(array_filter(
                            $this->queues[$queue][$list],
                            static fn (int $held): bool => $held !== $id,
                        ));
                    }
                }
                $this->ack($id);
            }
            $this->waiting[$i]['failed'] = true;
            return;
        }
    }

    /**
     * Drops a replica a leader rolled back with quorum_drop, by the message
     * id the leader assigned rather than the local id.
     */
    public function dropReplica(string $queue, string $messageId): void
    {
        if ($messageId === '') {
            return;
        }
        foreach ($this->msgs as $id => $msg) {
            if (($msg['qid'] ?? '') !== $messageId) {
                continue;
            }
            if ($queue !== '' && $msg['queue'] !== $queue) {
                continue;
            }
            $name = $msg['queue'];
            if (isset($this->queues[$name])) {
                foreach (['ready', 'replicas'] as $list) {
                    $this->queues[$name][$list] = array_values(array_filter(
                        $this->queues[$name][$list],
                        static fn (int $held): bool => $held !== $id,
                    ));
                }
            }
            $this->ack($id);
            return;
        }
    }

    /** Store a peer's quorum append. The body is durable before the reply. */
    public function enqueueLocal(string $queue, string $messageId, string $body, string $exchange, string $key, bool $persistent): bool
    {
        if (!isset($this->queues[$queue])) {
            return false;
        }
        $id = $this->nextId++;
        $this->msgs[$id] = [
            'queue' => $queue,
            'body' => $body,
            'mode' => $persistent ? 2 : 1,
            'redelivered' => false,
            'exchange' => $exchange,
            'key' => $key,
            'priority' => 0,
            'expires' => null,
            'headers' => [],
            'qid' => $messageId,
        ];
        if ($persistent) {
            $this->store->appendPublish($id, $queue, $body, 2);
            $this->store->sync();
        }
        $this->hold($queue, $id);
        return true;
    }

    public function pullBody(string $queue): ?string
    {
        if (!isset($this->queues[$queue]) || $this->queues[$queue]['ready'] === []) {
            return null;
        }
        $id = array_shift($this->queues[$queue]['ready']);
        if (!is_int($id) || !isset($this->msgs[$id])) {
            return null;
        }
        $body = $this->msgs[$id]['body'];
        $this->ack($id);
        return $body;
    }

    /** @return list<array{conn:int,ch:int,tag:int,nack:bool}> */
    public function flush(): array
    {
        $this->store->sync();
        $ready = [];
        $still = [];
        foreach ($this->waiting as $w) {
            // A quorum publish that could not reach a majority is nacked, so
            // the publisher learns the message was not accepted.
            if (($w['failed'] ?? false) === true) {
                $ready[] = ['conn' => $w['conn'], 'ch' => $w['ch'], 'tag' => $w['tag'], 'nack' => true];
                continue;
            }
            if ($w['end'] > $this->store->synced) {
                $still[] = $w;
                continue;
            }
            // A quorum confirm needs a durable majority, not just any replies.
            if (($w['quorumNeed'] ?? 0) > 0
                && !Features::durableMajority(max(1, count($this->members)), $w['copies'] ?? ['durable'])) {
                $still[] = $w;
                continue;
            }
            if ($w['end'] > 0) {
                foreach ($w['ids'] as $id) {
                    if (isset($this->msgs[$id])) {
                        $this->hold($this->msgs[$id]['queue'], $id);
                    }
                }
            }
            // The majority is in, so any gated transient quorum body in this
            // batch can now be delivered.
            $this->release($w['ids']);
            $ready[] = ['conn' => $w['conn'], 'ch' => $w['ch'], 'tag' => $w['tag'], 'nack' => false];
        }
        $this->waiting = $still;
        return $ready;
    }

    /**
     * Registers a local consumer.
     *
     * An exclusive consumer excludes every other consumer on the queue, and
     * cannot join a queue that already has one, which is a 403 as in Bun
     * (bun/src/broker/delivery.ts:200-202).
     *
     * @throws RuntimeException with the reply code as the exception code
     */
    public function addConsumer(string $queue, int $conn, int $ch, string $tag, bool $noAck = false, bool $exclusive = false, int $priority = 0): void
    {
        if (!isset($this->queues[$queue])) {
            throw new RuntimeException("NOT_FOUND - no queue '$queue'", 404);
        }
        foreach ($this->queues[$queue]['consumers'] as $existing) {
            if (($existing['exclusive'] ?? false) === true || $exclusive) {
                throw new RuntimeException("ACCESS_REFUSED - exclusive consumer on '$queue'", 403);
            }
        }
        $this->queues[$queue]['lastUsed'] = microtime(true);
        $this->queues[$queue]['consumers'][] = [
            'conn' => $conn,
            'ch' => $ch,
            'tag' => $tag,
            'credit' => 0,
            'noAck' => $noAck,
            'exclusive' => $exclusive,
            'priority' => $priority,
        ];
        $this->prom['consumers']++;
    }

    /**
     * Picks the consumer to serve next.
     *
     * With x-single-active-consumer the highest-priority ready consumer is
     * always chosen and the round-robin cursor does not move. Otherwise the
     * cursor walks the consumers at the highest priority present, so a lower
     * priority consumer only sees traffic when none above it has credit.
     *
     * @param callable(array<string, mixed>):bool $ready
     * @return ?int index into the queue's consumer list
     */
    public function pickConsumer(string $queue, callable $ready): ?int
    {
        $consumers = $this->queues[$queue]['consumers'] ?? [];
        if ($consumers === []) {
            return null;
        }
        $best = null;
        foreach ($consumers as $consumer) {
            if (!$ready($consumer)) {
                continue;
            }
            $priority = (int) ($consumer['priority'] ?? 0);
            if ($best === null || $priority > $best) {
                $best = $priority;
            }
        }
        if ($best === null) {
            return null;
        }
        if (($this->queues[$queue]['args']['singleActive'] ?? false) === true) {
            foreach ($consumers as $i => $consumer) {
                if ($ready($consumer) && (int) ($consumer['priority'] ?? 0) === $best) {
                    return $i;
                }
            }
            return null;
        }
        $n = count($consumers);
        $cursor = (int) ($this->queues[$queue]['rr'] ?? 0);
        for ($k = 0; $k < $n; $k++) {
            $i = ($cursor + $k) % $n;
            $consumer = $consumers[$i];
            if ($ready($consumer) && (int) ($consumer['priority'] ?? 0) === $best) {
                $this->queues[$queue]['rr'] = $i + 1;
                return $i;
            }
        }
        return null;
    }

    /**
     * Returns a message to the head of its queue. A message that has hit
     * x-delivery-limit is dead-lettered with reason rejected instead, so a
     * poison message cannot requeue forever.
     *
     * @return bool false when the message was dead-lettered rather than requeued
     */
    public function requeue(int $id): bool
    {
        if (!isset($this->msgs[$id])) {
            return false;
        }
        $queue = $this->msgs[$id]['queue'];
        // The queue was deleted while the message was out; there is nowhere to put it back.
        if (!isset($this->queues[$queue])) {
            unset($this->msgs[$id]);
            return false;
        }
        $limit = $this->queues[$queue]['args']['deliveryLimit'] ?? null;
        $this->msgs[$id]['deliveries'] = (int) ($this->msgs[$id]['deliveries'] ?? 0) + 1;
        // x-delivery-limit N allows N returns, so N+1 deliveries, as RabbitMQ does.
        if ($limit !== null && $this->msgs[$id]['deliveries'] > $limit) {
            $this->prom['dlxDeliveryLimit']++;
            $this->deadLetter($id, 'rejected');
            return false;
        }
        $this->msgs[$id]['redelivered'] = true;
        $this->prom['redelivered']++;
        $this->noteExpiry($queue, $id);
        array_unshift($this->queues[$queue]['ready'], $id);
        return true;
    }

    public function ack(int $id): void
    {
        if (!isset($this->msgs[$id])) {
            return;
        }
        $this->drop($id);
        $this->prom['acknowledged']++;
    }

    /**
     * Removes a message without counting it as acknowledged. Purge, rollback
     * and dead-letter all end here, so the ack counter reports only real
     * consumer acks.
     */
    public function drop(int $id): void
    {
        if (!isset($this->msgs[$id])) {
            return;
        }
        if (($this->msgs[$id]['mode'] ?? 1) === 2) {
            $this->store->appendAck($id);
        }
        unset($this->msgs[$id]);
    }

    /**
     * Dead-letters a message. x-death headers are attached as Bun builds them.
     *
     * With the at-least-once strategy a message the dead-letter exchange
     * would not accept is kept rather than dropped; at-most-once drops it,
     * which is the default.
     *
     * @return bool whether a destination accepted the message
     */
    public function deadLetter(int $id, string $reason = 'rejected'): bool
    {
        if (!isset($this->msgs[$id])) {
            return false;
        }
        $msg = $this->msgs[$id];
        $queue = $msg['queue'];
        $args = $this->queues[$queue]['args'] ?? Features::parseArgs([]);
        $exchange = $args['dlx'] ?? null;
        $atLeastOnce = ($args['dlxStrategy'] ?? 'at-most-once') === 'at-least-once';
        if (!is_string($exchange) || $exchange === '' || $this->dlxDepth >= self::DLX_DEPTH) {
            // Nowhere to send it. at-least-once keeps the message so it is
            // not silently lost; at-most-once drops it.
            if ($atLeastOnce) {
                return false;
            }
            $this->drop($id);
            return false;
        }
        $headers = Features::deathHeaders(
            is_array($msg['headers'] ?? null) ? $msg['headers'] : [],
            $queue,
            $reason,
            (string) ($msg['exchange'] ?? ''),
            (string) ($msg['key'] ?? $queue),
        );
        $this->dlxDepth++;
        try {
            $result = $this->publish(
                0,
                0,
                0,
                $exchange,
                $args['dlxKey'] ?? $msg['key'],
                $msg['body'],
                $msg['mode'] ?? 1,
                0,
                $headers,
            );
        } catch (RuntimeException $err) {
            $result = 'return';
        } finally {
            $this->dlxDepth--;
        }
        $accepted = $result === 'wait';
        if (!$accepted && $atLeastOnce) {
            return false;
        }
        $this->drop($id);
        return $accepted;
    }

    /** Nesting cap for dead-letter republishing, matching Bun's 8. */
    private const DLX_DEPTH = 8;
    private int $dlxDepth = 0;

    /**
     * Dead-letters a body that was rejected before it was ever queued, which
     * is what reject-publish-dlx does.
     *
     * @param list<array{0:string,1:mixed}> $headers
     */
    private function deadLetterBody(string $queue, string $exchange, string $key, string $body, array $headers = []): void
    {
        $args = $this->queues[$queue]['args'] ?? Features::parseArgs([]);
        $dlx = $args['dlx'] ?? null;
        if (!is_string($dlx) || $dlx === '' || $this->dlxDepth >= self::DLX_DEPTH) {
            return;
        }
        $this->dlxDepth++;
        try {
            $this->publish(
                0,
                0,
                0,
                $dlx,
                $args['dlxKey'] ?? $key,
                $body,
                1,
                0,
                Features::deathHeaders($headers, $queue, 'maxlen', $exchange, $key),
            );
        } catch (RuntimeException $err) {
            // No destination for the dead letter; nothing more to do.
        } finally {
            $this->dlxDepth--;
        }
    }

    /**
     * Dead-letters every message in a queue whose TTL has passed.
     *
     * Guarded by the queue's earliest known expiry, so a queue with no TTL
     * costs one integer comparison. Walking the ready list on every publish
     * would make a deep queue quadratic.
     */
    public function expire(string $queue): void
    {
        if (!isset($this->queues[$queue]) || $this->queues[$queue]['ready'] === []) {
            return;
        }
        $now = (int) (microtime(true) * 1000);
        if ($now < ($this->queues[$queue]['nextExpiry'] ?? PHP_INT_MAX)) {
            return;
        }
        $keep = [];
        $next = PHP_INT_MAX;
        foreach ($this->queues[$queue]['ready'] as $id) {
            $at = $this->msgs[$id]['expires'] ?? null;
            if (!is_int($at)) {
                $keep[] = $id;
                continue;
            }
            if ($at > $now) {
                $keep[] = $id;
                $next = min($next, $at);
                continue;
            }
            // The list is rebuilt as we go so a dead-letter republish that
            // lands back on this queue sees a consistent state.
            $this->queues[$queue]['ready'] = $keep;
            $this->prom['dlxExpired']++;
            if (!$this->deadLetter($id, 'expired') && isset($this->msgs[$id])) {
                // at-least-once kept it, so clear the expiry rather than
                // looping over the same message on every sweep.
                $this->msgs[$id]['expires'] = null;
                $keep[] = $id;
            }
        }
        $this->queues[$queue]['ready'] = $keep;
        $this->queues[$queue]['nextExpiry'] = $next;
    }

    /**
     * Expires messages across every queue and deletes queues idle past
     * x-expires. Called from the select loop.
     */
    public function sweep(): void
    {
        $now = microtime(true);
        foreach ($this->queues as $name => $queue) {
            $this->expire($name);
            $expiresMs = $queue['args']['expiresMs'] ?? null;
            if ($expiresMs === null || $queue['consumers'] !== []) {
                continue;
            }
            $idleMs = ($now - (float) ($queue['lastUsed'] ?? $now)) * 1000;
            if ($idleMs >= $expiresMs) {
                $this->deleteQueue($name);
            }
        }
    }

    /**
     * Rewrites the log when it is mostly dead records. Only safe while no
     * confirm is outstanding, because compaction resets the byte offsets the
     * waiting entries are gated on.
     */
    public function maybeCompact(): bool
    {
        if ($this->waiting !== [] || $this->store->sinceCompact < 256) {
            return false;
        }
        $this->store->sinceCompact = 0;
        $live = [];
        $bytes = 0;
        foreach ($this->queues as $name => $queue) {
            foreach (array_merge($queue['ready'], $queue['replicas']) as $id) {
                $msg = $this->msgs[$id] ?? null;
                if ($msg === null || ($msg['mode'] ?? 1) !== 2) {
                    continue;
                }
                $live[] = [
                    'id' => $id,
                    'queue' => $name,
                    'body' => $msg['body'],
                    'mode' => 2,
                    'propRaw' => $msg['propRaw'] ?? null,
                ];
                $bytes += strlen($msg['body']) + 32;
            }
        }
        if (!$this->store->shouldCompact($bytes)) {
            return false;
        }
        return $this->store->compact($live);
    }

    public function expired(int $id): bool
    {
        if (!isset($this->msgs[$id])) {
            return false;
        }
        $at = $this->msgs[$id]['expires'] ?? null;
        return is_int($at) && $at <= (int) (microtime(true) * 1000);
    }

    /**
     * Takes the next ready message without involving a consumer, for
     * basic.get. Expired messages are dead-lettered and skipped.
     */
    public function getReady(string $queue): ?int
    {
        if (!isset($this->queues[$queue])) {
            return null;
        }
        $this->expire($queue);
        while ($this->queues[$queue]['ready'] !== []) {
            $id = $this->queues[$queue]['ready'][0];
            // A gated quorum body is not available yet, and the queue is
            // ordered, so nothing behind it is either.
            if (is_int($id) && $this->isGated($id)) {
                return null;
            }
            array_shift($this->queues[$queue]['ready']);
            if (!is_int($id) || !isset($this->msgs[$id])) {
                continue;
            }
            $this->noteConsumed($queue, (string) ($this->msgs[$id]['qid'] ?? ''));
            return $id;
        }
        return null;
    }

    /** Ready message count, for basic.get-ok and queue.declare-ok. */
    public function readyCount(string $queue): int
    {
        return isset($this->queues[$queue]) ? count($this->queues[$queue]['ready']) : 0;
    }

    /** Consumer count, for queue.declare-ok. */
    public function consumerCount(string $queue): int
    {
        return isset($this->queues[$queue]) ? count($this->queues[$queue]['consumers']) : 0;
    }

    /** Drops every ready message and returns how many went. */
    public function purge(string $queue): int
    {
        if (!isset($this->queues[$queue])) {
            return 0;
        }
        $ids = $this->queues[$queue]['ready'];
        $this->queues[$queue]['ready'] = [];
        $n = 0;
        foreach ($ids as $id) {
            if (!is_int($id) || !isset($this->msgs[$id])) {
                continue;
            }
            // Purged, not acknowledged, so the ack counter is not inflated.
            $this->drop($id);
            $n++;
        }
        return $n;
    }

    /**
     * Removes a queue, its ready messages, and every binding that names it.
     * Returns the message count that went with it.
     */
    public function deleteQueue(string $name): int
    {
        if (!isset($this->queues[$name])) {
            return 0;
        }
        $n = $this->purge($name);
        $this->prom['consumers'] -= count($this->queues[$name]['consumers']);
        $this->prom['queuesDeleted']++;
        unset($this->queues[$name]);
        $this->bindings = array_values(array_filter(
            $this->bindings,
            static fn (array $row): bool => $row['queue'] !== $name,
        ));
        return $n;
    }

    /** @param list<array{0:string,1:string}> $args */
    public function unbind(string $queue, string $exchange, string $key, array $args = []): void
    {
        $this->bindings = array_values(array_filter(
            $this->bindings,
            static fn (array $row): bool => !(
                $row['queue'] === $queue
                && $row['exchange'] === $exchange
                && $row['key'] === $key
                && ($args === [] || $row['args'] === $args)
            ),
        ));
    }

    /**
     * Removes an exchange along with its bindings and exchange-to-exchange
     * links. The default exchange and the amq.* built-ins stay.
     */
    public function deleteExchange(string $name): bool
    {
        if ($name === '' || str_starts_with($name, 'amq.') || !isset($this->exchanges[$name])) {
            return false;
        }
        unset($this->exchanges[$name]);
        $this->bindings = array_values(array_filter(
            $this->bindings,
            static fn (array $row): bool => $row['exchange'] !== $name,
        ));
        $this->e2e = array_values(array_filter(
            $this->e2e,
            static fn (array $row): bool => $row['source'] !== $name && $row['destination'] !== $name,
        ));
        return true;
    }

    /** Links one exchange to another. Held in memory only, as Bun does. */
    public function bindExchange(string $destination, string $source, string $key): void
    {
        foreach ($this->e2e as $row) {
            if ($row['source'] === $source && $row['destination'] === $destination && $row['key'] === $key) {
                return;
            }
        }
        $this->e2e[] = ['source' => $source, 'destination' => $destination, 'key' => $key];
    }

    public function unbindExchange(string $destination, string $source, string $key): void
    {
        $this->e2e = array_values(array_filter(
            $this->e2e,
            static fn (array $row): bool => !(
                $row['source'] === $source
                && $row['destination'] === $destination
                && $row['key'] === $key
            ),
        ));
    }

    /**
     * The node that owns a classic queue. An empty member list means this is
     * a single node and everything is local.
     */
    public function home(string $queue): string
    {
        // A quorum queue keeps the node it was declared on.
        $stored = $this->queues[$queue]['home'] ?? '';
        if (is_string($stored) && $stored !== '') {
            return $stored;
        }
        if (count($this->members) < 2) {
            return $this->nodeId;
        }
        return Features::home($this->members, '/', $queue);
    }

    /** True when this node owns the queue, so no forwarding is needed. */
    public function ownsQueue(string $queue): bool
    {
        $home = $this->home($queue);
        return $home === '' || $home === $this->nodeId;
    }

    /**
     * Merges a peer's topology. Additive on purpose: an existing user,
     * exchange or queue is never overwritten, which is how Bun's
     * applySnapshot behaves. Bindings are matched before insert so repeated
     * handshakes do not pile up duplicates, which Bun does not guard against
     * (bun/src/broker/snapshot.ts:192-195).
     *
     * @param array<string, mixed> $snapshot
     */
    public function applySnapshot(array $snapshot): void
    {
        foreach ((array) ($snapshot['users'] ?? []) as $name => $hash) {
            // Accepts both a name list and a name to hash map.
            if (is_int($name) && is_string($hash)) {
                continue;
            }
            if (is_string($name) && is_string($hash) && !isset($this->users[$name])) {
                $this->users[$name] = $hash;
            }
        }
        foreach ((array) ($snapshot['exchanges'] ?? []) as $name => $kind) {
            if (is_string($name) && is_string($kind) && !isset($this->exchanges[$name])) {
                $this->exchanges[$name] = $kind;
            }
        }
        foreach ((array) ($snapshot['queues'] ?? []) as $row) {
            if (!is_array($row)) {
                continue;
            }
            $name = (string) ($row['name'] ?? '');
            if ($name === '' || isset($this->queues[$name])) {
                continue;
            }
            $type = (string) ($row['type'] ?? $row['queue_type'] ?? 'classic');
            $this->declareQueue($name, $type === 'quorum' ? ['x-queue-type' => 'quorum'] : []);
        }
        foreach ((array) ($snapshot['bindings'] ?? []) as $row) {
            if (!is_array($row)) {
                continue;
            }
            $queue = (string) ($row['queue'] ?? '');
            $exchange = (string) ($row['exchange'] ?? '');
            if ($queue === '') {
                continue;
            }
            $this->bind($queue, $exchange, (string) ($row['key'] ?? $row['routing_key'] ?? ''), []);
        }
    }

    /**
     * Registers a consumer that lives on another node. The pump sends it a
     * cluster deliver frame instead of an AMQP frame.
     */
    public function addRemoteConsumer(string $queue, string $peer, int $session, bool $noAck, ?int $credit): void
    {
        $this->declareQueue($queue);
        foreach ($this->queues[$queue]['consumers'] as $existing) {
            if (($existing['peer'] ?? '') === $peer && ($existing['session'] ?? 0) === $session) {
                return;
            }
        }
        $this->queues[$queue]['consumers'][] = [
            'conn' => -1,
            'ch' => 0,
            'tag' => 'peer-' . $peer . '-' . $session,
            'credit' => $credit ?? 0,
            'peer' => $peer,
            'session' => $session,
            'noAck' => $noAck,
        ];
    }

    public function removeRemoteConsumer(string $queue, string $peer, int $session): void
    {
        if (!isset($this->queues[$queue])) {
            return;
        }
        $this->queues[$queue]['consumers'] = array_values(array_filter(
            $this->queues[$queue]['consumers'],
            static fn (array $c): bool => !(($c['peer'] ?? '') === $peer && ($c['session'] ?? 0) === $session),
        ));
    }

    /** Adds delivery credit for a remote subscriber. Null means unlimited. */
    public function setRemoteCredit(string $queue, string $peer, int $session, ?int $credit, bool $add): void
    {
        if (!isset($this->queues[$queue])) {
            return;
        }
        foreach ($this->queues[$queue]['consumers'] as $i => $c) {
            if (($c['peer'] ?? '') !== $peer || ($c['session'] ?? 0) !== $session) {
                continue;
            }
            if ($credit === null) {
                $this->queues[$queue]['consumers'][$i]['credit'] = 0;
                return;
            }
            $this->queues[$queue]['consumers'][$i]['credit'] = $add
                ? (int) $c['credit'] + $credit
                : $credit;
            return;
        }
    }

    /** Drops every remote consumer belonging to a peer that went away. */
    public function dropPeerConsumers(string $peer): void
    {
        foreach ($this->queues as $name => $queue) {
            $this->queues[$name]['consumers'] = array_values(array_filter(
                $queue['consumers'],
                static fn (array $c): bool => ($c['peer'] ?? '') !== $peer,
            ));
        }
    }
}
