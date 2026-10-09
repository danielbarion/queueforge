<?php
declare(strict_types=1);

/**
 * Single-node and clustered queues. A classic confirm is not released until the
 * covering fsync. A quorum confirm also waits for a durable majority.
 */
require_once __DIR__ . "/Streams.php";
require_once __DIR__ . "/Security.php";

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
    public bool $raftApplying = false;
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
    public array $authConfig = [];
    /** Runtime identities keyed by credentials, never persisted as internal users. */
    public array $externalPrincipals = [];
    private ?string $externalIdentitySecret = null;
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
        'amq.rabbitmq.trace' => 'topic',
    ];
    /** True while the broker itself publishes, which may use an internal exchange. */
    public bool $internalPublish = false;
    public bool $tracing = false;
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

    public function __construct(public Store $store, public string $userFile, private ?Broker $registry = null, public string $vhost = "/")
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
                        $this->tags[$name] = array_key_exists('tags', $row) ? $tags : ['administrator'];
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
                'notBefore' => $meta['notBefore'] ?? null,
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
        if ($this->registry === null && is_file($this->dataDir() . '/topology.json')) {
            $state = json_decode((string) file_get_contents($this->dataDir() . '/topology.json'), true, 512, JSON_THROW_ON_ERROR);
            if (!is_array($state)) throw new RuntimeException('Invalid topology state');
            $this->savedTopology = $state; $this->vhosts = $state['vhosts'] ?? ['/'];
            foreach (['policies','operatorPolicies','topicPermissions','vhostLimits','userLimits','parameters','featureFlags'] as $field) if (isset($state[$field])) $this->$field = $state[$field];
            $this->restoreTopology($state['states']['/'] ?? []);
        }
    }

    /**
     * Creates a queue during replay. Recovery must not run the declare rules,
     * since a persisted queue already passed them and its declared arguments
     * are not in the log.
     */
    public function dataDir(): string { return dirname($this->store->path); }
    private array $vhostBrokers = [];
    private array $streams = [];
    private bool $loadingTopology = false;
    private array $savedTopology = [];

    public function root(): Broker { return $this->registry ?? $this; }
    public function forVhost(string $vhost): Broker
    {
        $root = $this->root();
        if (!in_array($vhost, $root->vhosts, true)) throw new RuntimeException("NOT_FOUND - vhost '$vhost'", 404);
        if ($vhost === '/') return $root;
        if (!isset($root->vhostBrokers[$vhost])) {
            $dir = $root->dataDir() . '/vhosts/' . bin2hex($vhost);
            $child = new Broker(new Store($dir . '/messages.log'), $root->userFile, $root, $vhost);
            foreach (['users','tags','permissions','topicPermissions','userLimits','vhostLimits','vhosts','policies','operatorPolicies','parameters','featureFlags','nodeId','members','cluster','prom','userByConn','currentUsers','authConfig','externalPrincipals'] as $field) $child->$field =& $root->$field;
            $root->vhostBrokers[$vhost] = $child;
            $child->restoreTopology($root->savedTopology['states'][$vhost] ?? []);
        }
        return $root->vhostBrokers[$vhost];
    }
    public $onDeleteVhost = null;
    public $onDeleteQueue = null;
    public function deleteVhost(string $name): void
    {
        $root = $this->root();
        if ($name === '/') throw new RuntimeException('The default vhost cannot be deleted', 406);
        if (!in_array($name, $root->vhosts, true)) return;
        if ($root->onDeleteVhost !== null) ($root->onDeleteVhost)($name);
        $path = $root->dataDir() . '/vhosts/' . bin2hex($name);
        self::removeTree($path);
        unset($root->vhostBrokers[$name], $root->savedTopology['states'][$name]);
        $root->vhosts = array_values(array_filter($root->vhosts, static fn($host) => $host !== $name));
        foreach (['policies','operatorPolicies','vhostLimits'] as $field) unset($root->{$field}[$name]);
        foreach ($root->permissions as $user => $hosts) unset($root->permissions[$user][$name], $root->topicPermissions[$user][$name]);
        foreach ($root->parameters as $component => $hosts) unset($root->parameters[$component][$name]);
        $root->saveUsers(); $root->saveTopology();
    }
    private static function removeTree(string $path): void
    {
        if (is_link($path) || is_file($path)) { if (!unlink($path)) throw new RuntimeException('Cannot remove broker resource'); return; }
        if (!is_dir($path)) return;
        foreach (new DirectoryIterator($path) as $entry) if (!$entry->isDot()) self::removeTree($entry->getPathname());
        if (!rmdir($path)) throw new RuntimeException('Cannot remove broker resource directory');
    }

    public function allBrokers(): array
    {
        $root = $this->root(); $all = [];
        foreach ($root->vhosts as $vhost) $all[$vhost] = $root->forVhost($vhost);
        return $all;
    }
    public static function writeDurable(string $path, string $bytes): void
    {
        $temp = $path . '.tmp-' . bin2hex(random_bytes(6)); $fp = @fopen($temp, 'xb');
        if ($fp === false) throw new RuntimeException('Cannot create durable state');
        try {
            $offset = 0; while ($offset < strlen($bytes)) { $n = fwrite($fp, substr($bytes, $offset)); if ($n === false || $n === 0) throw new RuntimeException('Cannot write durable state'); $offset += $n; }
            if (!fflush($fp) || !fsync($fp)) throw new RuntimeException('Cannot sync durable state');
            fclose($fp); $fp = null;
            if (!rename($temp, $path)) throw new RuntimeException('Cannot install durable state');
        } finally { if (is_resource($fp)) fclose($fp); if (is_file($temp)) unlink($temp); }
    }
    private function topologyState(): array
    {
        $queues = [];
        foreach ($this->queues as $name => $queue) if (($queue['durable'] ?? true) && !($queue['exclusive'] ?? false)) $queues[$name] = array_intersect_key($queue, array_flip(['declaredArgs','durable','exclusive','autoDelete','home','raftGroup']));
        $exchanges = []; $rows = [];
        foreach ($this->exchanges as $name => $kind) if (($this->exchangeRows[$name]['durable'] ?? true)) { $exchanges[$name] = $kind; if (isset($this->exchangeRows[$name])) $rows[$name] = $this->exchangeRows[$name]; }
        return ['tracing' => $this->tracing, 'queues' => $queues, 'exchanges' => $exchanges, 'exchangeRows' => $rows, 'bindings' => array_values(array_filter($this->bindings, static fn($b) => isset($queues[$b['queue']]) && isset($exchanges[$b['exchange']]))), 'e2e' => array_values(array_filter($this->e2e, static fn($b) => isset($exchanges[$b['source']], $exchanges[$b['destination']])) )];
    }
    private function restoreTopology(array $state): void
    {
        $this->loadingTopology = true;
        try {
            $this->tracing = ($state['tracing'] ?? false) === true;
            foreach (($state['queues'] ?? []) as $name => $queue) {
                if (!isset($this->queues[$name])) $this->declareRecovered($name);
                $this->queues[$name] = [...$this->queues[$name], ...$queue, 'args' => Features::parseArgs($this->withPolicy($queue['declaredArgs'] ?? [], $name))];
            }
            foreach (['exchanges','exchangeRows'] as $field) $this->$field = [...$this->$field, ...($state[$field] ?? [])];
            foreach (['bindings','e2e'] as $field) if (isset($state[$field])) $this->$field = $state[$field];
        } finally { $this->loadingTopology = false; }
    }
    public function saveTopology(): void
    {
        $root = $this->root(); if ($this->loadingTopology || $root->loadingTopology) return;
        $state = ['vhosts' => $root->vhosts, 'states' => []];
        foreach ($root->allBrokers() as $vhost => $broker) $state['states'][$vhost] = $broker->topologyState();
        foreach (['policies','operatorPolicies','topicPermissions','vhostLimits','userLimits','parameters','featureFlags'] as $field) $state[$field] = $root->$field;
        self::writeDurable($root->dataDir() . '/topology.json', json_encode($state, JSON_THROW_ON_ERROR)); $root->savedTopology = $state;
    }
    /** A portable Raft snapshot, shared with Bun and Rust. */
    public function raftState(string $group): mixed
    {
        if ($group === 'meta') {
            $root = $this->root(); $state = ['users'=>[], 'vhosts'=>$root->vhosts, 'permissions'=>[], 'queues'=>[], 'exchanges'=>[], 'bindings'=>[], 'exchangeBindings'=>[], 'policies'=>[], 'userLimits'=>[], 'vhostLimits'=>[], 'topicPermissions'=>[], 'parameters'=>[], 'globalParameters'=>[]];
            foreach ($root->users as $name=>$hash) $state['users'][] = ['name'=>$name, 'hash'=>$hash, 'password_hash'=>$hash, 'tags'=>$root->tags[$name] ?? []];
            foreach ($root->permissions as $user=>$vhosts) foreach ($vhosts as $vhost=>$row) $state['permissions'][] = ['user'=>$user, 'vhost'=>$vhost, ...$row];
            foreach ($root->allBrokers() as $scope) {
                foreach ($scope->queues as $name=>$q) if (!($q['exclusive'] ?? false)) $state['queues'][] = ['vhost'=>$scope->vhost, 'name'=>$name, 'durable'=>$q['durable'] ?? true, 'exclusive'=>false, 'autoDelete'=>$q['autoDelete'] ?? false, 'args'=>$this->raftQueueArgs($q['declaredArgs'] ?? []), 'auto_delete'=>$q['autoDelete'] ?? false, 'home'=>$q['home'] ?? null, 'raftGroup'=>$q['raftGroup'] ?? null];
                foreach ($scope->exchanges as $name=>$type) $state['exchanges'][] = ['vhost'=>$scope->vhost, 'name'=>$name, 'type'=>$type, 'kind'=>$name===''?'default':$type, 'auto_delete'=>$scope->exchangeRows[$name]['autoDelete'] ?? false, ...($scope->exchangeRows[$name] ?? ['durable'=>true, 'autoDelete'=>false, 'internal'=>false, 'alternate'=>null])];
                foreach ($scope->bindings as $b) if (!($scope->queues[$b['queue']]['exclusive'] ?? false)) $state['bindings'][] = ['vhost'=>$scope->vhost, 'queue'=>$b['queue'], 'exchange'=>$b['exchange'], 'routingKey'=>$b['key'], 'routing_key'=>$b['key'], 'args'=>$b['args'] ?? []];
                foreach ($scope->e2e as $b) $state['exchangeBindings'][] = ['vhost'=>$scope->vhost, 'source'=>$b['source'], 'destination'=>$b['destination'], 'routingKey'=>$b['key'], 'routing_key'=>$b['key'], 'key'=>$b['key']];
            }
            foreach (['policies','operatorPolicies'] as $field) foreach ($root->$field as $vhost=>$rows) foreach ($rows as $name=>$row) $state['policies'][] = $this->raftPolicyWire($vhost, (string)$name, $row, $field === 'operatorPolicies');
            foreach (['userLimits'=>'user','vhostLimits'=>'vhost'] as $field=>$key) foreach ($root->$field as $name=>$value) $state[$field][] = [$key=>$name, ...$value];
            foreach ($root->topicPermissions as $user=>$vhosts) foreach ($vhosts as $vhost=>$rows) foreach ($rows as $exchange=>$row) if (is_array($row)) $state['topicPermissions'][] = ['user'=>$user, 'vhost'=>$vhost, 'exchange'=>$exchange, ...$row];
            foreach ($root->parameters as $component=>$vhosts) foreach ($vhosts as $vhost=>$rows) foreach ($rows as $name=>$value) {
                if ($component === 'global') $state['globalParameters'][] = ['name'=>$name, 'value'=>$value];
                elseif ($component !== 'shovel') $state['parameters'][] = compact('component','vhost','name','value');
            }
            return $state;
        }
        $rows = [];
        foreach ($this->allBrokers() as $scope) foreach ($scope->queues as $name=>$queue) {
            if (($queue['raftGroup'] ?? 'quorum') !== $group) continue;
            if (($queue['args']['queueType'] ?? '') === 'stream') return $scope->stream($name)->snapshot($scope->vhost, (string)$name);
            if (($queue['args']['queueType'] ?? '') !== 'quorum') continue;
            $messages = [];
            foreach ($scope->msgs as $msg) if ($msg['queue'] === $name) $messages[] = ['v'=>1, 'vhost'=>$scope->vhost, 'queue'=>$name, 'message_id'=>$msg['qid'], 'body_b64'=>base64_encode($msg['body']), 'persistent'=>($msg['mode'] ?? 1) === 2, 'exchange'=>$msg['exchange'], 'routing_key'=>$msg['key'], 'headers'=>$msg['headers'] ?? [], 'propRaw'=>$msg['propRaw'] === null ? null : base64_encode($msg['propRaw']), 'priority'=>$msg['priority'] ?? 0, 'expiration'=>isset($msg['expires']) ? (string)max(0,$msg['expires']-(int)(microtime(true)*1000)) : null];
            $rows[] = ['vhost'=>$scope->vhost, 'queue'=>$name, 'messages'=>$messages];
        }
        return ['queues'=>$rows];
    }
    private function raftQueueArgs(array $args): array
    {
        foreach(['x-queue-type'=>'queue_type','x-message-ttl'=>'message_ttl_ms','x-expires'=>'expires_ms','x-max-length'=>'max_length','x-max-length-bytes'=>'max_length_bytes','x-overflow'=>'overflow','x-dead-letter-exchange'=>'dead_letter_exchange','x-dead-letter-routing-key'=>'dead_letter_routing_key','x-max-priority'=>'max_priority','x-delivery-limit'=>'delivery_limit','x-dead-letter-strategy'=>'dead_letter_strategy','x-queue-leader-locator'=>'leader_locator'] as $key=>$wire)if(isset($args[$key]))$args[$wire]=$args[$key];
        if(isset($args['x-single-active-consumer']))$args['single_active']=in_array($args['x-single-active-consumer'],[true,1,'true'],true);
        $maxAgeMs=self::streamMaxAgeMs($args['x-max-age']??null);
        if($maxAgeMs!==null)$args['max_age_ms']=$maxAgeMs;
        return $args;
    }
    private function raftPolicyWire(string $vhost, string $name, array $row, bool $operator): array
    {
        $out = ['vhost'=>$vhost, 'name'=>$name, 'pattern'=>$row['pattern'] ?? '', 'apply_to'=>$row['apply-to'] ?? 'all', 'priority'=>$row['priority'] ?? 0, 'operator'=>$operator, 'definition'=>$row['definition'] ?? []];
        foreach (['message-ttl'=>'message_ttl_ms','expires'=>'expires_ms','max-length'=>'max_length','max-length-bytes'=>'max_length_bytes','overflow'=>'overflow','dead-letter-exchange'=>'dead_letter_exchange','dead-letter-routing-key'=>'dead_letter_routing_key','dead-letter-strategy'=>'dead_letter_strategy','delivery-limit'=>'delivery_limit','alternate-exchange'=>'alternate_exchange'] as $key=>$wire) if (array_key_exists($key,$row['definition'] ?? [])) $out[$wire]=$row['definition'][$key];
        return $out;
    }
    public function installRaftState(string $group, mixed $state): void
    {
        if (!is_array($state)) throw new RuntimeException('Invalid Raft snapshot');
        $root=$this->root(); $previous=$root->raftApplying; $root->raftApplying=true;
        try {
            if ($group === 'meta') {
                foreach (['vhosts','users','permissions','exchanges','queues','bindings'] as $field) if (!isset($state[$field]) || !is_array($state[$field])) throw new RuntimeException('Unsupported metadata snapshot');
                if (isset($state['exchangeBindings']) && !is_array($state['exchangeBindings'])) throw new RuntimeException('Invalid exchange bindings snapshot');
                foreach (['users','permissions','exchanges','queues','bindings','exchangeBindings'] as $field) foreach ($state[$field] ?? [] as $row) if (!is_array($row)) throw new RuntimeException('Invalid metadata snapshot row');
                $vhosts=[]; foreach ($state['vhosts'] as $row) { $name=is_string($row)?$row:($row['name']??null); if(!is_string($name))throw new RuntimeException('Invalid vhost snapshot');$vhosts[]=$name; }
                $root->loadingTopology=true;
                try {
                    $root->vhosts=array_values(array_unique($vhosts));
                    foreach ($root->allBrokers() as $scope) {
                        $desired=[]; foreach ($state['queues'] as $q) if (($q['vhost']??'/')===$scope->vhost) $desired[(string)($q['name']??$q['queue']??'')]=true;
                        foreach ($scope->queues as $name=>$q) if (!($q['exclusive']??false) && !isset($desired[$name])) $scope->deleteQueue((string)$name);
                        $scope->bindings=array_values(array_filter($scope->bindings,static fn($b)=>$scope->queues[$b['queue']]['exclusive']??false)); $scope->e2e=[];
                        $scope->exchanges=[]; $scope->exchangeRows=[];
                    }
                    $root->users=[];$root->tags=[];$root->permissions=[];$root->policies=[];$root->operatorPolicies=[];
                    foreach (['userLimits','vhostLimits','topicPermissions'] as $field) if(array_key_exists($field,$state))$root->$field=[];
                    if(array_key_exists('parameters',$state)) foreach(array_keys($root->parameters) as $component) if(!in_array($component,['shovel','global'],true))unset($root->parameters[$component]);
                    if(array_key_exists('globalParameters',$state))unset($root->parameters['global']);
                    foreach (['users'=>'user','exchanges'=>'exchange','queues'=>'queue','bindings'=>'binding','permissions'=>'permission','policies'=>'policy','userLimits'=>'user_limits','vhostLimits'=>'vhost_limits','topicPermissions'=>'topic_permission','parameters'=>'parameter','globalParameters'=>'global_parameter'] as $field=>$kind) foreach ($state[$field]??[] as $row) $root->applyRaft('meta',$kind,$row,0);
                    foreach ($state['exchangeBindings'] ?? [] as $row) $root->applyRaft('meta', 'binding', ['destinationType'=>'exchange', ...$row], 0);
                    foreach(array_keys($root->vhostBrokers) as $vhost)if(!in_array($vhost,$root->vhosts,true))unset($root->vhostBrokers[$vhost]);
                } finally { $root->loadingTopology=false; }
                $root->saveUsers();$root->saveTopology();return;
            }
            if (array_key_exists('stream',$state)) {
                $snap=$state['stream'];if(!is_array($snap)||!is_string($snap['vhost']??null)||!is_string($snap['queue']??null))throw new RuntimeException('Invalid stream snapshot identity');
                $scope=$root->forVhost($snap['vhost']);$name=$snap['queue'];if(($scope->queues[$name]['raftGroup']??'quorum')!==$group)throw new RuntimeException('Stream snapshot group mismatch');
                $scope->stream($name)->install($state,$group);return;
            }
            if (!isset($state['queues']) || !is_array($state['queues'])) throw new RuntimeException('Unsupported quorum snapshot');
            $messages=[];
            foreach ($state['queues'] as $row) {
                if(!is_array($row)||!is_string($row['vhost']??null)||!is_string($row['queue']??null)||!is_array($row['messages']??null))throw new RuntimeException('Invalid quorum snapshot row');
                $scope=$root->forVhost($row['vhost']);$q=$scope->queues[$row['queue']]??null;
                if(($q['args']['queueType']??'')!=='quorum'||($q['raftGroup']??'quorum')!==$group)throw new RuntimeException('Quorum snapshot group mismatch');
                foreach($row['messages'] as $msg){if(!is_array($msg)||($msg['vhost']??null)!==$row['vhost']||($msg['queue']??null)!==$row['queue']||!is_string($msg['message_id']??null)||base64_decode((string)($msg['body_b64']??''),true)===false||(isset($msg['propRaw'])&&base64_decode((string)$msg['propRaw'],true)===false))throw new RuntimeException('Invalid quorum snapshot message');$messages[$row['vhost']."\0".$row['queue']."\0".$msg['message_id']]=$msg;}
            }
            foreach($root->allBrokers() as $scope)foreach($scope->queues as $name=>$q)if(($q['args']['queueType']??'')==='quorum'&&($q['raftGroup']??'quorum')===$group){
                foreach($scope->msgs as $id=>$msg)if($msg['queue']===$name){$scope->drop($id);unset($scope->consumed[$name."\0".$msg['qid']]);}
                $scope->queues[$name]['ready']=[];$scope->queues[$name]['replicas']=[];
            }
            foreach($messages as $msg)$root->applyRaft($group,'enq',$msg,0);
            foreach($root->allBrokers() as $scope)$scope->flushDurable();
        } finally { $root->raftApplying=$previous; }
    }
    public function applyRaft(string $group, string $kind, mixed $data, int $index): void
    {
        if(in_array($kind,['noop','config'],true))return;
        if(!is_array($data))throw new RuntimeException('Invalid Raft entry');
        $root=$this->root();$previous=$root->raftApplying;$root->raftApplying=true;
        try {
            if($kind==='members'){$root->members=$data;$root->saveTopology();return;}
            $vhost=(string)($data['vhost']??'/');$name=(string)($data['name']??$data['queue']??'');
            if($kind==='vhost'){$name=(string)($data['name']??$vhost);if(!in_array($name,$root->vhosts,true))$root->vhosts[]=$name;$root->forVhost($name);$root->saveTopology();return;}
            if($kind==='delete_vhost') { $root->deleteVhost((string)($data['name'] ?? $vhost)); return; }
            $scope=in_array($kind,['user','delete_user','permission','delete_permission','topic_permission','delete_topic_permission','user_limits','vhost_limits','global_parameter','delete_global_parameter'],true)?$root:$root->forVhost($vhost);
            switch($kind){
                case 'queue': case 'declare_queue':
                    $args=(array)($data['arguments']??$data['args']??[]);
                    foreach(['queue_type'=>'x-queue-type','message_ttl_ms'=>'x-message-ttl','expires_ms'=>'x-expires','max_length'=>'x-max-length','max_length_bytes'=>'x-max-length-bytes','overflow'=>'x-overflow','dead_letter_exchange'=>'x-dead-letter-exchange','dead_letter_routing_key'=>'x-dead-letter-routing-key','max_priority'=>'x-max-priority','delivery_limit'=>'x-delivery-limit','dead_letter_strategy'=>'x-dead-letter-strategy','leader_locator'=>'x-queue-leader-locator'] as $key=>$arg)if(isset($args[$key])){$args[$arg]=$args[$key];unset($args[$key]);}
                    if(isset($args['max_age_ms'])){$args['x-max-age']=ceil($args['max_age_ms']/1000).'s';unset($args['max_age_ms']);}if($args['single_active']??false)$args['x-single-active-consumer']=true;unset($args['single_active']);
                    if(isset($data['type']))$args['x-queue-type']=$data['type'];
                    if (isset($scope->queues[$name])) {
                        $scope->queues[$name]['declaredArgs']=$args; $scope->queues[$name]['args']=Features::parseArgs($scope->withPolicy($args,$name));
                        foreach (['durable'=>true,'exclusive'=>false,'autoDelete'=>false] as $field=>$default) $scope->queues[$name][$field]=$data[$field]??($field==='autoDelete'?($data['auto_delete']??$default):$default);
                    } else $scope->declareQueue($name,$args,$data['durable']??true,$data['exclusive']??false,false,$data['autoDelete']??$data['auto_delete']??false);
                    if(isset($data['home']))$scope->queues[$name]['home']=$data['home'];
                    if(isset($data['raftGroup'])){$scope->queues[$name]['raftGroup']=$data['raftGroup'];$root->cluster?->registerQueueGroup($data['raftGroup'], ($data['raftLeader'] ?? '') === $root->nodeId);}break;
                case 'exchange':
                    $type=(string)($data['type']??$data['kind']??'direct');if($type==='default')$type='direct';
                    if($name===''||str_starts_with($name,'amq.')) { $scope->exchanges[$name]=$type;$scope->exchangeRows[$name]=['durable'=>$data['durable']??true,'autoDelete'=>$data['autoDelete']??$data['auto_delete']??false,'internal'=>$data['internal']??false,'alternate'=>$data['alternate']??$data['alternate_exchange']??null]; }
                    else $scope->declareExchange($name,$type,$data['durable']??true,$data['autoDelete']??$data['auto_delete']??false,$data['internal']??false,$data['alternate']??$data['alternate_exchange']??null,false,$data['arguments']??[]);break;
                case 'binding': case 'unbind':
                    $source=(string)($data['source']??$data['exchange']??'');$dest=(string)($data['destination']??$data['queue']??'');$key=(string)($data['routingKey']??$data['routing_key']??$data['key']??'');$args=(array)($data['arguments']??$data['args']??[]);
                    if(($data['destinationType']??$data['destination_type']??'queue')==='exchange'){if($kind==='binding')$scope->bindExchange($dest,$source,$key);else $scope->e2e=array_values(array_filter($scope->e2e,static fn($b)=>!($b['source']===$source&&$b['destination']===$dest&&$b['key']===$key)));}
                    elseif($kind==='binding')$scope->bind($dest,$source,$key,$args);else $scope->unbind($dest,$source,$key,$args);break;
                case 'delete_queue':if(isset($scope->queues[$name]))$scope->deleteQueue($name);break;
                case 'delete_exchange':if(isset($scope->exchanges[$name]))$scope->deleteExchange($name);break;
                case 'user':$hash=$data['hash']??$data['password_hash']??null;if(is_string($hash)){$root->users[$name]=$hash;$root->tags[$name]=$data['tags']??[];$root->saveUsers();}break;
                case 'delete_user':$root->deleteUser($name);break;
                case 'permission':$root->setPermissions((string)($data['user']??$name),$vhost,(string)($data['configure']??''),(string)($data['write']??''),(string)($data['read']??''));break;
                case 'delete_permission':unset($root->permissions[(string)$data['user']][$vhost]);$root->saveUsers();break;
                case 'topic_permission':$root->topicPermissions[(string)$data['user']][$vhost][(string)$data['exchange']]=['write'=>(string)$data['write'],'read'=>(string)$data['read']];break;
                case 'delete_topic_permission':unset($root->topicPermissions[(string)$data['user']][$vhost][(string)$data['exchange']]);break;
                case 'user_limits': case 'vhost_limits':
                    $field=$kind==='user_limits'?'userLimits':'vhostLimits';$key=$kind==='user_limits'?'user':'vhost';$id=(string)($data[$key]??$name);$row=[];
                    foreach($data['value']??$data as $k=>$value)if(str_starts_with((string)$k,'max-')&&$value!==null)$row[$k]=$value;
                    if($row===[])unset($root->{$field}[$id]);else $root->{$field}[$id]=$row;break;
                case 'policy': case 'delete_policy':
                    $field=($data['operator']??false)?'operatorPolicies':'policies';if($kind==='delete_policy')unset($root->{$field}[$vhost][$name]);else{
                        $definition=(array)($data['definition']??[]);
                        foreach(['messageTtl'=>'message-ttl','message_ttl_ms'=>'message-ttl','expiresMs'=>'expires','expires_ms'=>'expires','dlx'=>'dead-letter-exchange','dead_letter_exchange'=>'dead-letter-exchange','dlxKey'=>'dead-letter-routing-key','dead_letter_routing_key'=>'dead-letter-routing-key','maxLength'=>'max-length','max_length'=>'max-length','maxLengthBytes'=>'max-length-bytes','max_length_bytes'=>'max-length-bytes','overflow'=>'overflow','dlxStrategy'=>'dead-letter-strategy','dead_letter_strategy'=>'dead-letter-strategy','deliveryLimit'=>'delivery-limit','delivery_limit'=>'delivery-limit','alternate'=>'alternate-exchange','alternate_exchange'=>'alternate-exchange'] as $wire=>$key)if(isset($data[$wire]))$definition[$key]=$data[$wire];
                        $root->{$field}[$vhost][$name]=['pattern'=>$data['pattern']??'', 'priority'=>$data['priority']??0,'apply-to'=>$data['apply-to']??$data['applyTo']??$data['apply_to']??'all','definition'=>$definition];
                    }$scope->applyPolicies();break;
                case 'parameter':$root->parameters[(string)$data['component']][$vhost][$name]=$data['value']??[];break;
                case 'delete_parameter':unset($root->parameters[(string)$data['component']][$vhost][$name]);break;
                case 'global_parameter':$root->parameters['global']['/'][$name]=$data['value']??null;break;
                case 'delete_global_parameter':unset($root->parameters['global']['/'][$name]);break;
                case 'enq':
                    $body=base64_decode((string)($data['body_b64']??''),true);$raw=isset($data['propRaw'])?base64_decode((string)$data['propRaw'],true):null;if($body===false||$raw===false)throw new RuntimeException('Invalid Raft body');
                    if(!$scope->enqueueLocal((string)$data['queue'],(string)$data['message_id'],$body,(string)($data['exchange']??''),(string)($data['routing_key']??''),($data['persistent']??false)===true,$raw,(array)($data['headers']??[]),(int)($data['priority']??0),isset($data['expiration'])&&$data['expiration']!==''?(int)$data['expiration']:null))throw new RuntimeException('Raft queue missing');$scope->flushDurable();break;
                case 'drop':foreach($data['ids']??[($data['id']??'')] as $qid){$queue=(string)$data['queue'];foreach($scope->msgs as $id=>$msg)if($msg['queue']===$queue&&$msg['qid']===(string)$qid)$scope->drop($id);$scope->dropByQid($queue,(string)$qid);$scope->noteConsumed($queue,(string)$qid);}$scope->flushDurable();break;
                case 'purge':$queue=(string)$data['queue'];foreach($scope->msgs as $id=>$msg)if($msg['queue']===$queue){$scope->noteConsumed($queue,$msg['qid']);$scope->drop($id);}$scope->purge($queue);$scope->queues[$queue]['replicas']=[];$scope->flushDurable();break;
                case 'sappend':$body=base64_decode((string)($data['body_b64']??''),true);$raw=isset($data['propRaw'])?base64_decode((string)$data['propRaw'],true):null;if($body===false||$raw===false)throw new RuntimeException('Invalid stream body');$scope->stream((string)$data['queue'])->append($body,(array)($data['headers']??[]),$raw,$data['reference']??null,$data['sequence']??null,$group,$index,isset($data['ts'])?(int)$data['ts']:null,(string)($data['exchange']??''),(string)($data['routing_key']??''));break;
                default:throw new RuntimeException('Unsupported Raft entry '.$kind);
            }
            $root->saveTopology();
        } finally { $root->raftApplying=$previous; }
    }
    public function raftLeaderChanged(string $group, ?string $leader): void { foreach($this->allBrokers() as $scope)$scope->refreshRole(); }
    /** Per-reference reservations serialize accepted publisher sequences before proposal. */
    private array $streamReservations = [];
    public function streamAppendAsync(string $name,string $body,array $headers,?string $propRaw,?string $reference,?int $sequence,callable $done): void
    {
        $completed=false;
        $finish=static function(bool $ok,?int $offset=null)use(&$completed,$done):void{if($completed)return;$completed=true;$done($ok,$offset);};
        if ($this->cluster===null || !$this->cluster->raftEnabled() || count($this->members)<=1) {
            try{$offset=$this->streamAppend($name,$body,$headers,$propRaw,$reference,$sequence);}catch(Throwable){$finish(false);return;}
            $finish(true,$offset);return;
        }
        try {
            $stored=$reference===null?null:$this->streamPublisherSequence($name,$reference);
            if($sequence!==null&&$stored!==null&&self::streamSequenceAtMost($sequence,$stored)){$finish(true,$this->streamAppend($name,$body,$headers,$propRaw,$reference,$sequence));return;}
        } catch(Throwable){$finish(false);return;}
        $key=$reference!==null&&$sequence!==null?$name."\0".$reference:null;
        if($key!==null&&isset($this->streamReservations[$key])){
            $last=array_key_last($this->streamReservations[$key]);$job=$this->streamReservations[$key][$last];
            if(self::streamSequenceAtMost($sequence,$job->sequence)){$job->callbacks[]=$finish;return;}
        }
        $job=(object)['sequence'=>$sequence,'callbacks'=>[$finish],'start'=>null,'started'=>false];
        $job->start=function()use($job,$key,$name,$body,$headers,$propRaw,$reference,$sequence):void{
            if($job->started)return;$job->started=true;
            $settled=false;
            $complete=function(bool $ok,?string $error=null)use(&$settled,$job,$key,$name,$body,$headers,$propRaw,$reference,$sequence):void{
                if($settled)return;$settled=true;$offset=null;
                if($ok){try{$offset=$reference!==null&&$sequence!==null?$this->streamAppend($name,$body,$headers,$propRaw,$reference,$sequence):$this->streamNext($name)-1;}catch(Throwable){$ok=false;}}
                if($key!==null){array_shift($this->streamReservations[$key]);if($this->streamReservations[$key]===[])unset($this->streamReservations[$key]);}
                foreach($job->callbacks as $callback){try{$callback($ok,$offset);}catch(Throwable){}}
                if($key!==null&&isset($this->streamReservations[$key]))($this->streamReservations[$key][0]->start)();
            };
            try{
                $stored=$reference===null?null:$this->streamPublisherSequence($name,$reference);
                if($sequence!==null&&$stored!==null&&self::streamSequenceAtMost($sequence,$stored)){$complete(true);return;}
                $group=$this->cluster->quorumGroup($this->vhost,$name);
                // A rejected proposal invokes complete itself; never complete it twice.
                $this->cluster->propose($group,'sappend',['vhost'=>$this->vhost,'queue'=>$name,'ts'=>(int)(microtime(true)*1000),'body_b64'=>base64_encode($body),'headers'=>$headers,'propRaw'=>$propRaw===null?null:base64_encode($propRaw),'exchange'=>'','routing_key'=>'','reference'=>$reference,'sequence'=>$sequence],$complete);
            }catch(Throwable){$complete(false);}
        };
        if($key===null){($job->start)();return;}
        $this->streamReservations[$key][]=$job;
        if(count($this->streamReservations[$key])===1)($job->start)();
    }
    private static function streamSequenceAtMost(int $a,int $b):bool
    {
        return ($a<0)!==($b<0)?$a>=0:$a<=$b;
    }
    public function publishAsync(int $conn,int $ch,string $exchange,string $key,string $body,int $mode,int $priority,array $headers,?int $expiration,?string $propRaw,callable $done):void
    {
        try {
            $result=$this->publish($conn,$ch,0,$exchange,$key,$body,$mode,$priority,$headers,$expiration,$propRaw);
            if($result!=='wait'){$done($result==='return');return;}
            $at=array_key_last($this->waiting);
            if($at===null){$done(true);return;}
            $this->waiting[$at]['callback']=$done;$this->flushDurable();
        } catch(Throwable){$done(false);}
    }
    public function registerProtocolConsumer(string $queue,int $conn,string $tag,callable $readyFn,callable $deliverFn,bool $noAck=false,int $priority=0):void
    {
        $this->addConsumer($queue,$conn,-1,$tag,$noAck,false,$priority);
        $at=array_key_last($this->queues[$queue]['consumers']);
        $this->queues[$queue]['consumers'][$at]['readyFn']=$readyFn;$this->queues[$queue]['consumers'][$at]['deliverFn']=$deliverFn;
    }
    public function unregisterProtocolConsumer(string $queue,int $conn,string $tag):void
    {
        if(!isset($this->queues[$queue]))return;
        $before=count($this->queues[$queue]['consumers']);
        $this->queues[$queue]['consumers']=array_values(array_filter($this->queues[$queue]['consumers'],static fn($c)=>$c['conn']!==$conn||$c['tag']!==$tag));
        $this->prom['consumers']-=$before-count($this->queues[$queue]['consumers']);
    }
    public function pumpConsumers():void
    {
        $remaining=128;
        foreach($this->queues as $name=>$queue) {
            if(($queue['args']['queueType']??'')==='stream')continue;
            while($remaining>0 && ($this->queues[$name]['ready']??[])!==[]) {
                $pick=$this->pickConsumer($name,static fn($c)=>isset($c['readyFn'])&&($c['readyFn'])());
                if($pick===null)break;$consumer=$this->queues[$name]['consumers'][$pick];if(!isset($consumer['deliverFn']))break;
                $id=$this->getReady($name);if($id===null)break;$message=$this->msgs[$id];$remaining--;
                ($consumer['deliverFn'])($message,$id);
                if(($consumer['noAck']??false)&&isset($this->msgs[$id]))$this->ack($id);
            }
        }
    }

    public function resourceAllowed(string $user, string $vhost, string $operation, string $name): bool
    {
        $principal = $this->externalPrincipal($user);
        if ($principal !== null) return Security::allows($principal, $vhost, $operation, $name);
        if ($this->isExternalIdentity($user) && !array_key_exists($user, $this->users)) return false;
        if ($this->isAdmin($user)) return true;
        $pattern = $this->permissions[$user][$vhost][$operation] ?? null;
        return is_string($pattern) && @preg_match('~' . str_replace('~', '\\~', $pattern) . '~', $name) === 1;
    }
    private function stream(string $name): Streams
    {
        if (($this->queues[$name]['args']['queueType'] ?? null) !== 'stream') throw new RuntimeException('NOT_FOUND - stream queue', 404);
        $stream = $this->streams[$name] ??= new Streams($this->dataDir() . '/streams/' . bin2hex($name));
        $maxBytes = $this->queues[$name]['args']['maxLengthBytes'] ?? null;
        $maxAgeMs = self::streamMaxAgeMs($this->queues[$name]['declaredArgs']['x-max-age'] ?? null);
        $stream->retention(is_int($maxBytes) ? $maxBytes : null, $maxAgeMs);
        return $stream;
    }
    private static function streamMaxAgeMs(mixed $age): ?int
    {
        if (is_string($age) && preg_match('/^(\d+)([YMDdhms])$/', $age, $match)) {
            $unit = ['Y'=>31536000000, 'M'=>2592000000, 'D'=>86400000, 'd'=>86400000, 'h'=>3600000, 'm'=>60000, 's'=>1000][$match[2]];
            $amount = (float)$match[1];
            return $amount > intdiv(PHP_INT_MAX, $unit) ? PHP_INT_MAX : (int)$amount * $unit;
        }
        return null;
    }
    public function streamAppend(string $name, string $body, array $headers = [], ?string $propRaw = null, ?string $reference = null, ?int $sequence = null): int { return $this->stream($name)->append($body,$headers,$propRaw,$reference,$sequence); }
    public function streamRead(string $name, int $offset, int $count): array { return $this->stream($name)->read($offset,$count); }
    public function streamFirst(string $name): int { return $this->stream($name)->first(); }
    public function streamNext(string $name): int { return $this->stream($name)->next(); }
    public function streamPublisherSequence(string $name, string $reference): ?int { return $this->stream($name)->sequence($reference); }
    public function streamStoredOffset(string $name, string $reference): ?int { return $this->stream($name)->stored($reference); }
    public function streamStoreOffset(string $name, string $reference, int $offset): void { $this->stream($name)->storeOffset($reference,$offset); }
    public function flushDurable(): void
    {
        $this->store->sync(); foreach ($this->streams as $stream) $stream->sync();
        $still = [];
        $batch = $this->waiting; $this->waiting = [];
        foreach ($batch as $waiting) {
            if ($waiting['tag'] === 0 && ($waiting['completion']->pending ?? 0) === 0 && !($waiting['completion']->failed ?? false) && $waiting['end'] <= $this->store->synced && ($waiting['quorumNeed'] ?? 0) <= 1) {
                foreach ($waiting['ids'] as $id) if (isset($this->msgs[$id]) && !in_array($id, $this->queues[$this->msgs[$id]['queue']]['ready'], true)) $this->hold($this->msgs[$id]['queue'], $id);
                $this->release($waiting['ids']);
                if(isset($waiting['callback']))($waiting['callback'])(true);
            } else $still[] = $waiting;
        }
        $this->waiting = [...$still, ...$this->waiting];
    }

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
        $tags = $this->userTags($name);
        foreach (['administrator', 'management', 'monitoring'] as $tag) {
            if (in_array($tag, $tags, true)) {
                return true;
            }
        }
        return false;
    }

    public function isAdmin(string $name): bool
    {
        return in_array('administrator', $this->userTags($name), true);
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

    /** Returns a credential-specific identity; callers retain it for every permission check. */
    public function authenticate(string $user, string $pass): ?string
    {
        if (array_key_exists($user, $this->users)) return Auth::matches($pass, $this->users[$user]) ? $user : null;
        $principal = Security::verify($this->root(), $this->authConfig, $user, $pass);
        return $principal === null ? null : $this->registerExternalPrincipal($user, $pass, $principal);
    }
    public function verify(string $user, string $pass): bool { return $this->authenticate($user, $pass) !== null; }
    private function registerExternalPrincipal(string $user, string $pass, array $principal): string
    {
        $root = $this->root();
        $root->externalIdentitySecret ??= random_bytes(32);
        $identity = '@external:' . hash_hmac('sha256', pack('N', strlen($user)) . $user . $pass, $root->externalIdentitySecret);
        $root->externalPrincipals[$identity] = $principal;
        return $identity;
    }
    private function isExternalIdentity(string $identity): bool { return str_starts_with($identity, '@external:'); }
    private function externalPrincipal(string $identity): ?array
    {
        $principal = $this->root()->externalPrincipals[$identity] ?? null;
        if (!is_array($principal)) return null;
        // Creating an internal account revokes an external identity with that visible name.
        if (array_key_exists($identity, $this->users) || array_key_exists((string) ($principal['name'] ?? ''), $this->users)) $principal['expiresAt'] = 0;
        return $principal;
    }
    public function identityName(string $identity): string
    {
        return (string) ($this->externalPrincipal($identity)['name'] ?? $identity);
    }
    public function userTags(string $identity): array
    {
        $principal = $this->externalPrincipal($identity);
        if ($principal !== null) return Security::live($principal) ? ($principal['tags'] ?? []) : [];
        return $this->isExternalIdentity($identity) && !array_key_exists($identity, $this->users) ? [] : ($this->tags[$identity] ?? []);
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
        if (in_array($type, ['quorum','stream'], true) && (!$durable || $exclusive)) {
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
        $this->saveTopology();
        $this->emitEvent('queue.created', [['name', $name], ['durable', $durable], ['exclusive', $exclusive]]);
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
            Policy::match($this->policies[$this->vhost] ?? [], $name, 'queues'),
            Policy::match($this->operatorPolicies[$this->vhost] ?? [], $name, 'queues'),
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
    public function declareExchange(string $name, string $kind, bool $durable = true, bool $autoDelete = false, bool $internal = false, ?string $alternate = null, bool $passive = false, array $arguments = []): void
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
            'arguments' => $arguments,
        ];
        $this->saveTopology();
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
        $this->saveTopology();
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
            $headers[] = ['vhost', $this->vhost];
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
                $this->policies[$this->vhost] ?? [],
                $this->operatorPolicies[$this->vhost] ?? [],
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
        if ($kind === 'x-delayed-message') $kind = $this->exchangeRows[$exchange]['arguments']['x-delayed-type'] ?? 'direct';
        if ($kind === 'x-local-random') return $rows === [] ? [] : [$rows[random_int(0, count($rows)-1)]['queue']];
        if ($kind === 'x-consistent-hash') {
            $chosen = null; $best = null;
            foreach ($rows as $row) {
                $weight = filter_var($row['key'], FILTER_VALIDATE_INT);
                if ($weight === false || $weight < 1 || $weight > 10000) continue;
                for ($replica = 0; $replica < $weight; $replica++) {
                    $score = hash('sha256', $key . "\0" . $row['queue'] . "\0" . $replica);
                    if ($best === null || strcmp($score, $best) > 0) { $best = $score; $chosen = $row['queue']; }
                }
            }
            return $chosen === null ? [] : [$chosen];
        }
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
            $user = $this->userByConn[$conn] ?? $this->currentUsers[$conn] ?? 'guest';
            if (!$this->topicWriteAllowed($user, $this->vhost, $exchange, $key)) {
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
        if ($this->tracing && !$this->internalPublish && $exchange !== 'amq.rabbitmq.trace') {
            $this->internalPublish = true;
            try { $this->publish(0, 0, 0, 'amq.rabbitmq.trace', 'publish.' . $exchange, $body, 1, 0, [['exchange_name', $exchange], ['routing_keys', [$key]]]); }
            finally { $this->internalPublish = false; }
        }
        $this->prom['routed']++;
        $ids = [];
        $qids = [];
        $end = 0;
        $rejected = 0;
        $forwarded = 0;
        $streamWrites = 0;
        $completion = (object) ['pending' => 0, 'failed' => false];
        $settled = static function (bool $ok) use ($completion): void {
            $completion->pending--;
            if (!$ok) $completion->failed = true;
        };
        foreach ($dests as $queue) {
            if (!isset($this->queues[$queue])) {
                continue;
            }
            if (($this->queues[$queue]['args']['queueType'] ?? 'classic') === 'stream') {
                $completion->pending++;
                $this->streamAppendAsync($queue, $body, $headers, $propRaw, null, null, static function (bool $ok, ?int $offset) use ($settled): void { $settled($ok); });
                $streamWrites++;
                continue;
            }
            if (($this->queues[$queue]['args']['queueType'] ?? '') === 'quorum' && $this->cluster?->raftEnabled()) {
                $completion->pending++;
                $qid = 'q-' . $this->nodeId . '-' . bin2hex(random_bytes(12));
                $group = $this->cluster->quorumGroup($this->vhost, $queue);
                $this->cluster->registerQueueGroup($group);
                $accepted = $this->cluster->propose($group, 'enq', [
                    'v' => 1, 'vhost' => $this->vhost, 'queue' => $queue, 'message_id' => $qid,
                    'body_b64' => base64_encode($body), 'persistent' => $mode === 2,
                    'exchange' => $exchange, 'routing_key' => $key, 'headers' => $headers,
                    'propRaw' => $propRaw === null ? null : base64_encode($propRaw),
                ], static function (bool $ok, ?string $error) use ($settled): void { $settled($ok); });
                // propose invokes the completion callback even when rejected.
                $forwarded++;
                continue;
            }
            // A classic queue lives on one node. A publish that arrives
            // anywhere else is forwarded to its home, otherwise the message
            // sits here and a consumer attached at the home never sees it.
            $home = $this->remoteHomeOf($queue);
            if ($home !== null && $this->cluster !== null) {
                $completion->pending++;
                $this->cluster->request($home, 'enqueue', [
                    'vhost' => $this->vhost,
                    'queue' => $queue,
                    'exchange' => $exchange,
                    'routing_key' => $key,
                    'body_b64' => base64_encode($body),
                    'persistent' => $mode === 2,
                    'durable' => $mode === 2,
                    'headers' => $headers,
                    'propRaw' => $propRaw === null ? null : base64_encode($propRaw),
                ], '', static function (?array $reply) use ($settled): void { $settled($reply !== null); });
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
                'notBefore' => ($this->exchanges[$exchange] ?? '') === 'x-delayed-message' ? $now + max(0, (int) (array_column($headers, 1, 0)['x-delay'] ?? 0)) : null,
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
        if ($rejected > 0 || ($ids === [] && $forwarded === 0 && $streamWrites === 0)) {
            return 'nack';
        }
        $need = 0;
        $copies = ['durable'];
        if ($this->quorumPublish($dests) && !($this->cluster?->raftEnabled() ?? false)) {
            $need = Features::majority(max(1, count($this->members)));
            if ($this->cluster !== null) {
                foreach ($qids as $i => $qid) {
                    $queue = $this->msgs[$ids[$i]]['queue'];
                    $this->cluster->replicate(Features::encodeQuorumAppend($this->vhost, $queue, $qid, $body, $exchange, $key, $mode === 2));
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
            'completion' => $completion,
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
        if (($msg['notBefore'] ?? null) !== null) return array_intersect_key($msg, array_flip(['exchange','key','priority','expires','notBefore','headers','qid','deliveries']));
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
        if (!in_array($vhost, $this->vhosts, true)) return false;
        $principal = $this->externalPrincipal($user);
        if ($principal !== null) return Security::hasVhost($principal, $vhost);
        if ($this->isExternalIdentity($user) && !array_key_exists($user, $this->users)) return false;
        if ($this->isAdmin($user)) return true;
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
        $principal = $this->externalPrincipal($user);
        if ($principal !== null) return Security::allows($principal, $vhost, 'write', $exchange, $key);
        if ($this->isExternalIdentity($user) && !array_key_exists($user, $this->users)) return false;
        $row = $this->topicPermissions[$user][$vhost][$exchange] ?? ($vhost === '/' ? $this->topicPermissions[$user][$exchange] ?? null : null);
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
        $principal = $this->externalPrincipal($user);
        if ($principal !== null) return Security::allows($principal, $vhost, 'read', $exchange, $key);
        if ($this->isExternalIdentity($user) && !array_key_exists($user, $this->users)) return false;
        $row = $this->topicPermissions[$user][$vhost][$exchange] ?? ($vhost === '/' ? $this->topicPermissions[$user][$exchange] ?? null : null);
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
        if ($qid === '' || ($this->cluster?->raftEnabled() ?? false)) {
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
        if ($type === 'quorum' && !$this->queueIsLeader($queue)) {
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

    public function queueIsLeader(string $queue): bool
    {
        if ($this->cluster?->raftEnabled() && ($this->queues[$queue]['args']['queueType'] ?? '') === 'quorum') {
            return $this->cluster->queueLeader($this->vhost, $queue) === $this->nodeId;
        }
        return $this->isLeader();
    }

    public function refreshRole(): void
    {
        foreach ($this->queues as $name => $queue) {
            if (($queue['args']['queueType'] ?? 'classic') !== 'quorum') {
                continue;
            }
            if ($this->queueIsLeader($name)) {
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
                            'vhost' => $this->vhost,
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
    public function enqueueLocal(string $queue, string $messageId, string $body, string $exchange, string $key, bool $persistent, ?string $propRaw = null, array $headers = [], int $priority = 0, ?int $expiration = null): bool
    {
        if (!isset($this->queues[$queue])) {
            return false;
        }
        if ($this->wasConsumed($queue, $messageId)) return true;
        foreach ($this->msgs as $msg) if ($msg['queue'] === $queue && ($msg['qid'] ?? '') === $messageId) return true;
        $id = $this->nextId++;
        $this->msgs[$id] = [
            'queue' => $queue,
            'body' => $body,
            'mode' => $persistent ? 2 : 1,
            'propRaw' => $propRaw, 'headers' => $headers,
            'redelivered' => false,
            'exchange' => $exchange,
            'key' => $key,
            'priority' => $priority,
            'expires' => $expiration === null ? null : (int)(microtime(true)*1000) + $expiration,
            'qid' => $messageId,
        ];
        if ($persistent) {
            $this->store->appendPublish($id, $queue, $body, 2, $propRaw, self::meta($this->msgs[$id]));
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
        $batch = $this->waiting; $this->waiting = [];
        foreach ($batch as $w) {
            // A quorum publish that could not reach a majority is nacked, so
            // the publisher learns the message was not accepted.
            if (($w['failed'] ?? false) === true || ($w['completion']->failed ?? false)) {
                if(isset($w['callback']))($w['callback'])(false); else $ready[] = ['conn' => $w['conn'], 'ch' => $w['ch'], 'tag' => $w['tag'], 'nack' => true];
                continue;
            }
            if (($w['completion']->pending ?? 0) > 0 || $w['end'] > $this->store->synced) {
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
            if(isset($w['callback']))($w['callback'])(true); else $ready[] = ['conn' => $w['conn'], 'ch' => $w['ch'], 'tag' => $w['tag'], 'nack' => false];
        }
        $this->waiting = [...$still, ...$this->waiting];
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
        if (($this->queues[$queue]['args']['singleActive'] ?? false) === true) {
            $active = $this->queues[$queue]['activeConsumer'] ?? null;
            $found = null;
            foreach ($consumers as $i => $consumer) if (($consumer['tag'] ?? '') . ':' . ($consumer['conn'] ?? $consumer['session'] ?? '') === $active) $found = $i;
            if ($found === null) {
                $found = array_key_first($consumers);
                $consumer = $consumers[$found];
                $this->queues[$queue]['activeConsumer'] = ($consumer['tag'] ?? '') . ':' . ($consumer['conn'] ?? $consumer['session'] ?? '');
            }
            return $ready($consumers[$found]) ? $found : null;
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
        $message = $this->msgs[$id];
        $queue = $message['queue'];
        if ($this->cluster?->raftEnabled() && !$this->root()->raftApplying && ($this->queues[$queue]['args']['queueType'] ?? '') === 'quorum') {
            $group = $this->cluster->quorumGroup($this->vhost, $queue);
            $accepted = $this->cluster->propose($group, 'drop', ['vhost' => $this->vhost, 'queue' => $queue, 'ids' => [(string) $message['qid']]], function (bool $ok, ?string $error) use ($id): void {
                if (!$ok && isset($this->msgs[$id])) $this->requeue($id);
            });
            // Rejection also invokes the callback; avoid requeueing twice.
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
        if (!isset($this->msgs[$id])) return false;
        if($this->msgs[$id]['dlxPending']??false)return true;
        $msg=$this->msgs[$id];$queue=$msg['queue'];$args=$this->queues[$queue]['args']??Features::parseArgs([]);
        $exchange=$args['dlx']??null;$atLeastOnce=($args['dlxStrategy']??'at-most-once')==='at-least-once';
        if(!is_string($exchange)||$this->dlxDepth>=self::DLX_DEPTH){if(!$atLeastOnce)$this->settleDeadLetter($id);return false;}
        $headers=Features::deathHeaders(is_array($msg['headers']??null)?$msg['headers']:[],$queue,$reason,(string)($msg['exchange']??''),(string)($msg['key']??$queue));
        $this->msgs[$id]['dlxPending']=true;
        $confirmed=null;
        $complete=function(bool $ok)use($id,$queue,$atLeastOnce,&$confirmed):void{
            $confirmed=$ok;if(!isset($this->msgs[$id]))return;unset($this->msgs[$id]['dlxPending']);
            if($ok||!$atLeastOnce){$this->settleDeadLetter($id);return;}
            // A failed destination commit leaves the durable source available for retry.
            if(isset($this->queues[$queue])&&!in_array($id,$this->queues[$queue]['ready'],true))$this->pushReady($queue,$id);
        };
        $this->dlxDepth++;
        try{$result=$this->publish(0,0,0,$exchange,$args['dlxKey']??$msg['key'],$msg['body'],$msg['mode']??1,0,$headers);}
        catch(Throwable){$result='return';}
        finally{$this->dlxDepth--;}
        if($result!=='wait'){$complete(false);return false;}
        $at=array_key_last($this->waiting);
        if($at===null){$complete(false);return false;}
        $this->waiting[$at]['callback']=$complete;
        // Keep the body in the source log but prevent another consumer taking it during transfer.
        if(isset($this->queues[$queue]))$this->queues[$queue]['ready']=array_values(array_filter($this->queues[$queue]['ready'],static fn($ready)=>$ready!==$id));
        $this->flushDurable();
        return $confirmed!==false;
    }
    /** Quorum source removal must commit too, including rejects and expiry. */
    private function settleDeadLetter(int $id): void
    {
        if(!isset($this->msgs[$id]))return;$queue=$this->msgs[$id]['queue'];
        if($this->cluster?->raftEnabled()&&!$this->root()->raftApplying&&($this->queues[$queue]['args']['queueType']??'')==='quorum')$this->ack($id);
        else $this->drop($id);
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
        if (($this->queues[$queue]['args']['queueType'] ?? '') === 'quorum' && !$this->queueIsLeader($queue)) return null;
        $this->expire($queue);
        $now = (int) (microtime(true) * 1000);
        foreach ($this->queues[$queue]['ready'] as $position => $id) {
            if (($this->msgs[$id]['notBefore'] ?? 0) > $now) continue;
            // A gated quorum body is not available yet, and the queue is
            // ordered, so nothing behind it is either.
            if (is_int($id) && $this->isGated($id)) {
                return null;
            }
            array_splice($this->queues[$queue]['ready'], $position, 1);
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
        if ($this->root()->raftApplying && $this->root()->onDeleteQueue !== null) ($this->root()->onDeleteQueue)($this->vhost, $name, $this->queues[$name]['consumers']);
        $n = $this->purge($name);
        if (($this->queues[$name]['args']['queueType'] ?? '') === 'stream') {
            unset($this->streams[$name]);
            self::removeTree($this->dataDir() . '/streams/' . bin2hex($name));
        }
        $this->prom['consumers'] -= count($this->queues[$name]['consumers']);
        $this->prom['queuesDeleted']++;
        unset($this->queues[$name]);
        $this->bindings = array_values(array_filter(
            $this->bindings,
            static fn (array $row): bool => $row['queue'] !== $name,
        ));
        $this->saveTopology();
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
        $this->saveTopology();
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
        $this->saveTopology();
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
        $this->saveTopology();
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
        $this->saveTopology();
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
        return Features::home($this->members, $this->vhost, $queue);
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
        foreach ($snapshot['vhosts'] ?? [] as $host) {
            $name = is_array($host) ? ($host['name'] ?? '') : $host;
            if (is_string($name) && !in_array($name, $this->vhosts, true)) $this->vhosts[] = $name;
        }
        foreach (['users'=>'user','exchanges'=>'exchange','queues'=>'queue','bindings'=>'binding','permissions'=>'permission'] as $field=>$kind) {
            foreach ($snapshot[$field] ?? [] as $key=>$row) {
                if (is_string($row) && is_string($key)) $row = $field === 'users' ? ['name'=>$key,'hash'=>$row] : ['name'=>$key,'type'=>$row];
                if (!is_array($row)) continue;
                $scope = $this->forVhost((string)($row['vhost'] ?? '/'));
                $name = (string)($row['name'] ?? '');
                if ($kind === 'user' && isset($this->users[$name])) continue;
                if ($kind === 'exchange' && isset($scope->exchanges[$name])) continue;
                if ($kind === 'queue' && isset($scope->queues[$name])) continue;
                $this->applyRaft('meta', $kind, $row, 0);
            }
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
