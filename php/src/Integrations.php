<?php
declare(strict_types=1);

/**
 * Dynamic shovels and federation links. A source delivery remains
 * unacknowledged until the destination confirms it.
 */
final class Integrations
{
    private array $links = [];
    private int $next = -3000000;
    /** Links are reconciled against parameters and policies a few times a second, not on every loop pass. */
    private float $nextScan = 0.0;
    private array $wanted = [];
    /** The parameter and policy tables the last scan read; an edit replaces the array, so `!==` is a pointer compare until then. */
    private ?array $scannedParameters = null;
    private ?array $scannedPolicies = null;
    public function __construct(private Broker $broker) {}

    public static function localVhost(string $uri, string $fallback): ?string
    {
        if (!preg_match('~^amqps?://(?:/(.*))?$~', trim($uri), $m)) return null;
        return isset($m[1]) && $m[1] !== '' ? rawurldecode($m[1]) : $fallback;
    }
    /** A URI with its password removed, as the management API shows it. */
    public static function redact(string $uri): string
    {
        return (string) preg_replace('~^(\w+://[^:/@]*):[^@/]*@~', '$1@', $uri);
    }
    /** The status is operational; unsupported or disconnected endpoints never appear as running. */
    public function status(): array
    {
        $rows = [];
        foreach ($this->links as $link) $rows[] = ['name'=>$link->definition['name'],'vhost'=>$link->definition['vhost'],'type'=>$link->definition['type'],'state'=>$link->state(),'error'=>$link->error,'transferred'=>$link->transferred];
        return $rows;
    }
    /** GET /api/shovels rows: name, vhost, type "dynamic", state. */
    public function shovels(?string $vhost): array
    {
        $rows = [];
        foreach ($this->links as $link) {
            $d = $link->definition;
            if ($d['type'] !== 'shovel' || ($vhost !== null && $d['vhost'] !== $vhost)) continue;
            $state = $link->state();
            $rows[] = ['node' => $this->broker->nodeId, 'name' => $d['name'], 'vhost' => $d['vhost'], 'type' => 'dynamic',
                'state' => $state === 'retrying' ? 'starting' : ($state === 'error' ? 'terminated' : $state),
                'src_protocol' => $d['src-protocol'] ?? 'amqp091', 'dest_protocol' => $d['dest-protocol'] ?? 'amqp091',
                'src_uri' => self::redact(is_array($d['src-uri'] ?? null) ? (string) ($d['src-uri'][0] ?? '') : (string) ($d['src-uri'] ?? '')),
                'dest_uri' => self::redact(is_array($d['dest-uri'] ?? null) ? (string) ($d['dest-uri'][0] ?? '') : (string) ($d['dest-uri'] ?? '')),
                'reason' => $link->error === '' ? null : $link->error, 'messages_transferred' => $link->transferred, 'timestamp' => $link->started];
        }
        return $rows;
    }
    /** GET /api/federation-links rows. */
    public function federationLinks(?string $vhost): array
    {
        $rows = [];
        foreach ($this->links as $link) {
            $d = $link->definition;
            if ($d['type'] !== 'federation' || ($vhost !== null && $d['vhost'] !== $vhost)) continue;
            $state = $link->state();
            $queue = ($d['kind'] ?? 'exchange') === 'queue';
            $row = ['node' => $this->broker->nodeId, 'vhost' => $d['vhost'], 'id' => substr(hash('sha256', $d['name']), 0, 8), 'upstream' => $d['upstream'],
                'type' => $queue ? 'queue' : 'exchange', 'uri' => self::redact((string) $d['src-uri']),
                'status' => $state === 'running' ? 'running' : ($state === 'starting' ? 'starting' : 'error'), 'timestamp' => $link->started];
            if ($queue) { $row['queue'] = $d['dest-queue']; $row['upstream_queue'] = $d['src-queue']; }
            else { $row['exchange'] = $d['dest-exchange']; $row['upstream_exchange'] = $d['src-exchange']; }
            if ($link->error !== '') $row['error'] = $link->error;
            $rows[] = $row;
        }
        return $rows;
    }
    /** Upstreams a policy definition names: one upstream, a set, or "all". */
    private function upstreamsFor(string $vhost, array $definition): array
    {
        $upstreams = $this->broker->parameters['federation-upstream'][$vhost] ?? [];
        if (isset($definition['federation-upstream'])) return array_intersect_key($upstreams, [(string) $definition['federation-upstream'] => true]);
        if (!isset($definition['federation-upstream-set'])) return [];
        if ($definition['federation-upstream-set'] === 'all') return $upstreams;
        $set = $this->broker->parameters['federation-upstream-set'][$vhost][(string) $definition['federation-upstream-set']] ?? [];
        return array_intersect_key($upstreams, array_fill_keys(array_column(is_array($set) ? $set : [], 'upstream'), true));
    }
    private function scan(): array
    {
        $wanted = [];
        foreach ($this->broker->parameters['shovel'] ?? [] as $vhost => $rows) foreach ($rows as $name => $value) {
            if (!is_array($value)) continue;
            $wanted['shovel'."\0".$vhost."\0".$name] = ['type'=>'shovel','vhost'=>$vhost,'name'=>$name] + $value;
        }
        foreach ($this->broker->vhosts as $vhost) {
            $policies = $this->broker->policies[$vhost] ?? [];
            if ($policies === [] || ($this->broker->parameters['federation-upstream'][$vhost] ?? []) === []) continue;
            $scope = $this->broker->forVhost($vhost);
            foreach ($scope->exchanges as $exchange => $properties) {
                if ($exchange === '' || str_starts_with((string) $exchange, 'amq.')) continue;
                $definition = Policy::match($policies, (string) $exchange, 'exchanges')['definition'] ?? [];
                foreach ($this->upstreamsFor($vhost, is_array($definition) ? $definition : []) as $name => $value) {
                    $uri = is_array($value['uri'] ?? null) ? ($value['uri'][0] ?? '') : ($value['uri'] ?? '');
                    $wanted['federation'."\0".$vhost."\0".$name."\0".$exchange] = ['type'=>'federation','kind'=>'exchange','upstream'=>(string) $name,'vhost'=>$vhost,'name'=>$name.':'.$exchange,'src-uri'=>$uri,'dest-uri'=>'amqp://','src-queue'=>'qf-fed-'.substr(hash('sha256',$vhost."\0".$name."\0".$exchange),0,24),'dest-exchange'=>(string) $exchange,'src-exchange'=>(string) ($value['exchange'] ?? $exchange),'exchange-type'=>is_array($properties) ? ($properties['type'] ?? 'topic') : $properties,'max-hops'=>(int)($value['max-hops'] ?? 1),'queue-args'=>array_filter(['x-expires'=>is_int($value['expires'] ?? null) ? $value['expires'] : null,'x-message-ttl'=>is_int($value['message-ttl'] ?? null) ? $value['message-ttl'] : null], static fn ($v) => $v !== null)];
                }
            }
            foreach ($scope->queues as $queue => $row) {
                if (($row['args']['queueType'] ?? 'classic') === 'stream' || ($row['exclusive'] ?? false)) continue;
                $definition = Policy::match($policies, (string) $queue, 'queues')['definition'] ?? [];
                foreach ($this->upstreamsFor($vhost, is_array($definition) ? $definition : []) as $name => $value) {
                    $uri = is_array($value['uri'] ?? null) ? ($value['uri'][0] ?? '') : ($value['uri'] ?? '');
                    $wanted['federation-queue'."\0".$vhost."\0".$name."\0".$queue] = ['type'=>'federation','kind'=>'queue','upstream'=>(string) $name,'vhost'=>$vhost,'name'=>$name.':'.$queue,'src-uri'=>$uri,'dest-uri'=>'amqp://','src-queue'=>(string) ($value['queue'] ?? $queue),'dest-queue'=>(string) $queue,'max-hops'=>(int)($value['max-hops'] ?? 1)];
                }
            }
        }
        return $wanted;
    }
    public function tick(): void
    {
        $now = microtime(true);
        if ($now >= $this->nextScan || $this->scannedParameters !== $this->broker->parameters || $this->scannedPolicies !== $this->broker->policies) {
            $this->nextScan = $now + 0.25;
            $this->scannedParameters = $this->broker->parameters;
            $this->scannedPolicies = $this->broker->policies;
            $this->wanted = $this->scan();
            foreach ($this->links as $key => $link) if (!isset($this->wanted[$key]) || $this->wanted[$key] !== $link->definition) { $link->close(); unset($this->links[$key]); }
            foreach ($this->wanted as $key => $definition) {
                if (!isset($this->links[$key])) $this->links[$key] = new IntegrationLink($this->broker, $definition, $this->next--);
                $this->links[$key]->syncBindings();
            }
        }
        if ($this->links === []) return;
        foreach ($this->links as $link) $link->tick();
        foreach ($this->broker->vhosts as $vhost) { $scope=$this->broker->forVhost($vhost); $scope->pumpConsumers(); $scope->flushDurable(); }
    }
    public function close(): void { foreach ($this->links as $link) $link->close(); $this->links = []; }
}

/** One in-flight message per bridge also limits retry duplication after lost confirmations. */
final class IntegrationLink
{
    private ?Broker $source = null;
    private ?Broker $destination = null;
    private IntegrationPeer|Amqp10Peer|null $srcPeer = null;
    private IntegrationPeer|Amqp10Peer|null $destPeer = null;
    private bool $registered = false;
    private bool $busy = false;
    private bool $closed = false;
    private float $retry = 0;
    private string $tag;
    private string $srcQueue = '';
    private string $destQueue = '';
    private ?string $destExchange = null;
    private ?string $destKey = null;
    /** Next offset to read when the local source is a stream queue. */
    private ?int $streamCursor = null;
    /** @var list<string> binding keys currently on the internal federation queue */
    private ?array $boundKeys = null;
    public string $error = '';
    public int $transferred = 0;
    public string $started;
    public function __construct(private Broker $root, public readonly array $definition, private int $conn)
    {
        $this->tag = 'bridge-'.$conn;
        $this->started = gmdate('Y-m-d H:i:s');
        try {
            $this->srcQueue = (string) ($definition['src-queue'] ?? '');
            $this->destQueue = (string) ($definition['dest-queue'] ?? '');
            $this->destExchange = isset($definition['dest-exchange']) ? (string) $definition['dest-exchange'] : null;
            $this->destKey = isset($definition['dest-exchange-key']) ? (string) $definition['dest-exchange-key'] : null;
            foreach (['src','dest'] as $side) {
                $protocol = $definition[$side.'-protocol'] ?? 'amqp091';
                if (!in_array($protocol, ['amqp091', 'amqp10'], true)) throw new RuntimeException('Unsupported shovel protocol '.$protocol);
                $uri = $definition[$side.'-uri'] ?? 'amqp://';
                if (is_array($uri)) $uri = $uri[0] ?? '';
                if ($protocol === 'amqp10') $this->amqp10Address($side, (string) ($definition[$side.'-address'] ?? ''));
                $vhost = Integrations::localVhost((string)$uri, $definition['vhost']);
                if ($vhost !== null) {
                    if (!in_array($vhost, $root->vhosts, true)) throw new RuntimeException('Endpoint vhost does not exist');
                    if ($side === 'src') $this->source = $root->forVhost($vhost); else $this->destination = $root->forVhost($vhost);
                } elseif ($protocol === 'amqp10') {
                    $peer = new Amqp10Peer((string)$uri, $side === 'src' ? (string) $definition['src-address'] : (string) $definition['dest-address'], $side === 'dest', (int) ($definition['src-prefetch-count'] ?? 100));
                    if ($side === 'src') $this->srcPeer = $peer; else $this->destPeer = $peer;
                } else {
                    $consumerArgs = $side === 'src' && is_array($definition['src-consumer-args'] ?? null) ? $definition['src-consumer-args'] : null;
                    $peer = new IntegrationPeer((string)$uri, $side === 'src' ? $this->srcQueue : $this->destQueue, $side === 'dest', $side === 'src' && $definition['type'] === 'federation' && ($definition['kind'] ?? 'exchange') === 'exchange' ? $definition : null, $consumerArgs);
                    if ($side === 'src') $this->srcPeer = $peer; else $this->destPeer = $peer;
                }
            }
            if (($this->srcQueue === '' && $this->srcPeer === null) || ($this->destExchange === null && $this->destQueue === '' && !($this->destPeer instanceof Amqp10Peer))) throw new RuntimeException('Bridge requires source and destination resources');
            if ($this->source === $this->destination && $this->source !== null && $this->srcQueue === $this->destQueue && $this->destExchange === null) throw new RuntimeException('A shovel cannot consume its own destination');
            if ($definition['type'] === 'federation' && ($definition['kind'] ?? 'exchange') === 'exchange' && $this->source !== null) {
                if ($this->source === $this->destination && $definition['src-exchange'] === $definition['dest-exchange']) throw new RuntimeException('Federation source and destination are identical');
                $exchange = $definition['src-exchange'];
                $this->source->declareExchange($exchange, $definition['exchange-type']);
                // The upstream's expires and message-ttl apply to the link queue.
                $this->source->declareQueue($this->srcQueue, is_array($definition['queue-args'] ?? null) ? $definition['queue-args'] : []);
                $this->source->flushDurable();
                $this->syncBindings();
            }
        } catch (Throwable $e) { $this->error = $e->getMessage(); $this->closed = true; $this->srcPeer?->close(); $this->destPeer?->close(); }
    }
    /** An AMQP 1.0 address on a local endpoint names a queue (or, as a destination, an exchange and key). */
    private function amqp10Address(string $side, string $address): void
    {
        if ($address === '') throw new RuntimeException("$side-address is required for amqp10");
        $parsed = str_starts_with($address, '/') ? Amqp10::parseAddress($address) : ['queue' => $address];
        if ($parsed === null) throw new RuntimeException("invalid $side-address");
        if ($side === 'src') { if (!isset($parsed['queue'])) throw new RuntimeException('src-address must name a queue'); $this->srcQueue = $parsed['queue']; return; }
        if (isset($parsed['queue'])) { $this->destQueue = $parsed['queue']; $this->destExchange = null; }
        else { $this->destExchange = $parsed['exchange']; $this->destKey = $parsed['key']; }
    }
    /**
     * Keeps the internal queue of a local exchange-federation link bound with
     * the downstream exchange's binding keys, so only messages the downstream
     * side would route cross the link.
     */
    public function syncBindings(): void
    {
        if ($this->closed || $this->source === null || $this->destination === null || $this->definition['type'] !== 'federation' || ($this->definition['kind'] ?? 'exchange') !== 'exchange') return;
        $type = (string) $this->definition['exchange-type'];
        $down = (string) $this->definition['dest-exchange'];
        if ($type === 'topic' || $type === 'direct') {
            $keys = [];
            foreach ($this->destination->bindings as $row) if ($row['exchange'] === $down) $keys[$row['key']] = true;
            foreach ($this->destination->e2e as $row) if ($row['source'] === $down) $keys[$row['key']] = true;
            $keys = array_keys($keys); sort($keys);
        } else $keys = [$type === 'fanout' ? '' : '#'];
        $keys = array_map('strval', $keys);
        if ($keys === $this->boundKeys) return;
        $exchange = (string) $this->definition['src-exchange'];
        foreach ($this->boundKeys ?? [] as $key) if (!in_array($key, $keys, true)) $this->source->unbind($this->srcQueue, $exchange, $key);
        foreach ($keys as $key) if (!in_array($key, $this->boundKeys ?? [], true)) $this->source->bind($this->srcQueue, $exchange, $key);
        $this->boundKeys = $keys;
    }
    public function state(): string
    {
        if ($this->closed) return 'error';
        if ($this->error !== '') return 'retrying';
        return $this->destinationReady() && ($this->source !== null ? isset($this->source->queues[$this->srcQueue]) : $this->srcPeer?->ready()) ? 'running' : 'starting';
    }
    private function destinationReady(): bool
    {
        if ($this->destination !== null) return $this->destExchange !== null ? isset($this->destination->exchanges[$this->destExchange]) : isset($this->destination->queues[$this->destQueue]);
        return $this->destPeer?->ready() ?? false;
    }
    /**
     * A queue federation link only pulls while the downstream queue has a
     * consumer with spare credit and nothing ready, as RabbitMQ does.
     */
    private function demand(): bool
    {
        if (($this->definition['kind'] ?? '') !== 'queue') return true;
        $queue = $this->destination?->queues[$this->destQueue] ?? null;
        if ($queue === null || $queue['ready'] !== []) return false;
        foreach ($queue['consumers'] as $consumer) {
            if ($consumer['conn'] === $this->conn) continue;
            if (!isset($consumer['readyFn']) || ($consumer['readyFn'])()) return true;
        }
        return false;
    }
    public function tick(): void
    {
        if ($this->closed) return;
        $this->srcPeer?->tick(); $this->destPeer?->tick();
        if ($this->srcPeer?->error) $this->error = $this->srcPeer->error;
        elseif ($this->destPeer?->error) $this->error = $this->destPeer->error;
        if ($this->source !== null) {
            $queue = $this->srcQueue;
            if (($this->source->queues[$queue]['args']['queueType'] ?? '') === 'stream') { $this->tickStream($queue); return; }
            if ($this->registered) {
                $present=false; foreach($this->source->queues[$queue]['consumers'] ?? [] as $consumer) if($consumer['conn']===$this->conn && $consumer['tag']===$this->tag) $present=true;
                if(!$present) $this->registered=false;
            }
            if (!$this->registered && isset($this->source->queues[$queue])) {
                try {
                    $this->source->registerProtocolConsumer($queue, $this->conn, $this->tag,
                        fn()=>!$this->closed && !$this->busy && microtime(true)>=$this->retry && $this->destinationReady() && $this->demand(),
                        function(array $message, int $id):void { $this->move($message, function(bool $ok)use($id):void { if ($ok) {$this->source->ack($id);} else $this->source->requeue($id); }); });
                    $this->registered = true;
                } catch (Throwable $e) { $this->error = $e->getMessage(); }
            }
        } elseif (!$this->busy && microtime(true)>=$this->retry && $this->destinationReady() && $this->srcPeer?->ready() && $this->demand()) {
            $this->srcPeer->get(function(?array $message):void { if ($message !== null) $this->move($message, fn(bool $ok)=>$this->srcPeer->settle($message['tag'],$ok)); });
        }
    }
    /** A local stream source is read from an offset (src-consumer-args x-stream-offset, default next). */
    private function tickStream(string $queue): void
    {
        if ($this->streamCursor === null) {
            $offset = is_array($this->definition['src-consumer-args'] ?? null) ? ($this->definition['src-consumer-args']['x-stream-offset'] ?? 'next') : 'next';
            $first = $this->source->streamFirst($queue); $next = $this->source->streamNext($queue);
            $this->streamCursor = is_int($offset) ? max($first, $offset) : match ($offset) { 'first' => $first, 'last' => max($first, $next - 1), default => $next };
        }
        for ($budget = 64; $budget > 0 && !$this->busy && microtime(true) >= $this->retry && $this->destinationReady(); $budget--) {
            $records = $this->source->streamRead($queue, $this->streamCursor, 1);
            if ($records === []) return;
            $record = $records[0]; $offset = (int) $record['offset'];
            $this->move(['body' => $record['body'], 'key' => $record['key'] ?? $queue, 'exchange' => $record['exchange'] ?? '', 'headers' => $record['headers'] ?? [], 'propRaw' => $record['propRaw'] ?? null, 'mode' => 2],
                function (bool $ok) use ($offset): void { if ($ok) $this->streamCursor = max((int) $this->streamCursor, $offset + 1); });
        }
    }
    private function move(array $message, callable $settle): void
    {
        $this->busy = true;
        $done = function(bool $ok)use($settle):void {
            $settle($ok); $this->busy = false;
            if ($ok) { $this->transferred++; $this->error = ''; }
            else { $this->error = 'Destination publish failed; source requeued'; $this->retry = microtime(true)+1; }
        };
        $headers = $message['headers'] ?? [];
        $raw = $message['propRaw'] ?? null;
        if ($this->definition['type'] === 'federation') {
            $hops = 0; $received = [];
            foreach ($headers as [$key,$value]) {
                if ($key === 'x-qf-federation-hops') $hops = max($hops, (int)$value);
                if ($key === 'x-received-from' && is_array($value)) { $received = array_values($value); $hops = max($hops, count($received)); }
            }
            if ($hops >= $this->definition['max-hops']) { $done(true); return; }
            $headers = array_values(array_filter($headers, static fn($pair)=>$pair[0] !== 'x-qf-federation-hops' && $pair[0] !== 'x-received-from'));
            $received[] = ['uri' => Integrations::redact((string) $this->definition['src-uri']), (($this->definition['kind'] ?? 'exchange') === 'queue' ? 'queue' : 'exchange') => (string) ($this->definition['src-exchange'] ?? $this->srcQueue), 'redelivered' => false];
            $headers[] = ['x-qf-federation-hops',$hops+1];
            $headers[] = ['x-received-from',$received];
            $props = Amqp10::readProps($raw); $raw = Amqp10::writeProps($props,$headers);
        }
        $exchange = $this->destExchange ?? '';
        $key = $this->destExchange !== null ? ($this->destKey ?? $message['key'] ?? '') : $this->destQueue;
        if ($this->destination !== null) {
            // Never interpret an unroutable shovel publish as successful acceptance.
            if (!$this->destinationReady()) { $done(false); return; }
            $this->destination->publishAsync($this->conn,0,$exchange,$key,$message['body'],$message['mode'] ?? 2,$message['priority'] ?? 0,$headers,null,$raw,$done);
        } else $this->destPeer->publish($exchange,$key,$message,$headers,$raw,$done);
    }
    public function close(): void
    {
        $this->closed = true;
        if ($this->registered) $this->source->unregisterProtocolConsumer($this->srcQueue,$this->conn,$this->tag);
        $this->srcPeer?->close(); $this->destPeer?->close();
    }
}

/** Small nonblocking AMQP 0-9-1 endpoint. No socket call waits for a broker reply. */
final class IntegrationPeer
{
    private $socket = null;
    private array $uri;
    private string $input = '';
    private string $output = '';
    private string $phase = 'disconnected';
    private float $retry = 0;
    private float $seen = 0;
    private float $beat = 0;
    private int $heartbeat = 0;
    private int $frameMax = 131072;
    private int $publishTag = 0;
    private $confirm = null;
    private $getter = null;
    private ?array $incoming = null;
    /** @var list<array<string, mixed>> deliveries from basic.consume not handed out yet */
    private array $buffered = [];
    private bool $returned = false;
    private bool $stopped = false;
    public string $error = '';
    /**
     * @param ?array<string, mixed> $consumerArgs basic.consume arguments; set
     * for a stream source (x-stream-offset), which cannot be read with basic.get
     */
    public function __construct(string $uri, private string $queue, private bool $publisher, private ?array $federation = null, private ?array $consumerArgs = null)
    {
        $parts = parse_url($uri);
        if (!is_array($parts) || !in_array($parts['scheme'] ?? '',['amqp','amqps'],true) || empty($parts['host'])) throw new RuntimeException('Invalid AMQP endpoint URI');
        $this->uri = $parts;
    }
    public function ready(): bool { return $this->phase === 'ready'; }
    private function method(int $class,int $method,string $args='',int $channel=1):void { $this->output .= Codec::method($channel,$class,$method,$args); }
    public function tick(): void
    {
        if ($this->stopped) return;
        try {
            if (!is_resource($this->socket)) {
                if (microtime(true)<$this->retry) return;
                $host = trim($this->uri['host'],'[]'); $port = $this->uri['port'] ?? ($this->uri['scheme']==='amqps'?5671:5672);
                $context = stream_context_create(['ssl'=>['verify_peer'=>true,'verify_peer_name'=>true,'peer_name'=>$host]]);
                $this->socket = @stream_socket_client('tcp://'.(str_contains($host,':')?'['.$host.']':$host).':'.$port,$errno,$error,0,STREAM_CLIENT_CONNECT|STREAM_CLIENT_ASYNC_CONNECT,$context);
                if (!is_resource($this->socket)) throw new RuntimeException('AMQP endpoint connection failed');
                stream_set_blocking($this->socket,false); $this->phase='connecting';$this->seen=$this->beat=microtime(true);
            }
            if ($this->phase==='connecting') {
                $read=[];$write=[$this->socket];$except=[];
                $ready=@stream_select($read,$write,$except,0,0);
                // Past FD_SETSIZE select fails; a peer name means connected.
                if ($ready===false) $ready=@stream_socket_get_name($this->socket,true)===false?0:1;
                if ($ready!==1) { if(microtime(true)-$this->seen>10)throw new RuntimeException('AMQP connect timed out');return; }
                $this->phase=$this->uri['scheme']==='amqps'?'tls':'start';
                if($this->phase==='start')$this->output="AMQP\0\0\x09\x01";
            }
            if ($this->phase==='tls') {
                $crypto=@stream_socket_enable_crypto($this->socket,true,STREAM_CRYPTO_METHOD_TLS_CLIENT);
                if ($crypto===false)throw new RuntimeException('AMQP TLS negotiation failed');
                if($crypto===0)return;$this->phase='start';$this->output="AMQP\0\0\x09\x01";
            }
            if ($this->output!=='') {$n=@fwrite($this->socket,$this->output);if($n===false)throw new RuntimeException('AMQP write failed');$this->output=substr($this->output,$n);}
            $bytes=@fread($this->socket,65536);if($bytes===false||feof($this->socket))throw new RuntimeException('AMQP endpoint disconnected');if($bytes!==''){$this->input.=$bytes;$this->seen=microtime(true);}
            while(strlen($this->input)>=7){$length=unpack('N',substr($this->input,3,4))[1];if($length>16777216)throw new RuntimeException('AMQP frame too large');if(strlen($this->input)<8+$length)break;$type=ord($this->input[0]);$payload=substr($this->input,7,$length);if($this->input[7+$length]!=="\xce")throw new RuntimeException('Invalid AMQP frame');$this->input=substr($this->input,8+$length);$this->frame($type,$payload);}
            $now=microtime(true);if($this->heartbeat&&$now-$this->seen>$this->heartbeat*2)throw new RuntimeException('AMQP heartbeat timed out');if($this->heartbeat&&$now-$this->beat>=$this->heartbeat/2){$this->output.=Codec::heartbeat();$this->beat=$now;}
            if (!$this->ready() && $now-$this->seen>15) throw new RuntimeException('AMQP handshake timed out');
            if ($this->getter !== null && $this->buffered !== []) { $getter=$this->getter;$this->getter=null;$getter(array_shift($this->buffered)); }
        } catch (Throwable $e) {$this->fail($e->getMessage());}
    }
    private function frame(int $type,string $payload):void
    {
        if($type===8)return;
        if($type===2){if($this->incoming===null)throw new RuntimeException('Unexpected AMQP content');$this->incoming['length']=Codec::readU64($payload,4);$raw=substr($payload,12);$props=Amqp10::readProps($raw);$this->incoming['propRaw']=$raw;$this->incoming['headers']=[];foreach($props['headers']??[]as$key=>$value)$this->incoming['headers'][]=[$key,$value];$this->incoming['mode']=$props['deliveryMode']??2;$this->incoming['priority']=$props['priority']??0;if($this->incoming['length']===0)$this->contentDone();return;}
        if($type===3){if($this->incoming===null)throw new RuntimeException('Unexpected AMQP body');$this->incoming['body'].=$payload;if(strlen($this->incoming['body'])===$this->incoming['length'])$this->contentDone();return;}
        if($type!==1||strlen($payload)<4)return;
        [$class,$method]=array_values(unpack('nclass/nmethod',substr($payload,0,4)));$p=substr($payload,4);
        if($class===10&&$method===10){$user=rawurldecode($this->uri['user']??'guest');$pass=rawurldecode($this->uri['pass']??'guest');$this->method(10,11,pack('N',0).Codec::shortstr('PLAIN').Codec::longstr("\0".$user."\0".$pass).Codec::shortstr('en_US'),0);return;}
        if($class===10&&$method===30){$v=unpack('nchannels/Nframe/nheartbeat',$p);$this->frameMax=$v['frame']?:131072;$this->heartbeat=$v['heartbeat'];$this->method(10,31,pack('nNn',0,$this->frameMax,$this->heartbeat),0);$vhost=isset($this->uri['path'])&&$this->uri['path']!=='/'?rawurldecode(substr($this->uri['path'],1)):'/';$this->method(10,40,Codec::shortstr($vhost)."\0\0",0);return;}
        if($class===10&&$method===41){$this->method(20,10,"\0");return;}
        if($class===20&&$method===11){if($this->federation!==null){$this->method(40,10,pack('n',0).Codec::shortstr($this->federation['src-exchange']).Codec::shortstr($this->federation['exchange-type']).chr(2).pack('N',0));}else $this->declareQueue();return;}
        if($class===40&&$method===11){$this->declareQueue();return;}
        if($class===50&&$method===11){if($this->federation!==null)$this->method(50,20,pack('n',0).Codec::shortstr($this->queue).Codec::shortstr($this->federation['src-exchange']).Codec::shortstr($this->federation['exchange-type']==='topic'?'#':'')."\0".pack('N',0));elseif($this->consumerArgs!==null){$this->method(60,10,pack('NnC',0,100,0));}else $this->finishHandshake();return;}
        if($class===50&&$method===21){$this->finishHandshake();return;}
        if($class===60&&$method===11){$args=[];foreach($this->consumerArgs??[] as $k=>$v)$args[]=[(string)$k,$v];$this->method(60,20,pack('n',0).Codec::shortstr($this->queue).Codec::shortstr('qf-shovel')."\0".Codec::writeTable($args));return;}
        if($class===60&&$method===21){$this->phase='ready';$this->error='';return;}
        if($class===85&&$method===11){$this->phase='ready';$this->error='';return;}
        if(($class===10&&$method===50)||($class===20&&$method===40))throw new RuntimeException('AMQP peer closed: '.bin2hex(substr($p,0,2)).' '.substr($p,3,ord($p[2]??"\0")));
        if($class===60&&$method===60){$o=0;Codec::readShortstr($p,$o);$tag=Codec::readU64($p,$o);$o+=8;$redelivered=ord($p[$o++])!==0;$exchange=Codec::readShortstr($p,$o);$key=Codec::readShortstr($p,$o);$this->incoming=['tag'=>$tag,'exchange'=>$exchange,'key'=>$key,'body'=>'','consumed'=>true];return;}
        if($class===60&&$method===71){$o=8;$redelivered=ord($p[$o++])!==0;$exchange=Codec::readShortstr($p,$o);$key=Codec::readShortstr($p,$o);$this->incoming=['tag'=>Codec::readU64($p,0),'exchange'=>$exchange,'key'=>$key,'body'=>''];return;}
        if($class===60&&$method===72){$getter=$this->getter;$this->getter=null;if($getter)$getter(null);return;}
        if($class===60&&$method===50){$this->returned=true;$this->incoming=['return'=>true,'body'=>''];return;}
        if($class===60&&($method===80||$method===120)&&$this->confirm!==null){$tag=Codec::readU64($p,0);$multiple=(ord($p[8]??"\0")&1)!==0;if($tag===$this->publishTag||($multiple&&$tag>=$this->publishTag)){$done=$this->confirm;$this->confirm=null;$done($method===80&&!$this->returned);}return;}
    }
    private function declareQueue():void {if($this->publisher&&$this->queue===''){$this->finishHandshake();return;}$this->method(50,10,pack('n',0).Codec::shortstr($this->queue).chr($this->consumerArgs!==null?1:2).pack('N',0));}
    private function finishHandshake():void {if($this->publisher)$this->method(85,10,"\0");else{$this->phase='ready';$this->error='';}}
    private function contentDone():void {$message=$this->incoming;$this->incoming=null;if(isset($message['return']))return;if($message['consumed']??false){$this->buffered[]=$message;return;}$getter=$this->getter;$this->getter=null;if($getter)$getter($message);}
    public function get(callable $done):void {if(!$this->ready()||$this->getter!==null)return;if($this->consumerArgs!==null){if($this->buffered!==[])$done(array_shift($this->buffered));else $this->getter=$done;return;}$this->getter=$done;$this->method(60,70,pack('n',0).Codec::shortstr($this->queue)."\0");}
    public function settle(int $tag,bool $ok):void {if($this->ready())$this->method(60,$ok?80:120,Codec::u64($tag).chr($ok?0:2));}
    public function publish(string $exchange,string $key,array $message,array $headers,?string $raw,callable $done):void
    {
        if(!$this->ready()||$this->confirm!==null){$done(false);return;}$this->confirm=$done;$this->publishTag++;$this->returned=false;
        $this->method(60,40,pack('n',0).Codec::shortstr($exchange).Codec::shortstr($key).chr(1));
        $this->output.=Codec::frame(2,1,Codec::contentHeader(strlen($message['body']),$message['mode']??2,$raw,$headers));
        $max=max(1,$this->frameMax-8);for($at=0;$at<strlen($message['body']);$at+=$max)$this->output.=Codec::frame(3,1,substr($message['body'],$at,$max));
    }
    private function fail(string $error):void
    {
        if(is_resource($this->socket))fclose($this->socket);$this->socket=null;$this->phase='disconnected';$this->retry=microtime(true)+1;$this->error=$error;$this->input=$this->output='';$this->incoming=null;$this->getter=null;$this->buffered=[];$this->publishTag=0;$done=$this->confirm;$this->confirm=null;if($done)$done(false);
    }
    public function close():void {$this->stopped=true;$this->fail('Bridge stopped');}
}

/**
 * Small nonblocking AMQP 1.0 client endpoint for shovels: SASL PLAIN (or
 * ANONYMOUS without credentials), one session, one link. As a source it is a
 * receiver granting credit and settling each transfer after the destination
 * confirms; as a destination it is an unsettled sender waiting for the
 * disposition.
 */
final class Amqp10Peer
{
    private $socket = null;
    private array $uri;
    private string $input = '';
    private string $output = '';
    private string $phase = 'disconnected';
    private float $retry = 0;
    private float $seen = 0;
    private int $credit = 0;
    private int $deliveryId = 0;
    private int $deliveryCount = 0;
    private int $nextIncoming = 0;
    private $confirm = null;
    private ?int $confirmId = null;
    private $getter = null;
    /** @var list<array<string, mixed>> */
    private array $buffered = [];
    private ?array $partial = null;
    private bool $stopped = false;
    public string $error = '';
    public function __construct(string $uri, private string $address, private bool $sender, private int $prefetch = 100)
    {
        $parts = parse_url($uri);
        if (!is_array($parts) || !in_array($parts['scheme'] ?? '', ['amqp', 'amqps'], true) || empty($parts['host'])) throw new RuntimeException('Invalid AMQP 1.0 endpoint URI');
        if ($this->address === '') throw new RuntimeException('AMQP 1.0 address required');
        $this->uri = $parts;
        $this->prefetch = max(1, min(1000, $prefetch));
    }
    public function ready(): bool { return $this->phase === 'ready' && (!$this->sender || $this->credit > 0 || $this->confirm !== null); }
    private static function u(int $n): Amqp10Value { return Amqp10Codec::uint($n); }
    private static function perf(int $code, array $fields): string { while ($fields !== [] && end($fields) === null) array_pop($fields); return Amqp10Codec::encode(new Amqp10Described($code, $fields)); }
    private function send(int $code, array $fields, int $type = 0, string $payload = ''): void { $body = self::perf($code, $fields) . $payload; $this->output .= pack('NCCn', strlen($body) + 8, 2, $type, 0) . $body; }
    public function tick(): void
    {
        if ($this->stopped) return;
        try {
            if (!is_resource($this->socket)) {
                if (microtime(true) < $this->retry) return;
                $host = trim($this->uri['host'], '[]'); $port = $this->uri['port'] ?? ($this->uri['scheme'] === 'amqps' ? 5671 : 5672);
                $context = stream_context_create(['ssl' => ['verify_peer' => true, 'verify_peer_name' => true, 'peer_name' => $host]]);
                $this->socket = @stream_socket_client('tcp://' . (str_contains($host, ':') ? '[' . $host . ']' : $host) . ':' . $port, $errno, $error, 0, STREAM_CLIENT_CONNECT | STREAM_CLIENT_ASYNC_CONNECT, $context);
                if (!is_resource($this->socket)) throw new RuntimeException('AMQP 1.0 endpoint connection failed');
                stream_set_blocking($this->socket, false); $this->phase = 'connecting'; $this->seen = microtime(true);
            }
            if ($this->phase === 'connecting') {
                $read = []; $write = [$this->socket]; $except = [];
                $ready = @stream_select($read, $write, $except, 0, 0);
                if ($ready === false) $ready = @stream_socket_get_name($this->socket, true) === false ? 0 : 1;
                if ($ready !== 1) { if (microtime(true) - $this->seen > 10) throw new RuntimeException('AMQP 1.0 connect timed out'); return; }
                $this->phase = $this->uri['scheme'] === 'amqps' ? 'tls' : 'sasl-header';
                if ($this->phase === 'sasl-header') $this->output = "AMQP\x03\x01\x00\x00";
            }
            if ($this->phase === 'tls') {
                $crypto = @stream_socket_enable_crypto($this->socket, true, STREAM_CRYPTO_METHOD_TLS_CLIENT);
                if ($crypto === false) throw new RuntimeException('AMQP 1.0 TLS negotiation failed');
                if ($crypto === 0) return; $this->phase = 'sasl-header'; $this->output = "AMQP\x03\x01\x00\x00";
            }
            if ($this->output !== '') { $n = @fwrite($this->socket, $this->output); if ($n === false) throw new RuntimeException('AMQP 1.0 write failed'); $this->output = substr($this->output, $n); }
            $bytes = @fread($this->socket, 65536); if ($bytes === false || feof($this->socket)) throw new RuntimeException('AMQP 1.0 endpoint disconnected');
            if ($bytes !== '') { $this->input .= $bytes; $this->seen = microtime(true); }
            for (;;) {
                if (in_array($this->phase, ['sasl-header', 'amqp-header'], true)) {
                    if (strlen($this->input) < 8) break;
                    $header = substr($this->input, 0, 8); $this->input = substr($this->input, 8);
                    $want = $this->phase === 'sasl-header' ? "AMQP\x03\x01\x00\x00" : "AMQP\x00\x01\x00\x00";
                    if ($header !== $want) throw new RuntimeException('AMQP 1.0 protocol header mismatch');
                    $this->phase = $this->phase === 'sasl-header' ? 'sasl' : 'opening';
                    continue;
                }
                if (strlen($this->input) < 8) break;
                $size = unpack('N', substr($this->input, 0, 4))[1]; $doff = ord($this->input[4]) * 4;
                if ($size < 8 || $size > 16777216 || $doff < 8 || $doff > $size) throw new RuntimeException('AMQP 1.0 framing error');
                if (strlen($this->input) < $size) break;
                $body = substr($this->input, $doff, $size - $doff); $this->input = substr($this->input, $size);
                if ($body === '') continue;
                $decoder = new Amqp10Codec($body); $perf = $decoder->value();
                if (!$perf instanceof Amqp10Described || !is_int($perf->descriptor)) throw new RuntimeException('AMQP 1.0 expected performative');
                $this->onFrame($perf->descriptor, is_array($perf->value) ? $perf->value : [], substr($body, $decoder->at));
            }
            if ($this->getter !== null && $this->buffered !== []) { $getter = $this->getter; $this->getter = null; $getter(array_shift($this->buffered)); }
            if ($this->phase !== 'ready' && microtime(true) - $this->seen > 15) throw new RuntimeException('AMQP 1.0 handshake timed out');
        } catch (Throwable $e) { $this->fail($e->getMessage()); }
    }
    private function onFrame(int $code, array $f, string $payload): void
    {
        if ($code === 0x40) {
            $offered = array_map(static fn ($m) => Amqp10Codec::scalar($m), (array) Amqp10Codec::scalar($f[0] ?? []));
            $user = isset($this->uri['user']) ? rawurldecode($this->uri['user']) : null;
            if ($user !== null && in_array('PLAIN', $offered, true)) $this->send(0x41, [Amqp10Codec::symbol('PLAIN'), Amqp10Codec::binary("\0" . $user . "\0" . rawurldecode($this->uri['pass'] ?? ''))], 1);
            elseif (in_array('ANONYMOUS', $offered, true)) $this->send(0x41, [Amqp10Codec::symbol('ANONYMOUS')], 1);
            else throw new RuntimeException('AMQP 1.0 peer offers no usable SASL mechanism');
            return;
        }
        if ($code === 0x44) {
            if ((int) Amqp10Codec::scalar($f[0] ?? 1) !== 0) throw new RuntimeException('AMQP 1.0 SASL authentication failed');
            $this->phase = 'amqp-header'; $this->output .= "AMQP\x00\x01\x00\x00";
            $vhost = isset($this->uri['path']) && $this->uri['path'] !== '/' && $this->uri['path'] !== '' ? rawurldecode(substr($this->uri['path'], 1)) : '/';
            $this->send(0x10, ['queueforge-shovel-' . bin2hex(random_bytes(4)), 'vhost:' . $vhost, self::u(131072), new Amqp10Value('ushort', 1)]);
            $this->send(0x11, [null, self::u(0), self::u(5000), self::u(5000)]);
            $endpoint = new Amqp10Described(0x28, [$this->address]);
            $target = new Amqp10Described(0x29, [$this->address]);
            // role false = sender, true = receiver; snd-settle-mode unsettled, rcv-settle-mode first.
            $this->send(0x12, ['queueforge-shovel', self::u(0), !$this->sender, new Amqp10Value('ubyte', 0), new Amqp10Value('ubyte', 0), $this->sender ? null : $endpoint, $this->sender ? $target : null, null, null, $this->sender ? self::u(0) : null]);
            return;
        }
        if ($code === 0x10 || $code === 0x11) return;
        if ($code === 0x12) {
            if ($this->sender && ($f[6] ?? null) === null) return; // refused attach; a detach follows
            $this->phase = 'ready'; $this->error = '';
            if (!$this->sender) $this->grant();
            return;
        }
        if ($code === 0x13) {
            if ($this->sender && isset($f[4])) $this->credit = max(0, (int) Amqp10Codec::scalar($f[5] ?? 0) + (int) Amqp10Codec::scalar($f[6] ?? 0) - $this->deliveryCount);
            return;
        }
        if ($code === 0x14 && !$this->sender) {
            if ($this->partial === null) $this->partial = ['tag' => (int) Amqp10Codec::scalar($f[1] ?? 0), 'body' => '', 'settled' => ($f[4] ?? false) === true];
            $this->partial['body'] .= $payload;
            if (($f[5] ?? false) === true) return;
            $done = $this->partial; $this->partial = null; $this->nextIncoming++; $this->deliveryCount++; $this->credit--;
            $msg = Amqp10::inbound($done['body']);
            $headers = $msg['headers'];
            $this->buffered[] = ['tag' => $done['tag'], 'body' => $msg['body'], 'mode' => $msg['mode'], 'priority' => $msg['priority'], 'headers' => $headers, 'propRaw' => Amqp10::writeProps($msg['props'], $msg['typedHeaders'] === [] ? $headers : array_map(static fn ($p) => [$p[0], Amqp10Codec::scalar($p[1])], $msg['typedHeaders'])), 'key' => $msg['subject'] ?? $this->address, 'exchange' => ''];
            if ($this->credit < intdiv($this->prefetch, 2)) $this->grant();
            return;
        }
        if ($code === 0x15 && $this->sender) {
            $first = (int) Amqp10Codec::scalar($f[1] ?? -1); $last = isset($f[2]) ? (int) Amqp10Codec::scalar($f[2]) : $first;
            if ($this->confirm === null || $this->confirmId === null || $this->confirmId < $first || $this->confirmId > $last) return;
            $state = $f[4] ?? null; $ok = $state instanceof Amqp10Described && $state->descriptor === 0x24;
            $done = $this->confirm; $this->confirm = null; $this->confirmId = null; $done($ok);
            return;
        }
        if ($code === 0x16 || $code === 0x18 || $code === 0x17) {
            $error = $code === 0x16 ? ($f[2] ?? null) : ($code === 0x17 ? ($f[0] ?? null) : ($f[0] ?? null));
            $text = $error instanceof Amqp10Described && is_array($error->value) ? (string) Amqp10Codec::scalar($error->value[0] ?? '') . ' ' . (string) ($error->value[1] ?? '') : 'closed';
            throw new RuntimeException('AMQP 1.0 peer closed: ' . trim($text));
        }
    }
    private function grant(): void
    {
        $this->credit = $this->prefetch;
        $this->send(0x13, [self::u($this->nextIncoming), self::u(5000), self::u(0), self::u(5000), self::u(0), self::u($this->deliveryCount), self::u($this->credit)]);
    }
    public function get(callable $done): void { if ($this->phase !== 'ready' || $this->getter !== null) return; if ($this->buffered !== []) $done(array_shift($this->buffered)); else $this->getter = $done; }
    public function settle(int $tag, bool $ok): void
    {
        if ($this->phase !== 'ready') return;
        // accepted, or released so the source broker redelivers.
        $this->send(0x15, [true, self::u($tag), null, true, new Amqp10Described($ok ? 0x24 : 0x26, [])]);
    }
    public function publish(string $exchange, string $key, array $message, array $headers, ?string $raw, callable $done): void
    {
        if ($this->phase !== 'ready' || $this->confirm !== null || $this->credit <= 0) { $done(false); return; }
        $this->confirm = $done; $this->confirmId = $this->deliveryId; $this->credit--; $this->deliveryCount++;
        $payload = Amqp10::outbound(['propRaw' => $raw ?? Amqp10::writeProps(['deliveryMode' => $message['mode'] ?? 2], $headers), 'headers' => $headers, 'mode' => $message['mode'] ?? 2, 'exchange' => $exchange, 'key' => $key, 'body' => $message['body']]);
        $this->send(0x14, [self::u(0), self::u($this->deliveryId), Amqp10Codec::binary(pack('N', $this->deliveryId)), self::u(0), false], 0, $payload);
        $this->deliveryId++;
    }
    private function fail(string $error): void
    {
        if (is_resource($this->socket)) fclose($this->socket);
        $this->socket = null; $this->phase = 'disconnected'; $this->retry = microtime(true) + 1; $this->error = $error; $this->input = $this->output = '';
        $this->partial = null; $this->getter = null; $this->buffered = []; $this->credit = 0; $this->deliveryCount = 0; $this->nextIncoming = 0; $this->deliveryId = 0;
        $done = $this->confirm; $this->confirm = null; $this->confirmId = null; if ($done) $done(false);
    }
    public function close(): void { $this->stopped = true; $this->fail('Bridge stopped'); }
}
