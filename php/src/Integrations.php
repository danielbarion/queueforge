<?php
declare(strict_types=1);

/** Dynamic bridges. A source delivery remains unacknowledged until a destination confirm. */
final class Integrations
{
    private array $links = [];
    private int $next = -3000000;
    public function __construct(private Broker $broker) {}

    public static function localVhost(string $uri, string $fallback): ?string
    {
        if (!preg_match('~^amqps?://(?:/(.*))?$~', trim($uri), $m)) return null;
        return isset($m[1]) && $m[1] !== '' ? rawurldecode($m[1]) : $fallback;
    }
    /** The status is operational; unsupported or disconnected endpoints never appear as running. */
    public function status(): array
    {
        $rows = [];
        foreach ($this->links as $key => $link) $rows[] = ['name'=>$link->definition['name'],'vhost'=>$link->definition['vhost'],'type'=>$link->definition['type'],'state'=>$link->state(),'error'=>$link->error,'transferred'=>$link->transferred];
        return $rows;
    }
    public function tick(): void
    {
        $wanted = [];
        foreach ($this->broker->parameters['shovel'] ?? [] as $vhost => $rows) foreach ($rows as $name => $value) {
            $def = ['type'=>'shovel','vhost'=>$vhost,'name'=>$name] + $value;
            $wanted['shovel'."\0".$vhost."\0".$name] = $def;
        }
        foreach ($this->broker->vhosts as $vhost) {
            $scope = $this->broker->forVhost($vhost);
            foreach ($scope->exchanges as $exchange => $properties) {
                $policy = Policy::match($this->broker->policies[$vhost] ?? [], $exchange, 'exchanges');
                $definition = $policy['definition'] ?? [];
                $upstreams = $this->broker->parameters['federation-upstream'][$vhost] ?? [];
                if (isset($definition['federation-upstream'])) $upstreams = array_intersect_key($upstreams, [(string)$definition['federation-upstream']=>true]);
                elseif (($definition['federation-upstream-set'] ?? '') !== 'all') {
                    $set = $this->broker->parameters['federation-upstream-set'][$vhost][$definition['federation-upstream-set'] ?? ''] ?? [];
                    $upstreams = array_intersect_key($upstreams, array_fill_keys(array_column($set, 'upstream'), true));
                }
                foreach ($upstreams as $name => $value) {
                    $uri = is_array($value['uri'] ?? null) ? ($value['uri'][0] ?? '') : ($value['uri'] ?? '');
                    $def = ['type'=>'federation','vhost'=>$vhost,'name'=>$name.':'.$exchange,'src-uri'=>$uri,'dest-uri'=>'amqp://','src-queue'=>'qf-fed-'.substr(hash('sha256',$vhost."\0".$name."\0".$exchange),0,24),'dest-exchange'=>$exchange,'src-exchange'=>$value['exchange'] ?? $exchange,'exchange-type'=>is_array($properties) ? ($properties['type'] ?? 'topic') : $properties,'max-hops'=>(int)($value['max-hops'] ?? 1)];
                    $wanted['federation'."\0".$vhost."\0".$name."\0".$exchange] = $def;
                }
            }
        }
        foreach ($this->links as $key => $link) if (!isset($wanted[$key]) || $wanted[$key] !== $link->definition) { $link->close(); unset($this->links[$key]); }
        foreach ($wanted as $key => $definition) {
            if (!isset($this->links[$key])) $this->links[$key] = new IntegrationLink($this->broker, $definition, $this->next--);
            $this->links[$key]->tick();
        }
        foreach ($this->broker->vhosts as $vhost) { $scope=$this->broker->forVhost($vhost); $scope->pumpConsumers(); $scope->flushDurable(); }
    }
    public function close(): void { foreach ($this->links as $link) $link->close(); $this->links = []; }
}

/** One in-flight message per bridge also limits retry duplication after lost confirmations. */
final class IntegrationLink
{
    private ?Broker $source = null;
    private ?Broker $destination = null;
    private ?IntegrationPeer $srcPeer = null;
    private ?IntegrationPeer $destPeer = null;
    private bool $registered = false;
    private bool $busy = false;
    private bool $closed = false;
    private float $retry = 0;
    private string $tag;
    public string $error = '';
    public int $transferred = 0;
    public function __construct(private Broker $root, public readonly array $definition, private int $conn)
    {
        $this->tag = 'bridge-'.$conn;
        try {
            foreach (['src','dest'] as $side) {
                $protocol = $definition[$side.'-protocol'] ?? 'amqp091';
                if ($protocol !== 'amqp091') throw new RuntimeException('Only amqp091 bridge endpoints are supported');
                $uri = $definition[$side.'-uri'] ?? 'amqp://';
                if (is_array($uri)) $uri = $uri[0] ?? '';
                $vhost = Integrations::localVhost((string)$uri, $definition['vhost']);
                if ($vhost !== null) {
                    if (!in_array($vhost, $root->vhosts, true)) throw new RuntimeException('Endpoint vhost does not exist');
                    if ($side === 'src') $this->source = $root->forVhost($vhost); else $this->destination = $root->forVhost($vhost);
                } else {
                    $peer = new IntegrationPeer((string)$uri, (string)($definition[$side.'-queue'] ?? ''), $side === 'dest', $side === 'src' && $definition['type'] === 'federation' ? $definition : null);
                    if ($side === 'src') $this->srcPeer = $peer; else $this->destPeer = $peer;
                }
            }
            if (empty($definition['src-queue']) || (!isset($definition['dest-exchange']) && empty($definition['dest-queue']))) throw new RuntimeException('Bridge requires source and destination resources');
            if ($this->source === $this->destination && $this->source !== null && ($definition['src-queue'] ?? '') === ($definition['dest-queue'] ?? '')) throw new RuntimeException('A shovel cannot consume its own destination');
            if ($definition['type'] === 'federation' && $this->source !== null) {
                if ($this->source === $this->destination && $definition['src-exchange'] === $definition['dest-exchange']) throw new RuntimeException('Federation source and destination are identical');
                $exchange = $definition['src-exchange'];
                $this->source->declareExchange($exchange, $definition['exchange-type']);
                $this->source->declareQueue($definition['src-queue']);
                $this->source->bind($definition['src-queue'], $exchange, $definition['exchange-type'] === 'topic' ? '#' : '');
                $this->source->flushDurable();
            }
        } catch (Throwable $e) { $this->error = $e->getMessage(); $this->closed = true; $this->srcPeer?->close(); $this->destPeer?->close(); }
    }
    public function state(): string
    {
        if ($this->closed) return 'error';
        if ($this->error !== '') return 'retrying';
        return $this->destinationReady() && ($this->source !== null ? isset($this->source->queues[$this->definition['src-queue']]) : $this->srcPeer?->ready()) ? 'running' : 'starting';
    }
    private function destinationReady(): bool
    {
        if ($this->destination !== null) return isset($this->definition['dest-exchange']) ? isset($this->destination->exchanges[$this->definition['dest-exchange']]) : isset($this->destination->queues[$this->definition['dest-queue']]);
        return $this->destPeer?->ready() ?? false;
    }
    public function tick(): void
    {
        if ($this->closed) return;
        $this->srcPeer?->tick(); $this->destPeer?->tick();
        if ($this->srcPeer?->error) $this->error = $this->srcPeer->error;
        elseif ($this->destPeer?->error) $this->error = $this->destPeer->error;
        if ($this->source !== null) {
            $queue = $this->definition['src-queue'];
            if ($this->registered) {
                $present=false; foreach($this->source->queues[$queue]['consumers'] ?? [] as $consumer) if($consumer['conn']===$this->conn && $consumer['tag']===$this->tag) $present=true;
                if(!$present) $this->registered=false;
            }
            if (!$this->registered && isset($this->source->queues[$queue])) {
                if (($this->source->queues[$queue]['args']['queueType'] ?? '') === 'stream') { $this->error = 'A stream source requires an offset consumer'; return; }
                try {
                    $this->source->registerProtocolConsumer($queue, $this->conn, $this->tag,
                        fn()=>!$this->closed && !$this->busy && microtime(true)>=$this->retry && $this->destinationReady(),
                        function(array $message, int $id):void { $this->move($message, function(bool $ok)use($id):void { if ($ok) {$this->source->ack($id);} else $this->source->requeue($id); }); });
                    $this->registered = true;
                } catch (Throwable $e) { $this->error = $e->getMessage(); }
            }
        } elseif (!$this->busy && microtime(true)>=$this->retry && $this->destinationReady() && $this->srcPeer?->ready()) {
            $this->srcPeer->get(function(?array $message):void { if ($message !== null) $this->move($message, fn(bool $ok)=>$this->srcPeer->settle($message['tag'],$ok)); });
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
            $hops = 0; foreach ($headers as [$key,$value]) if ($key === 'x-qf-federation-hops') $hops = (int)$value;
            if ($hops >= $this->definition['max-hops']) { $done(true); return; }
            $headers = array_values(array_filter($headers, static fn($pair)=>$pair[0] !== 'x-qf-federation-hops'));
            $headers[] = ['x-qf-federation-hops',$hops+1];
            $props = Amqp10::readProps($raw); $raw = Amqp10::writeProps($props,$headers);
        }
        $exchange = $this->definition['dest-exchange'] ?? '';
        $key = isset($this->definition['dest-exchange']) ? ($this->definition['dest-exchange-key'] ?? $message['key'] ?? '') : $this->definition['dest-queue'];
        if ($this->destination !== null) {
            // Never interpret an unroutable shovel publish as successful acceptance.
            if (!$this->destinationReady()) { $done(false); return; }
            $this->destination->publishAsync($this->conn,0,$exchange,$key,$message['body'],$message['mode'] ?? 2,$message['priority'] ?? 0,$headers,null,$raw,$done);
        } else $this->destPeer->publish($exchange,$key,$message,$headers,$raw,$done);
    }
    public function close(): void
    {
        $this->closed = true;
        if ($this->registered) $this->source->unregisterProtocolConsumer($this->definition['src-queue'],$this->conn,$this->tag);
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
    private bool $returned = false;
    private bool $stopped = false;
    public string $error = '';
    public function __construct(string $uri, private string $queue, private bool $publisher, private ?array $federation = null)
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
                if (@stream_select($read,$write,$except,0,0)!==1) { if(microtime(true)-$this->seen>10)throw new RuntimeException('AMQP connect timed out');return; }
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
        if($class===50&&$method===11){if($this->federation!==null)$this->method(50,20,pack('n',0).Codec::shortstr($this->queue).Codec::shortstr($this->federation['src-exchange']).Codec::shortstr($this->federation['exchange-type']==='topic'?'#':'')."\0".pack('N',0));else $this->finishHandshake();return;}
        if($class===50&&$method===21){$this->finishHandshake();return;}
        if($class===85&&$method===11){$this->phase='ready';$this->error='';return;}
        if(($class===10&&$method===50)||($class===20&&$method===40))throw new RuntimeException('AMQP peer closed: '.bin2hex(substr($p,0,2)).' '.substr($p,3,ord($p[2]??"\0")));
        if($class===60&&$method===71){$o=8;$redelivered=ord($p[$o++])!==0;$exchange=Codec::readShortstr($p,$o);$key=Codec::readShortstr($p,$o);$this->incoming=['tag'=>Codec::readU64($p,0),'exchange'=>$exchange,'key'=>$key,'body'=>''];return;}
        if($class===60&&$method===72){$getter=$this->getter;$this->getter=null;if($getter)$getter(null);return;}
        if($class===60&&$method===50){$this->returned=true;$this->incoming=['return'=>true,'body'=>''];return;}
        if($class===60&&($method===80||$method===120)&&$this->confirm!==null){$tag=Codec::readU64($p,0);$multiple=(ord($p[8]??"\0")&1)!==0;if($tag===$this->publishTag||($multiple&&$tag>=$this->publishTag)){$done=$this->confirm;$this->confirm=null;$done($method===80&&!$this->returned);}return;}
    }
    private function declareQueue():void {if($this->publisher&&$this->queue===''){$this->finishHandshake();return;}$this->method(50,10,pack('n',0).Codec::shortstr($this->queue).chr(2).pack('N',0));}
    private function finishHandshake():void {if($this->publisher)$this->method(85,10,"\0");else{$this->phase='ready';$this->error='';}}
    private function contentDone():void {$message=$this->incoming;$this->incoming=null;if(isset($message['return']))return;$getter=$this->getter;$this->getter=null;if($getter)$getter($message);}
    public function get(callable $done):void {if(!$this->ready()||$this->getter!==null)return;$this->getter=$done;$this->method(60,70,pack('n',0).Codec::shortstr($this->queue)."\0");}
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
        if(is_resource($this->socket))fclose($this->socket);$this->socket=null;$this->phase='disconnected';$this->retry=microtime(true)+1;$this->error=$error;$this->input=$this->output='';$this->incoming=null;$this->getter=null;$this->publishTag=0;$done=$this->confirm;$this->confirm=null;if($done)$done(false);
    }
    public function close():void {$this->stopped=true;$this->fail('Bridge stopped');}
}
