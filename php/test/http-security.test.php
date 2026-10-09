<?php
declare(strict_types=1);
require_once __DIR__.'/lib/Harness.php';
foreach (['Routing','Features','Policy','Codec','Auth','Store','Cluster','Broker','Streams','Http'] as $class) require_once dirname(__DIR__).'/src/'.$class.'.php';
Harness::guard('management permissions apply to every HTTP path', static function (): void {
    $dir = sys_get_temp_dir().'/qf-http-security-'.bin2hex(random_bytes(6));
    $b = new Broker(new Store($dir.'/messages.log'), $dir.'/users.json');
    $b->bootstrap('devpassword12');
    $b->putUser('limited','testpassword12',['management']);
    $b->setPermissions('limited','/','^own$','^q$','^events$');
    $b->vhosts[] = '/secret'; $b->forVhost('/secret')->declareQueue('private');
    $b->declareExchange('events','topic'); $b->declareQueue('q'); $b->declareQueue('own');
    $b->topicPermissions['limited']['/']['events'] = ['read'=>'^allowed$','write'=>'^allowed$'];
    $http = new Http($b,'',0,false);
    $request = static function (string $method, string $path, array $json = [], ?callable $reply = null) use ($http): string {
        $body = json_encode($json);
        return $http->handle("$method $path HTTP/1.1\r\nAuthorization: Basic ".base64_encode('limited:testpassword12')."\r\nContent-Length: ".strlen($body)."\r\n\r\n".$body,$reply);
    };
    foreach (['/api/definitions','/api/users','/api/permissions','/api/queues/%2Fsecret'] as $path) {
        $response = $request('GET',$path);
        Harness::eq($path.' denied', '403', substr($response,9,3));
        Harness::ok($path.' no hash leak', !str_contains($response,'password_hash'));
        Harness::ok($path.' no private queue leak', !str_contains($response,'private'));
    }
    Harness::ok('queue collection filters unreadable names', !str_contains($request('GET','/api/queues/%2F'),'"name":"own"'));
    foreach (['/api/bindings/%2F','/api/bindings/%2F/e/events/q/q'] as $path) {
        foreach ([null,static function(string $response): void {}] as $reply) {
            Harness::eq('forbidden topic binding denied', '403', substr($request('POST',$path,['source'=>'events','destination'=>'q','routing_key'=>'forbidden'],$reply),9,3));
        }
    }
    Harness::eq('forbidden bindings leave topology unchanged', [], $b->bindings);
    Harness::eq('allowed binding succeeds','201',substr($request('POST','/api/bindings/%2F',['source'=>'events','destination'=>'q','routing_key'=>'allowed']),9,3));
    $b->setPermissions('limited','/','^own$','^events$','^events$');
    Harness::eq('forbidden topic publish denied','403',substr($request('POST','/api/exchanges/%2F/events/publish',['routing_key'=>'forbidden','payload'=>'body']),9,3));
    Harness::eq('forbidden publish never enqueues',0,count($b->msgs));
});
Harness::guard('Raft enable requires administrator and negotiated support', static function (): void {
    $dir = sys_get_temp_dir().'/qf-http-flags-'.bin2hex(random_bytes(6));
    $b = new Broker(new Store($dir.'/messages.log'), $dir.'/users.json'); $b->bootstrap('devpassword12');
    $b->putUser('limited','testpassword12',['management']);
    $cluster = new Cluster($b,'local');
    $b->members = [['id'=>'local','addr'=>'unused'],['id'=>'unknown','addr'=>'unused']];
    $http = new Http($b,'',0,false);
    $request = static function(string $user, string $operation) use($http): string {
        $pass = $user === 'admin' ? 'devpassword12' : 'testpassword12';
        return $http->handle("PUT /api/feature-flags/raft/$operation HTTP/1.1\r\nAuthorization: Basic ".base64_encode("$user:$pass")."\r\n\r\n");
    };
    Harness::eq('limited user cannot enable Raft','403',substr($request('limited','enable'),9,3));
    Harness::eq('unknown peer cannot activate Raft','400',substr($request('admin','enable'),9,3));
    Harness::eq('failed activation preserves legacy state',false,$cluster->raftEnabled());
    $b->members = [['id'=>'local','addr'=>'unused']];
    Harness::eq('administrator enables supported singleton','204',substr($request('admin','enable'),9,3));
    Harness::eq('runtime activation occurs',true,$cluster->raftEnabled());
    Harness::ok('activation persists',is_file($b->dataDir().'/raft/enabled'));
    Harness::eq('Raft cannot be disabled','400',substr($request('admin','disable'),9,3));
});
Harness::done();
