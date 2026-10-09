<?php
declare(strict_types=1);
require_once __DIR__.'/lib/Harness.php';
require_once dirname(__DIR__).'/src/Discovery.php';
Harness::guard('DNS discovery retains self and resolves address IDs', static function(): void {
    $config=['discovery'=>'dns','dns_name'=>'localhost','dns_port'=>45678,'cluster'=>'127.0.0.1:45679','node_id'=>'self'];
    $resolved=Discovery::resolve($config);$peers=array_column($resolved['members'],null,'id');
    Harness::eq('self retained', ['id'=>'self','addr'=>'127.0.0.1:45679'],$peers['self']);
    Harness::ok('localhost A address discovered',isset($peers['127.0.0.1:45678']));
    foreach($peers as$id=>$peer)if($id!=='self')Harness::eq('peer address is its portable ID',$id,$peer['addr']);
    Harness::eq('stable voter order',array_keys($peers),(static function(array $keys):array{sort($keys,SORT_STRING);return$keys;})(array_keys($peers)));
    unset($config['node_id']);Harness::eq('empty identity derives from listen','127.0.0.1:45679',Discovery::resolve($config)['node_id']);
    $invalid=false;try{Discovery::resolve(['discovery'=>'dns']);}catch(RuntimeException){$invalid=true;}Harness::ok('missing DNS settings fail before broker starts',$invalid);
    $config['dns_port']=70000;$invalid=false;try{Discovery::resolve($config);}catch(RuntimeException){$invalid=true;}Harness::ok('invalid DNS port rejected',$invalid);
    $manual=['members'=>[['id'=>'a','addr'=>'localhost:5672']]];Harness::eq('static membership remains intact',$manual,Discovery::resolve($manual));
});
Harness::done();
