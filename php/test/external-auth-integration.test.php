<?php
declare(strict_types=1);
require_once __DIR__ . '/lib/Harness.php';
foreach (['Routing','Features','Policy','Codec','Auth','Store','Broker'] as $file) require_once dirname(__DIR__) . '/src/' . $file . '.php';

function authBroker(): Broker
{
    $dir=sys_get_temp_dir().'/qf-external-integration-'.bin2hex(random_bytes(5));mkdir($dir);
    $broker=new Broker(new Store($dir.'/messages.log'),$dir.'/users.json');$broker->vhosts=['/','prod','dev'];return $broker;
}
function externalPrincipal(array $scopes, array $tags=['management'], string $name='shared-user'): array
{
    return ['name'=>$name,'source'=>'oauth','tags'=>$tags,'scopes'=>$scopes,'expiresAt'=>(microtime(true)+600)*1000,'permissions'=>[],'topicPermissions'=>[]];
}
function scoped(string $kind, string $host, string $resource, ?string $key=null): array
{
    return ['kind'=>$kind,'vhost'=>$host,'resource'=>$resource,'routingKey'=>$key];
}
function registerFixture(Broker $broker,string $user,string $credential,array $principal): string
{
    // Inject validated backend results at the private credential-registration seam.
    return (new ReflectionMethod(Broker::class,'registerExternalPrincipal'))->invoke($broker,$user,$credential,$principal);
}

Harness::guard('internal identity precedence and compatibility',static function():void{
    $broker=authBroker();$broker->users['internal']=Auth::hash('internal-password');$broker->tags['internal']=['administrator'];
    Harness::eq('internal authentication returns internal name','internal',$broker->authenticate('internal','internal-password'));
    Harness::eq('wrong internal password fails',null,$broker->authenticate('internal','wrong'));
    Harness::ok('verify preserves bool compatibility',$broker->verify('internal','internal-password')&&!$broker->verify('internal','wrong'));
    Harness::eq('unconfigured external authentication fails',null,$broker->authenticate('outside','opaque-token'));
    Harness::eq('internal visible name unchanged','internal',$broker->identityName('internal'));
    Harness::eq('internal tags unchanged',['administrator'],$broker->userTags('internal'));
    Harness::ok('internal admin behavior retained',$broker->isAdmin('internal')&&$broker->hasVhostAccess('internal','prod')&&$broker->resourceAllowed('internal','prod','write','queue'));
    $broker->users['@external:local']=Auth::hash('password');$broker->tags['@external:local']=['administrator'];
    Harness::ok('prefix cannot suppress a genuine internal account',$broker->resourceAllowed('@external:local','prod','configure','q'));
});

Harness::guard('credential identities isolate concurrent same-name tokens',static function():void{
    $broker=authBroker();
    $read=externalPrincipal([scoped('read','^prod$','^orders$')]);
    $write=externalPrincipal([scoped('write','^dev$','^events$')]);
    $a=registerFixture($broker,'shared-user','read-token',$read);
    $b=registerFixture($broker,'shared-user','write-token',$write);
    Harness::ok('tokens receive different opaque identities',$a!==$b&&$a!=='shared-user'&&$b!=='shared-user');
    Harness::eq('same credentials reuse their own identity',$a,registerFixture($broker,'shared-user','read-token',$read));
    Harness::eq('read token visible name','shared-user',$broker->identityName($a));
    Harness::eq('write token visible name','shared-user',$broker->identityName($b));
    Harness::ok('read token retains its own grant',$broker->resourceAllowed($a,'prod','read','orders'));
    Harness::ok('write token retains its own grant',$broker->resourceAllowed($b,'dev','write','events'));
    Harness::ok('read token never borrows writer scope',!$broker->resourceAllowed($a,'dev','write','events'));
    Harness::ok('writer never borrows reader scope',!$broker->resourceAllowed($b,'prod','read','orders'));
    Harness::ok('vhost access follows each token scope',$broker->hasVhostAccess($a,'prod')&&!$broker->hasVhostAccess($a,'dev'));
    $child=$broker->forVhost('prod');
    Harness::ok('child reads shared principal registry',$child->resourceAllowed($a,'prod','read','orders')&&!$child->resourceAllowed($b,'prod','read','orders'));
    $broker->authConfig=['oauth'=>['resourceServerId'=>'shared-config']];
    Harness::eq('children share authentication config',$broker->authConfig,$child->authConfig);
    $child->externalPrincipals[$a]['expiresAt']=(microtime(true)-1)*1000;
    Harness::ok('shared expiration revokes read grant',!$broker->resourceAllowed($a,'prod','read','orders'));
    Harness::ok('expired principal loses management and vhost access',!$child->canManage($a)&&!$child->hasVhostAccess($a,'prod')&&$child->userTags($a)===[]);
    Harness::ok('other token stays authorized',$broker->resourceAllowed($b,'dev','write','events'));
    $broker->users[$b]=Auth::hash('collision-password'); $broker->tags[$b]=['administrator'];
    Harness::ok('opaque identity collision cannot turn a session into an internal administrator',!$broker->isAdmin($b)&&!$broker->resourceAllowed($b,'prod','configure','anything'));
});

Harness::guard('OAuth tags never widen scopes or routing keys',static function():void{
    $broker=authBroker();$p=externalPrincipal([
        scoped('write','^prod$','^exchange-a$','^sales\\..*$'),
        scoped('write','^prod$','^exchange-b$','^private\\..*$'),
        scoped('read','^prod$','^exchange-a$','^public\\..*$'),
    ],['administrator','management']);
    $id=registerFixture($broker,'shared-user','admin-token',$p);
    Harness::ok('management tag permits management access',$broker->canManage($id)&&$broker->isAdmin($id));
    Harness::ok('administrator tag cannot widen resource scope',!$broker->resourceAllowed($id,'prod','configure','exchange-a')&&!$broker->resourceAllowed($id,'dev','write','exchange-a'));
    Harness::ok('administrator tag cannot widen vhost scope',!$broker->hasVhostAccess($id,'dev'));
    Harness::ok('write routing key matches paired resource',$broker->topicWriteAllowed($id,'prod','exchange-a','sales.eu'));
    Harness::ok('cannot borrow another resource routing key',!$broker->topicWriteAllowed($id,'prod','exchange-a','private.eu'));
    Harness::ok('read routing key uses read scope',$broker->topicReadAllowed($id,'prod','exchange-a','public.eu')&&!$broker->topicReadAllowed($id,'prod','exchange-a','sales.eu'));
    Harness::ok('missing external topic scope is denial',!$broker->topicReadAllowed($id,'prod','unknown','public.eu'));
    $broker->externalPrincipals[$id]['expiresAt']=0;
    Harness::ok('expired admin loses every privileged path',!$broker->isAdmin($id)&&!$broker->canManage($id)&&!$broker->hasVhostAccess($id,'prod')&&!$broker->resourceAllowed($id,'prod','write','exchange-a')&&!$broker->topicWriteAllowed($id,'prod','exchange-a','sales.eu')&&!$broker->topicReadAllowed($id,'prod','exchange-a','public.eu'));
    Harness::ok('unknown opaque identity fails closed',!$broker->canManage('@external:unknown')&&!$broker->resourceAllowed('@external:unknown','prod','read','q')&&!$broker->topicReadAllowed('@external:unknown','prod','ex','key')&&!$broker->topicWriteAllowed('@external:unknown','prod','ex','key'));
});

Harness::guard('LDAP defaults and later internal account precedence',static function():void{
    $broker=authBroker();$p=['name'=>'ldap-user','source'=>'ldap','tags'=>['management'],'scopes'=>null,'expiresAt'=>null,'permissions'=>[],'topicPermissions'=>[]];
    $id=registerFixture($broker,'ldap-user','ldap-password',$p);
    Harness::ok('LDAP grants configured vhosts and resources',$broker->hasVhostAccess($id,'prod')&&$broker->resourceAllowed($id,'prod','configure','q')&&$broker->topicWriteAllowed($id,'prod','ex','any'));
    Harness::ok('missing broker vhost still denied',!$broker->hasVhostAccess($id,'missing'));
    Harness::ok('LDAP management is not administrator',$broker->canManage($id)&&!$broker->isAdmin($id));
    $broker->users['ldap-user']=Auth::hash('local-password');$broker->tags['ldap-user']=['administrator'];
    Harness::ok('new internal account revokes external identity',!$broker->canManage($id)&&!$broker->hasVhostAccess($id,'prod')&&!$broker->resourceAllowed($id,'prod','write','q')&&!$broker->topicWriteAllowed($id,'prod','ex','any'));
    Harness::eq('external password cannot authenticate internal name',null,$broker->authenticate('ldap-user','ldap-password'));
    Harness::eq('internal password wins','ldap-user',$broker->authenticate('ldap-user','local-password'));
});
Harness::done();
