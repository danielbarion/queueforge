//! Discovery backends against local mock HTTP servers, and SigV4 against
//! AWS's published examples.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::*;

/// One request a mock saw.
#[derive(Clone, Debug)]
struct Seen {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: String,
}

impl Seen {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }
}

type Handler = Arc<dyn Fn(&Seen) -> (u16, String) + Send + Sync>;

/// A mock HTTP server on a free loopback port. Returns its base URL and the log.
async fn mock(handler: Handler) -> (String, Arc<Mutex<Vec<Seen>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&log);
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else { break };
            let handler = Arc::clone(&handler);
            let seen = Arc::clone(&seen);
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let head_end = loop {
                    if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break i;
                    }
                    let n = sock.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                };
                let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                let mut lines = head.split("\r\n");
                let mut first = lines.next().unwrap_or_default().split(' ');
                let method = first.next().unwrap_or_default().to_string();
                let path = first.next().unwrap_or_default().to_string();
                let headers: Vec<(String, String)> = lines
                    .filter_map(|l| l.split_once(':').map(|(k, v)| (k.trim().to_string(), v.trim().to_string())))
                    .collect();
                let len: usize = headers.iter().find(|(k, _)| k.eq_ignore_ascii_case("content-length")).and_then(|(_, v)| v.parse().ok()).unwrap_or(0);
                while buf.len() < head_end + 4 + len {
                    let n = sock.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let body = String::from_utf8_lossy(&buf[head_end + 4..]).to_string();
                let req = Seen { method, path, headers, body };
                let (status, reply) = handler(&req);
                seen.lock().unwrap().push(req);
                let out = format!("HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}", reply.len());
                let _ = sock.write_all(out.as_bytes()).await;
            });
        }
    });
    (format!("http://{addr}"), log)
}

fn base_config(discovery: &str) -> ClusterConfig {
    ClusterConfig {
        discovery: Some(discovery.into()),
        listen: Some("127.0.0.1:25901".parse().unwrap()),
        dns_port: Some(25901),
        ..Default::default()
    }
}

fn host_port(base: &str) -> (String, u16) {
    let rest = base.trim_start_matches("http://");
    let (h, p) = rest.rsplit_once(':').unwrap();
    (h.to_string(), p.parse().unwrap())
}

#[test]
fn sigv4_matches_the_aws_get_vanilla_test_vector() {
    // aws-sig-v4-test-suite/get-vanilla.
    let creds = AwsCredentials {
        access_key: "AKIDEXAMPLE".into(),
        secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
        session_token: None,
    };
    let headers = vec![("Host".to_string(), "example.amazonaws.com".to_string()), ("X-Amz-Date".to_string(), "20150830T123600Z".to_string())];
    let auth = sigv4_authorization("GET", "/", &[], &headers, b"", &creds, "us-east-1", "service", "20150830T123600Z");
    assert_eq!(
        auth,
        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
         SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
    );
}

#[test]
fn sigv4_matches_the_aws_documentation_iam_example() {
    // The signing walkthrough in the AWS General Reference (IAM ListUsers).
    assert_eq!(
        hex(&signing_key("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20150830", "us-east-1", "iam")),
        "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
    );
    let creds = AwsCredentials {
        access_key: "AKIDEXAMPLE".into(),
        secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
        session_token: None,
    };
    let headers = vec![
        ("Content-Type".to_string(), "application/x-www-form-urlencoded; charset=utf-8".to_string()),
        ("Host".to_string(), "iam.amazonaws.com".to_string()),
        ("X-Amz-Date".to_string(), "20150830T123600Z".to_string()),
    ];
    let query = vec![("Action".to_string(), "ListUsers".to_string()), ("Version".to_string(), "2010-05-08".to_string())];
    let auth = sigv4_authorization("GET", "/", &query, &headers, b"", &creds, "us-east-1", "iam", "20150830T123600Z");
    assert!(
        auth.ends_with("Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"),
        "{auth}"
    );
}

#[test]
fn etcd_range_end_is_the_prefix_plus_one() {
    assert_eq!(range_end("/qf/nodes/"), b"/qf/nodes0".to_vec());
    assert_eq!(range_end("a\u{7f}"), b"a\x80".to_vec());
}

#[tokio::test]
async fn k8s_lists_ready_and_not_ready_endpoint_addresses() {
    let (base, log) = mock(Arc::new(|req: &Seen| {
        if req.path == "/api/v1/namespaces/prod/endpoints/qf" {
            (200, r#"{"subsets":[{"addresses":[{"ip":"10.0.0.7","hostname":"qf-0"},{"ip":"10.0.0.8","hostname":"qf-1"}],
                "notReadyAddresses":[{"ip":"10.0.0.9","hostname":"qf-2"}]}]}"#.into())
        } else {
            (404, "{}".into())
        }
    }))
    .await;
    let dir = std::env::temp_dir().join(format!("qf-k8s-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("token"), "sa-token\n").unwrap();
    std::fs::write(dir.join("namespace"), "prod").unwrap();
    let (host, port) = host_port(&base);
    let mut c = base_config("k8s");
    c.k8s_scheme = Some("http".into());
    c.k8s_host = Some(host);
    c.k8s_port = Some(port);
    c.k8s_service_name = Some("qf".into());
    c.k8s_token_path = Some(dir.join("token").display().to_string());
    c.k8s_namespace_path = Some(dir.join("namespace").display().to_string());
    resolve(&mut c).await.unwrap();
    let ids: Vec<&str> = c.members.iter().map(|m| m.id.as_str()).collect();
    assert_eq!(ids, ["10.0.0.7:25901", "10.0.0.8:25901", "10.0.0.9:25901", "127.0.0.1:25901"]);
    assert_eq!(c.node_id, "127.0.0.1:25901");
    assert!(c.validate().is_ok());
    let seen = log.lock().unwrap().clone();
    assert_eq!(seen[0].header("Authorization"), Some("Bearer sa-token"));
    // Hostname addressing with a suffix.
    c.k8s_address_type = Some("hostname".into());
    c.k8s_hostname_suffix = Some(".invalid-qf-test".into());
    assert!(k8s_hosts(&c).await.unwrap().contains(&"qf-0.invalid-qf-test".to_string()));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn consul_registers_passes_its_check_lists_and_deregisters() {
    let (base, log) = mock(Arc::new(|req: &Seen| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/v1/health/service/qf?passing=true") => (
            200,
            r#"[{"Node":{"Address":"10.1.0.1"},"Service":{"Address":"","Port":25901}},
                {"Node":{"Address":"10.1.0.9"},"Service":{"Address":"10.1.0.2","Port":25901}}]"#
                .into(),
        ),
        ("PUT", _) => (200, "".into()),
        _ => (404, "".into()),
    }))
    .await;
    let (host, port) = host_port(&base);
    let mut c = base_config("consul");
    c.consul_host = Some(host);
    c.consul_port = Some(port);
    c.consul_svc = Some("qf".into());
    c.consul_acl_token = Some("acl-1".into());
    c.consul_svc_ttl = Some(20);
    let registration = resolve(&mut c).await.unwrap();
    let ids: Vec<&str> = c.members.iter().map(|m| m.id.as_str()).collect();
    assert_eq!(ids, ["10.1.0.1:25901", "10.1.0.2:25901", "127.0.0.1:25901"]);
    registration.remove().await;
    let seen = log.lock().unwrap().clone();
    let register = seen.iter().find(|r| r.path == "/v1/agent/service/register").expect("register");
    let body: Value = serde_json::from_str(&register.body).unwrap();
    assert_eq!(body["Name"], "qf");
    assert_eq!(body["Address"], "127.0.0.1");
    assert_eq!(body["Port"], 25901);
    assert_eq!(body["Check"]["TTL"], "20s");
    assert_eq!(register.header("X-Consul-Token"), Some("acl-1"));
    let id = body["ID"].as_str().unwrap();
    assert!(seen.iter().any(|r| r.path == format!("/v1/agent/check/pass/service:{id}")));
    assert!(seen.iter().any(|r| r.path == format!("/v1/agent/service/deregister/{id}")));
}

#[tokio::test]
async fn etcd_puts_a_leased_key_ranges_the_nodes_and_revokes() {
    let store: Arc<Mutex<BTreeMap<String, String>>> = Arc::new(Mutex::new(BTreeMap::new()));
    store.lock().unwrap().insert("/qf/discovery/prod/nodes/10.2.0.5:25901".into(), "10.2.0.5:25901".into());
    let kv = Arc::clone(&store);
    let (base, log) = mock(Arc::new(move |req: &Seen| {
        let body: Value = serde_json::from_str(&req.body).unwrap_or(Value::Null);
        let text = |k: &str| String::from_utf8(unb64(body[k].as_str().unwrap_or_default())).unwrap_or_default();
        match req.path.as_str() {
            "/v3/lease/grant" => (200, r#"{"ID":"7587","TTL":"15"}"#.into()),
            "/v3/kv/put" => {
                kv.lock().unwrap().insert(text("key"), text("value"));
                (200, "{}".into())
            }
            "/v3/kv/range" => {
                let (from, to) = (text("key"), text("range_end"));
                let kvs: Vec<Value> = kv
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(k, _)| k.as_str() >= from.as_str() && k.as_str() < to.as_str())
                    .map(|(k, v)| json!({"key": b64(k.as_bytes()), "value": b64(v.as_bytes())}))
                    .collect();
                (200, json!({ "kvs": kvs }).to_string())
            }
            "/v3/lease/keepalive" | "/v3/lease/revoke" => (200, "{}".into()),
            _ => (404, "{}".into()),
        }
    }))
    .await;
    let mut c = base_config("etcd");
    c.etcd_endpoints = vec!["http://127.0.0.1:1".into(), base];
    c.etcd_key_prefix = Some("qf".into());
    c.etcd_cluster_name = Some("prod".into());
    c.etcd_node_ttl = Some(15);
    let registration = resolve(&mut c).await.unwrap();
    let ids: Vec<&str> = c.members.iter().map(|m| m.id.as_str()).collect();
    assert_eq!(ids, ["10.2.0.5:25901", "127.0.0.1:25901"]);
    assert!(store.lock().unwrap().contains_key("/qf/discovery/prod/nodes/127.0.0.1:25901"));
    registration.remove().await;
    let seen = log.lock().unwrap().clone();
    let put = seen.iter().find(|r| r.path == "/v3/kv/put").unwrap();
    assert_eq!(serde_json::from_str::<Value>(&put.body).unwrap()["lease"], "7587");
    assert!(seen.iter().any(|r| r.path == "/v3/lease/revoke" && r.body.contains("7587")));
}

#[tokio::test]
async fn aws_signs_describe_instances_and_reads_the_private_addresses() {
    let (base, log) = mock(Arc::new(|req: &Seen| {
        if req.path.starts_with("/?Action=DescribeInstances") {
            (
                200,
                r#"<DescribeInstancesResponse><reservationSet><item><instancesSet>
                <item><instanceId>i-1</instanceId><privateDnsName>ip-10-3-0-4</privateDnsName><privateIpAddress>10.3.0.4</privateIpAddress>
                  <ipAddress>54.0.0.4</ipAddress><networkInterfaceSet><item><privateIpAddress>10.3.0.4</privateIpAddress></item></networkInterfaceSet></item>
                <item><instanceId>i-2</instanceId><privateIpAddress>10.3.0.5</privateIpAddress><ipAddress>54.0.0.5</ipAddress></item>
                </instancesSet></item></reservationSet></DescribeInstancesResponse>"#
                    .into(),
            )
        } else {
            (400, "".into())
        }
    }))
    .await;
    let mut c = base_config("aws");
    c.aws_region = Some("eu-west-1".into());
    c.aws_access_key_id = Some("AKIDTEST".into());
    c.aws_secret_access_key = Some("secret".into());
    c.aws_endpoint = Some(base);
    c.aws_ec2_tags = BTreeMap::from([("service".to_string(), "queueforge".to_string())]);
    resolve(&mut c).await.unwrap();
    let ids: Vec<&str> = c.members.iter().map(|m| m.id.as_str()).collect();
    assert_eq!(ids, ["10.3.0.4:25901", "10.3.0.5:25901", "127.0.0.1:25901"]);
    let seen = log.lock().unwrap().clone();
    let req = &seen[0];
    for part in [
        "Action=DescribeInstances",
        "Version=2016-11-15",
        "Filter.1.Name=tag%3Aservice",
        "Filter.1.Value.1=queueforge",
        "Filter.2.Name=instance-state-name",
        "Filter.2.Value.1=running",
    ] {
        assert!(req.path.contains(part), "{} lacks {part}", req.path);
    }
    let auth = req.header("Authorization").unwrap();
    assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIDTEST/"), "{auth}");
    assert!(auth.contains("/eu-west-1/ec2/aws4_request, SignedHeaders=host;x-amz-date, Signature="), "{auth}");
    c.aws_use_private_ip = Some(false);
    assert_eq!(aws_hosts(&c).await.unwrap(), ["54.0.0.4", "54.0.0.5"]);
}

#[tokio::test]
async fn aws_falls_back_to_the_instance_role() {
    let (imds, _) = mock(Arc::new(|req: &Seen| match req.path.as_str() {
        "/latest/api/token" => (200, "tok".into()),
        "/latest/meta-data/iam/security-credentials/" if req.header("X-aws-ec2-metadata-token") == Some("tok") => (200, "qf-role".into()),
        "/latest/meta-data/iam/security-credentials/qf-role" => (200, r#"{"AccessKeyId":"ASIAROLE","SecretAccessKey":"s","Token":"session"}"#.into()),
        _ => (404, "".into()),
    }))
    .await;
    // SAFETY: tests in this binary that read these variables run only here.
    unsafe {
        std::env::set_var("AWS_EC2_METADATA_SERVICE_ENDPOINT", &imds);
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
    }
    let creds = aws_credentials(&base_config("aws")).await.unwrap();
    assert_eq!(creds.access_key, "ASIAROLE");
    assert_eq!(creds.session_token.as_deref(), Some("session"));
}
