//! Router binding identity, topic match, and unbind tests.

use super::*;
use crate::domain::ExchangeType;

#[test]
fn headers_unbind_keeps_the_sibling_binding() {
    use crate::domain::HeaderArg;
    let r = ExchangeRouter::new();
    r.put_exchange(Exchange {
        vhost: "/".into(),
        name: "h".into(),
        kind: ExchangeType::Headers,
        durable: false,
        auto_delete: false,
        internal: false,
        alternate: None,
        delayed_type: None,
    });
    let mut color = Binding::new("/", "h", "q2", "");
    color.args = vec![(CompactString::from("color"), HeaderArg::Str("blue".into()))];
    let mut size = Binding::new("/", "h", "q2", "other");
    size.args = vec![(CompactString::from("size"), HeaderArg::Str("l".into()))];
    r.bind(color.clone()).unwrap();
    r.bind(size).unwrap();
    r.unbind(&color).unwrap();
    let routed = r
        .route_publish(
            "/",
            "h",
            "ignored",
            &[(CompactString::from("size"), HeaderArg::Str("l".into()))],
        )
        .unwrap();
    let names: Vec<_> = routed
        .destinations
        .iter()
        .map(|k| k.name.as_str())
        .collect();
    assert_eq!(names, vec!["q2"]);
}

fn arg_binding(exchange: &str, queue: &str, routing_key: &str, name: &str, value: &str) -> Binding {
    let mut binding = Binding::new("/", exchange, queue, routing_key);
    binding.args = vec![(
        CompactString::from(name),
        crate::domain::HeaderArg::Str(value.into()),
    )];
    binding
}

fn put_kind(router: &ExchangeRouter, name: &str, kind: ExchangeType) {
    router.put_exchange(Exchange {
        vhost: "/".into(),
        name: name.into(),
        kind,
        durable: false,
        auto_delete: false,
        internal: false,
        alternate: None,
        delayed_type: None,
    });
}

#[test]
fn unbind_one_direct_arg_variant_keeps_the_sibling_routed() {
    let router = ExchangeRouter::new();
    put_kind(&router, "d", ExchangeType::Direct);
    let color = arg_binding("d", "q", "rk", "color", "blue");
    let size = arg_binding("d", "q", "rk", "size", "l");
    router.bind(color.clone()).unwrap();
    router.bind(size).unwrap();
    router.unbind(&color).unwrap();
    let routed = router.route_publish("/", "d", "rk", &[]).unwrap();
    let names: Vec<_> = routed
        .destinations
        .iter()
        .map(|k| k.name.as_str())
        .collect();
    assert_eq!(names, vec!["q"]);
}

#[test]
fn unbind_one_fanout_arg_variant_keeps_the_sibling_routed() {
    let router = ExchangeRouter::new();
    put_kind(&router, "f", ExchangeType::Fanout);
    let color = arg_binding("f", "q", "", "color", "blue");
    let size = arg_binding("f", "q", "", "size", "l");
    router.bind(color.clone()).unwrap();
    router.bind(size).unwrap();
    router.unbind(&color).unwrap();
    let routed = router.route_publish("/", "f", "ignored", &[]).unwrap();
    let names: Vec<_> = routed
        .destinations
        .iter()
        .map(|k| k.name.as_str())
        .collect();
    assert_eq!(names, vec!["q"]);
}

#[test]
fn unbind_one_topic_arg_variant_keeps_the_sibling_routed() {
    let router = ExchangeRouter::new();
    put_kind(&router, "t", ExchangeType::Topic);
    let color = arg_binding("t", "q", "orders.*", "color", "blue");
    let size = arg_binding("t", "q", "orders.*", "size", "l");
    router.bind(color.clone()).unwrap();
    router.bind(size).unwrap();
    router.unbind(&color).unwrap();
    let routed = router.route_publish("/", "t", "orders.new", &[]).unwrap();
    let names: Vec<_> = routed
        .destinations
        .iter()
        .map(|k| k.name.as_str())
        .collect();
    assert_eq!(names, vec!["q"]);
}

#[test]
fn unbind_one_headers_arg_variant_keeps_the_sibling_routed() {
    let router = ExchangeRouter::new();
    put_kind(&router, "h", ExchangeType::Headers);
    let color = arg_binding("h", "q", "", "color", "blue");
    let size = arg_binding("h", "q", "", "size", "l");
    router.bind(color.clone()).unwrap();
    router.bind(size).unwrap();
    router.unbind(&color).unwrap();
    let routed = router
        .route_publish(
            "/",
            "h",
            "",
            &[(
                CompactString::from("size"),
                crate::domain::HeaderArg::Str("l".into()),
            )],
        )
        .unwrap();
    let names: Vec<_> = routed
        .destinations
        .iter()
        .map(|k| k.name.as_str())
        .collect();
    assert_eq!(names, vec!["q"]);
    let gone = router
        .route_publish(
            "/",
            "h",
            "",
            &[(
                CompactString::from("color"),
                crate::domain::HeaderArg::Str("blue".into()),
            )],
        )
        .unwrap();
    assert!(gone.destinations.is_empty());
}

#[test]
fn topic_lookup_skips_unrelated_first_words() {
    let mut idx = BindingIndex::empty(1);
    for i in 0..1000 {
        let key = BindingKey::new("/", "t", format!("q{i}"), format!("other.{i}"));
        idx.insert(key, ExchangeType::Topic);
    }
    idx.insert(
        BindingKey::new("/", "t", "hit", "want.x"),
        ExchangeType::Topic,
    );
    let (names, examined) = idx.topic_destinations("/", "t", "want.x");
    assert_eq!(names, vec![CompactString::from("hit")]);
    assert!(
        examined < 10,
        "unrelated bindings were examined: {examined}"
    );
}

#[test]
fn topic_star_and_hash() {
    assert!(topic_matches("a.*", "a.b"));
    assert!(!topic_matches("a.*", "a.b.c"));
    assert!(topic_matches("a.#", "a.b.c"));
    assert!(topic_matches("a.#", "a"));
    assert!(topic_matches("#", "x.y.z"));
    assert!(topic_matches("#", ""));
    assert!(topic_matches("*.orange.*", "quick.orange.rabbit"));
    assert!(!topic_matches("*.orange.*", "quick.orange.male.rabbit"));
    assert!(topic_matches("lazy.#", "lazy.pink.rabbit"));
    assert!(topic_matches("*.*.rabbit", "quick.orange.rabbit"));
    assert!(!topic_matches("a.b", "a.c"));
    assert!(topic_matches("a.b", "a.b"));
}

#[test]
fn direct_and_fanout_routing() {
    let r = ExchangeRouter::new();
    r.put_exchange(Exchange::new("/", "d", ExchangeType::Direct));
    r.put_exchange(Exchange::new("/", "f", ExchangeType::Fanout));

    r.bind(Binding::new("/", "d", "q1", "rk1")).unwrap();
    r.bind(Binding::new("/", "d", "q2", "rk1")).unwrap();
    r.bind(Binding::new("/", "d", "q3", "other")).unwrap();
    r.bind(Binding::new("/", "f", "qa", "")).unwrap();
    r.bind(Binding::new("/", "f", "qb", "ignored")).unwrap();

    let d = r.route("/", "d", "rk1").unwrap();
    assert_eq!(d.destinations.len(), 2);
    let names: Vec<_> = d.destinations.iter().map(|k| k.name.as_str()).collect();
    assert!(names.contains(&"q1"));
    assert!(names.contains(&"q2"));

    let f = r.route("/", "f", "anything").unwrap();
    assert_eq!(f.destinations.len(), 2);

    let empty = r.route("/", "d", "nope").unwrap();
    assert!(empty.destinations.is_empty());
}

#[test]
fn topic_routing_dedupes_queue() {
    let r = ExchangeRouter::new();
    r.put_exchange(Exchange::new("/", "t", ExchangeType::Topic));
    r.bind(Binding::new("/", "t", "q1", "a.*")).unwrap();
    r.bind(Binding::new("/", "t", "q1", "a.#")).unwrap();
    r.bind(Binding::new("/", "t", "q2", "a.b")).unwrap();

    let res = r.route("/", "t", "a.b").unwrap();
    let names: Vec<_> = res.destinations.iter().map(|k| k.name.as_str()).collect();
    assert_eq!(names, vec!["q1", "q2"]);
}

#[test]
fn cannot_bind_default_exchange() {
    let r = ExchangeRouter::new();
    r.put_exchange(Exchange {
        vhost: "/".into(),
        name: "".into(),
        kind: ExchangeType::Default,
        durable: true,
        auto_delete: false,
        internal: true,
        alternate: None,
        delayed_type: None,
    });
    let err = r.bind(Binding::new("/", "", "q", "q")).unwrap_err();
    assert!(matches!(err, Error::PreconditionFailed(_)));
}

#[test]
fn unbind_and_delete_exchange() {
    let r = ExchangeRouter::new();
    r.put_exchange(Exchange::new("/", "x", ExchangeType::Direct));
    r.bind(Binding::new("/", "x", "q", "k")).unwrap();
    assert!(r.unbind(&Binding::new("/", "x", "q", "k")).unwrap());
    assert!(!r.unbind(&Binding::new("/", "x", "q", "k")).unwrap());
    r.bind(Binding::new("/", "x", "q", "k")).unwrap();
    r.delete_exchange("/", "x").unwrap();
    assert!(r.route("/", "x", "k").is_err());
}
