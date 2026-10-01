//! Metadata store CRUD, builtin exchange repair, and binding identity tests.

use super::*;
use queueforge_core::{ExchangeType, HeaderArg, DEFAULT_EXCHANGE_NAME};
use tempfile::TempDir;

fn open_tmp() -> (TempDir, MetadataStore) {
    let dir = TempDir::new().unwrap();
    let store = MetadataStore::open(dir.path()).unwrap();
    (dir, store)
}

#[test]
fn bootstrap_creates_default_vhost_and_builtins() {
    let (_dir, store) = open_tmp();
    assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION_V1);

    let vhosts = store.list_vhosts().unwrap();
    assert_eq!(vhosts.len(), 1);
    assert_eq!(vhosts[0].name, DEFAULT_VHOST);

    let exchanges = store.list_exchanges(DEFAULT_VHOST).unwrap();
    assert_eq!(exchanges.len(), 4);

    let default_ex = store
        .get_exchange(DEFAULT_VHOST, DEFAULT_EXCHANGE_NAME)
        .unwrap()
        .expect("default exchange");
    assert_eq!(default_ex.kind, ExchangeType::Default);
    assert!(default_ex.durable);
    assert!(!default_ex.auto_delete);
    assert!(default_ex.internal);

    for name in ["amq.direct", "amq.fanout", "amq.topic"] {
        let ex = store
            .get_exchange(DEFAULT_VHOST, name)
            .unwrap()
            .unwrap_or_else(|| panic!("missing {name}"));
        assert!(ex.durable);
        assert!(!ex.auto_delete);
        assert!(!ex.internal);
    }

    assert_eq!(
        store
            .get_exchange(DEFAULT_VHOST, "amq.direct")
            .unwrap()
            .unwrap()
            .kind,
        ExchangeType::Direct
    );
    assert_eq!(
        store
            .get_exchange(DEFAULT_VHOST, "amq.fanout")
            .unwrap()
            .unwrap()
            .kind,
        ExchangeType::Fanout
    );
    assert_eq!(
        store
            .get_exchange(DEFAULT_VHOST, "amq.topic")
            .unwrap()
            .unwrap()
            .kind,
        ExchangeType::Topic
    );
}

#[test]
fn reopen_preserves_data_and_does_not_duplicate_builtins() {
    let dir = TempDir::new().unwrap();
    {
        let store = MetadataStore::open(dir.path()).unwrap();
        store.create_vhost("app").unwrap();
        let mut q = Queue::new("app", "jobs");
        q.durable = true;
        store.create_queue(&q).unwrap();
    }
    let store = MetadataStore::open(dir.path()).unwrap();
    assert_eq!(store.list_vhosts().unwrap().len(), 2);
    assert_eq!(store.list_exchanges("app").unwrap().len(), 4);
    assert_eq!(store.list_queues("app").unwrap().len(), 1);
    let q = store.get_queue("app", "jobs").unwrap().unwrap();
    assert!(q.durable);
}

#[test]
fn vhost_crud() {
    let (_dir, store) = open_tmp();

    let vh = store.create_vhost("tenant-a").unwrap();
    assert_eq!(vh.name, "tenant-a");
    assert!(store.get_vhost("tenant-a").unwrap().is_some());

    let err = store.create_vhost("tenant-a").unwrap_err();
    assert!(matches!(err, StoreError::VhostExists(_)));

    assert_eq!(store.list_exchanges("tenant-a").unwrap().len(), 4);

    assert!(store.delete_vhost("tenant-a").unwrap());
    assert!(store.get_vhost("tenant-a").unwrap().is_none());
    assert!(store.list_exchanges("tenant-a").unwrap().is_empty());
    assert!(!store.delete_vhost("tenant-a").unwrap());
}

#[test]
fn exchange_crud() {
    let (_dir, store) = open_tmp();
    store.create_vhost("vh").unwrap();

    let ex = Exchange::new("vh", "orders", ExchangeType::Direct);
    store.create_exchange(&ex).unwrap();
    assert!(store.get_exchange("vh", "orders").unwrap().is_some());

    let err = store.create_exchange(&ex).unwrap_err();
    assert!(matches!(err, StoreError::ExchangeExists { .. }));

    let mut updated = ex.clone();
    updated.auto_delete = true;
    store.put_exchange(&updated).unwrap();
    assert!(
        store
            .get_exchange("vh", "orders")
            .unwrap()
            .unwrap()
            .auto_delete
    );

    assert!(store.delete_exchange("vh", "orders").unwrap());
    assert!(store.get_exchange("vh", "orders").unwrap().is_none());

    let err = store.delete_exchange("vh", "amq.direct").unwrap_err();
    assert!(matches!(err, StoreError::BuiltinExchange { .. }));

    let orphan = Exchange::new("nope", "x", ExchangeType::Fanout);
    let err = store.create_exchange(&orphan).unwrap_err();
    assert!(matches!(err, StoreError::VhostNotFound(_)));
}

#[test]
fn cannot_mutate_or_delete_builtin_exchanges() {
    let (_dir, store) = open_tmp();

    let before = store
        .get_exchange(DEFAULT_VHOST, "amq.direct")
        .unwrap()
        .unwrap();

    // put_exchange must not clobber builtins
    let mut evil = before.clone();
    evil.kind = ExchangeType::Fanout;
    evil.durable = false;
    evil.internal = true;
    let err = store.put_exchange(&evil).unwrap_err();
    assert!(matches!(err, StoreError::BuiltinExchange { .. }));
    assert_eq!(
        store
            .get_exchange(DEFAULT_VHOST, "amq.direct")
            .unwrap()
            .unwrap(),
        before
    );

    // create_exchange on builtin name rejected
    let err = store
        .create_exchange(&Exchange::new(
            DEFAULT_VHOST,
            "amq.topic",
            ExchangeType::Direct,
        ))
        .unwrap_err();
    assert!(matches!(err, StoreError::BuiltinExchange { .. }));

    // default exchange "" delete rejected; value unchanged
    let default_before = store
        .get_exchange(DEFAULT_VHOST, DEFAULT_EXCHANGE_NAME)
        .unwrap()
        .unwrap();
    let err = store
        .delete_exchange(DEFAULT_VHOST, DEFAULT_EXCHANGE_NAME)
        .unwrap_err();
    assert!(matches!(err, StoreError::BuiltinExchange { .. }));
    assert_eq!(
        store
            .get_exchange(DEFAULT_VHOST, DEFAULT_EXCHANGE_NAME)
            .unwrap()
            .unwrap(),
        default_before
    );

    // put on default name also rejected
    let mut evil_default = default_before.clone();
    evil_default.internal = false;
    let err = store.put_exchange(&evil_default).unwrap_err();
    assert!(matches!(err, StoreError::BuiltinExchange { .. }));
}

#[test]
fn repairs_corrupted_builtin_attributes_on_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let store = MetadataStore::open(dir.path()).unwrap();
        drop(store);
        // Corrupt amq.fanout attributes via raw redb write.
        let db = Database::create(dir.path().join(METADATA_DB_FILE)).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut exchanges = txn.open_table(EXCHANGES).unwrap();
            let mut bad = Exchange::new(DEFAULT_VHOST, "amq.fanout", ExchangeType::Direct);
            bad.durable = false;
            bad.auto_delete = true;
            let bytes = serde_json::to_vec(&bad).unwrap();
            exchanges
                .insert((DEFAULT_VHOST, "amq.fanout"), bytes.as_slice())
                .unwrap();
        }
        txn.commit().unwrap();
    }
    let store = MetadataStore::open(dir.path()).unwrap();
    let fixed = store
        .get_exchange(DEFAULT_VHOST, "amq.fanout")
        .unwrap()
        .unwrap();
    assert_eq!(fixed.kind, ExchangeType::Fanout);
    assert!(fixed.durable);
    assert!(!fixed.auto_delete);
    assert!(!fixed.internal);
}

#[test]
fn unsupported_schema_version_fails_closed() {
    let dir = TempDir::new().unwrap();
    {
        // Bootstrap normally then bump schema version.
        let store = MetadataStore::open(dir.path()).unwrap();
        drop(store);
        let db = Database::create(dir.path().join(METADATA_DB_FILE)).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut version = txn.open_table(SCHEMA_VERSION).unwrap();
            version.insert(SCHEMA_VERSION_KEY, 99u32).unwrap();
        }
        txn.commit().unwrap();
    }
    let err = match MetadataStore::open(dir.path()) {
        Ok(_) => panic!("expected UnsupportedSchema"),
        Err(e) => e,
    };
    assert!(matches!(err, StoreError::UnsupportedSchema(99)));
}

#[test]
fn queue_crud() {
    let (_dir, store) = open_tmp();

    let mut q = Queue::new(DEFAULT_VHOST, "q1");
    q.durable = true;
    q.exclusive = false;
    q.auto_delete = false;
    store.create_queue(&q).unwrap();

    let got = store.get_queue(DEFAULT_VHOST, "q1").unwrap().unwrap();
    assert!(got.durable);

    let err = store.create_queue(&q).unwrap_err();
    assert!(matches!(err, StoreError::QueueExists { .. }));

    q.auto_delete = true;
    store.put_queue(&q).unwrap();
    assert!(
        store
            .get_queue(DEFAULT_VHOST, "q1")
            .unwrap()
            .unwrap()
            .auto_delete
    );

    let listed = store.list_queues(DEFAULT_VHOST).unwrap();
    assert_eq!(listed.len(), 1);

    assert!(store.delete_queue(DEFAULT_VHOST, "q1").unwrap());
    assert!(store.list_queues(DEFAULT_VHOST).unwrap().is_empty());
    assert!(!store.delete_queue(DEFAULT_VHOST, "q1").unwrap());
}

#[test]
fn delete_vhost_cascades_queues_exchanges_and_permissions() {
    use queueforge_core::{Permission, User, UserTag};

    let (_dir, store) = open_tmp();
    store.create_vhost("gone").unwrap();
    store
        .create_exchange(&Exchange::new("gone", "ex", ExchangeType::Topic))
        .unwrap();
    store.create_queue(&Queue::new("gone", "q")).unwrap();

    // Sibling vhost data must survive cascade.
    store.create_vhost("keep").unwrap();
    store.create_queue(&Queue::new("keep", "q")).unwrap();

    let user = User::new("carol", "$argon2id$placeholder", vec![UserTag::Management]);
    store.create_user(&user).unwrap();
    store
        .put_permission(&Permission::full_access("carol", "gone"))
        .unwrap();
    store
        .put_permission(&Permission::full_access("carol", "keep"))
        .unwrap();

    assert!(store.delete_vhost("gone").unwrap());
    assert!(store.list_exchanges("gone").unwrap().is_empty());
    assert!(store.list_queues("gone").unwrap().is_empty());
    assert!(store.get_permission("carol", "gone").unwrap().is_none());
    // Sibling vhost perms + topology intact.
    assert!(store.get_permission("carol", "keep").unwrap().is_some());
    assert_eq!(store.list_exchanges("keep").unwrap().len(), 4);
    assert_eq!(store.list_queues("keep").unwrap().len(), 1);
    // Default vhost untouched.
    assert_eq!(store.list_exchanges(DEFAULT_VHOST).unwrap().len(), 4);
}

#[test]
fn list_is_scoped_to_vhost_via_range() {
    let (_dir, store) = open_tmp();
    store.create_vhost("a").unwrap();
    store.create_vhost("b").unwrap();
    store
        .create_exchange(&Exchange::new("a", "only-a", ExchangeType::Direct))
        .unwrap();
    store
        .create_exchange(&Exchange::new("b", "only-b", ExchangeType::Topic))
        .unwrap();
    store.create_queue(&Queue::new("a", "qa")).unwrap();
    store.create_queue(&Queue::new("b", "qb")).unwrap();

    let a_ex = store.list_exchanges("a").unwrap();
    assert!(a_ex.iter().any(|e| e.name == "only-a"));
    assert!(a_ex.iter().all(|e| e.vhost == "a"));
    assert!(!a_ex.iter().any(|e| e.name == "only-b"));

    let b_q = store.list_queues("b").unwrap();
    assert_eq!(b_q.len(), 1);
    assert_eq!(b_q[0].name, "qb");
}

#[test]
fn creates_data_dir_if_missing() {
    let dir = TempDir::new().unwrap();
    let nested = dir.path().join("a").join("b").join("data");
    assert!(!nested.exists());
    let store = MetadataStore::open(&nested).unwrap();
    assert!(nested.join(METADATA_DB_FILE).exists());
    assert_eq!(store.list_vhosts().unwrap().len(), 1);
}

#[test]
fn user_and_permission_crud() {
    use queueforge_core::{Permission, User, UserTag};

    let (_dir, store) = open_tmp();
    assert_eq!(store.user_count().unwrap(), 0);

    let user = User::new("alice", "$argon2id$placeholder", vec![UserTag::Management]);
    store.create_user(&user).unwrap();
    assert!(store.get_user("alice").unwrap().is_some());
    assert_eq!(store.user_count().unwrap(), 1);

    let err = store.create_user(&user).unwrap_err();
    assert!(matches!(err, StoreError::UserExists(_)));

    let perm = Permission::new("alice", DEFAULT_VHOST, ".*", "^q\\.", ".*");
    store.put_permission(&perm).unwrap();
    let got = store
        .get_permission("alice", DEFAULT_VHOST)
        .unwrap()
        .unwrap();
    assert_eq!(got.write, "^q\\.");

    // Unknown user / vhost rejected.
    let orphan = Permission::new("nobody", DEFAULT_VHOST, ".*", ".*", ".*");
    assert!(matches!(
        store.put_permission(&orphan).unwrap_err(),
        StoreError::UserNotFound(_)
    ));
    let bad_vh = Permission::new("alice", "missing", ".*", ".*", ".*");
    assert!(matches!(
        store.put_permission(&bad_vh).unwrap_err(),
        StoreError::VhostNotFound(_)
    ));

    assert!(store.delete_permission("alice", DEFAULT_VHOST).unwrap());
    assert!(store
        .get_permission("alice", DEFAULT_VHOST)
        .unwrap()
        .is_none());

    // Re-add permission then delete user cascades.
    store.put_permission(&perm).unwrap();
    assert!(store.delete_user("alice").unwrap());
    assert!(store.get_user("alice").unwrap().is_none());
    assert!(store
        .get_permission("alice", DEFAULT_VHOST)
        .unwrap()
        .is_none());
}

#[test]
fn create_user_with_permission_is_atomic() {
    use queueforge_core::{Permission, User, UserTag};

    let (_dir, store) = open_tmp();
    let user = User::new(
        "boot",
        "$argon2id$placeholder",
        vec![UserTag::Administrator],
    );
    let perm = Permission::full_access("boot", DEFAULT_VHOST);
    store.create_user_with_permission(&user, &perm).unwrap();
    assert!(store.get_user("boot").unwrap().is_some());
    assert_eq!(
        store
            .get_permission("boot", DEFAULT_VHOST)
            .unwrap()
            .unwrap()
            .configure,
        ".*"
    );

    // Missing vhost rolls back (no orphan user).
    let user2 = User::new("orphan", "$argon2id$placeholder", vec![]);
    let bad = Permission::full_access("orphan", "no-such-vhost");
    let err = store.create_user_with_permission(&user2, &bad).unwrap_err();
    assert!(matches!(err, StoreError::VhostNotFound(_)));
    assert!(store.get_user("orphan").unwrap().is_none());
}

#[test]
fn restores_missing_builtins_on_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let store = MetadataStore::open(dir.path()).unwrap();
        drop(store);
        let db = Database::create(dir.path().join(METADATA_DB_FILE)).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut exchanges = txn.open_table(EXCHANGES).unwrap();
            exchanges.remove((DEFAULT_VHOST, "amq.topic")).unwrap();
        }
        txn.commit().unwrap();
    }
    let store = MetadataStore::open(dir.path()).unwrap();
    let ex = store
        .get_exchange(DEFAULT_VHOST, "amq.topic")
        .unwrap()
        .expect("builtin restored");
    assert_eq!(ex.kind, ExchangeType::Topic);
}

#[test]
fn same_routing_key_headers_bindings_stay_distinct_across_reopen() {
    use compact_str::CompactString;

    let dir = TempDir::new().unwrap();
    let color_args = vec![(CompactString::from("color"), HeaderArg::Str("blue".into()))];
    let size_args = vec![(CompactString::from("size"), HeaderArg::Str("l".into()))];
    {
        let store = MetadataStore::open(dir.path()).unwrap();
        store
            .create_exchange(&Exchange::new("/", "hdr-dur", ExchangeType::Headers))
            .unwrap();
        let mut queue = Queue::new("/", "q2");
        queue.durable = true;
        store.create_queue(&queue).unwrap();

        let mut color = Binding::new("/", "hdr-dur", "q2", "");
        color.args = color_args.clone();
        let mut size = Binding::new("/", "hdr-dur", "q2", "");
        size.args = size_args.clone();
        store.put_binding(&color).unwrap();
        store.put_binding(&size).unwrap();

        let stored = store.list_bindings_for_exchange("/", "hdr-dur").unwrap();
        assert_eq!(stored.len(), 2, "args must keep sibling rows apart");

        let color_key = binding_args_key(&color.args);
        assert!(store
            .delete_binding("/", "hdr-dur", "q2", "", &color_key)
            .unwrap());
    }

    let store = MetadataStore::open(dir.path()).unwrap();
    let left = store.list_bindings_for_exchange("/", "hdr-dur").unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].args, size_args);
    let router = store.bootstrap_router().unwrap();
    let size_hit = router
        .route_publish(
            "/",
            "hdr-dur",
            "",
            &[(CompactString::from("size"), HeaderArg::Str("l".into()))],
        )
        .unwrap();
    assert_eq!(size_hit.destinations.len(), 1);
    let color_hit = router
        .route_publish(
            "/",
            "hdr-dur",
            "",
            &[(CompactString::from("color"), HeaderArg::Str("blue".into()))],
        )
        .unwrap();
    assert!(color_hit.destinations.is_empty());
}

#[test]
fn legacy_bindings_table_is_copied_on_reopen() {
    use compact_str::CompactString;
    use redb::{TableDefinition, TableHandle};

    let dir = TempDir::new().unwrap();
    {
        let store = MetadataStore::open(dir.path()).unwrap();
        store
            .create_exchange(&Exchange::new("/", "hdr-dur", ExchangeType::Headers))
            .unwrap();
        let mut queue = Queue::new("/", "q2");
        queue.durable = true;
        store.create_queue(&queue).unwrap();
        drop(store);

        let db = Database::create(dir.path().join(METADATA_DB_FILE)).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut binding = Binding::new("/", "hdr-dur", "q2", "");
            binding.args = vec![(CompactString::from("color"), HeaderArg::Str("blue".into()))];
            let bytes = serde_json::to_vec(&binding).unwrap();
            let mut old = txn
                .open_table(TableDefinition::<(&str, &str, &str, &str), &[u8]>::new(
                    "bindings",
                ))
                .unwrap();
            old.insert(("/", "hdr-dur", "q2", ""), bytes.as_slice())
                .unwrap();
        }
        txn.commit().unwrap();
    }

    let store = MetadataStore::open(dir.path()).unwrap();
    let router = store.bootstrap_router().unwrap();
    let hit = router
        .route_publish(
            "/",
            "hdr-dur",
            "",
            &[(CompactString::from("color"), HeaderArg::Str("blue".into()))],
        )
        .unwrap();
    assert_eq!(hit.destinations.len(), 1);
    assert!(
        !store
            .db
            .begin_read()
            .unwrap()
            .list_tables()
            .unwrap()
            .any(|table| table.name() == "bindings"),
        "legacy table must be consumed"
    );
}
