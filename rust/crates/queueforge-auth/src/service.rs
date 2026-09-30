//! Auth service: user verification, permission checks, bootstrap.

use queueforge_core::{Permission, User, UserTag, DEFAULT_VHOST};
use queueforge_store::MetadataStore;
use tracing::{info, warn};

use crate::error::{AuthError, Result};
use crate::password::{
    dummy_password_hash, hash_password, validate_password_policy, verify_password,
};
use crate::permission::{
    check_queue_bind, check_queue_unbind, check_user_permission, PermissionKind, ResourceKind,
};

/// Environment variable for bootstrap admin username.
pub const ENV_ADMIN_USER: &str = "QUEUEFORGE_ADMIN_USER";
/// Environment variable for bootstrap admin password.
pub const ENV_ADMIN_PASSWORD: &str = "QUEUEFORGE_ADMIN_PASSWORD";

/// Dev-bootstrap default username (only with `--dev-bootstrap`).
pub const DEV_BOOTSTRAP_USER: &str = "admin";
/// Dev-bootstrap default password (meets min length; local only).
pub const DEV_BOOTSTRAP_PASSWORD: &str = "devpassword12";

/// How to obtain bootstrap credentials when the user table is empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapMode {
    /// Require `QUEUEFORGE_ADMIN_USER` + `QUEUEFORGE_ADMIN_PASSWORD` (production).
    EnvRequired,
    /// Fall back to local dev credentials if env is unset (`--dev-bootstrap`).
    ///
    /// Never enables remote `guest`/`guest`; uses a documented 12+ char password.
    DevFallback,
}

/// Authentication / authorization service backed by [`MetadataStore`].
pub struct AuthService<'a> {
    store: &'a MetadataStore,
}

impl<'a> AuthService<'a> {
    /// Borrow the metadata store for auth operations.
    pub fn new(store: &'a MetadataStore) -> Self {
        Self { store }
    }

    /// Underlying store reference.
    pub fn store(&self) -> &MetadataStore {
        self.store
    }

    /// Create a user with a plaintext password (hashed with Argon2id).
    pub fn create_user(&self, name: &str, password: &str, tags: Vec<UserTag>) -> Result<User> {
        validate_username(name)?;
        let password_hash = hash_password(password)?;
        let user = User::new(name, password_hash, tags);
        self.store.create_user(&user)?;
        Ok(user)
    }

    /// Verify username + password. Returns the user on success, `None` on bad credentials.
    ///
    /// Missing users still run Argon2 against a dummy hash so the cost is
    /// comparable to a failed password (mitigates online username enumeration).
    pub fn authenticate(&self, name: &str, password: &str) -> Result<Option<User>> {
        match self.store.get_user(name)? {
            Some(user) => {
                if verify_password(password, &user.password_hash)? {
                    Ok(Some(user))
                } else {
                    Ok(None)
                }
            }
            None => {
                // Constant-ish work on missing user; ignore verify result.
                let _ = verify_password(password, dummy_password_hash())?;
                Ok(None)
            }
        }
    }

    /// Set permissions for `(user, vhost)` (regex triple).
    pub fn set_permission(
        &self,
        user: &str,
        vhost: &str,
        configure: &str,
        write: &str,
        read: &str,
    ) -> Result<()> {
        // Validate regexes early by attempting a no-op match compile path.
        let perm = Permission::new(user, vhost, configure, write, read);
        // Exercise compile via check against empty resource (errors on bad pattern).
        let _ = crate::permission::check_permission(
            &perm,
            "_",
            ResourceKind::Queue,
            PermissionKind::Configure,
        )?;
        let _ = crate::permission::check_permission(
            &perm,
            "_",
            ResourceKind::Queue,
            PermissionKind::Write,
        )?;
        let _ = crate::permission::check_permission(
            &perm,
            "_",
            ResourceKind::Queue,
            PermissionKind::Read,
        )?;
        self.store.put_permission(&perm)?;
        Ok(())
    }

    /// Check resource permission for `user` on `vhost` / `resource` / `kind`.
    ///
    /// Loads the permission row; missing row denies. Tags are not consulted.
    /// `resource_kind` controls empty-name normalization (exchanges only).
    pub fn check_permission(
        &self,
        user: &str,
        vhost: &str,
        resource: &str,
        resource_kind: ResourceKind,
        kind: PermissionKind,
    ) -> Result<bool> {
        let perm = self.store.get_permission(user, vhost)?;
        check_user_permission(perm.as_ref(), resource, resource_kind, kind)
    }

    /// Check `queue.bind` authorization (write on queue, read on exchange).
    pub fn check_queue_bind(
        &self,
        user: &str,
        vhost: &str,
        queue: &str,
        exchange: &str,
    ) -> Result<bool> {
        let Some(perm) = self.store.get_permission(user, vhost)? else {
            return Ok(false);
        };
        check_queue_bind(&perm, queue, exchange)
    }

    /// Check `queue.unbind` authorization (same as bind).
    pub fn check_queue_unbind(
        &self,
        user: &str,
        vhost: &str,
        queue: &str,
        exchange: &str,
    ) -> Result<bool> {
        let Some(perm) = self.store.get_permission(user, vhost)? else {
            return Ok(false);
        };
        check_queue_unbind(&perm, queue, exchange)
    }

    /// Bootstrap an administrator if the user table is empty.
    ///
    /// Credentials come from env (`QUEUEFORGE_ADMIN_USER` /
    /// `QUEUEFORGE_ADMIN_PASSWORD`), or from dev defaults when
    /// `mode == BootstrapMode::DevFallback`.
    ///
    /// The admin receives the `administrator` tag and full `.*` permissions on
    /// the default vhost `/`, written in a **single** store transaction so a
    /// crash cannot leave an admin without permissions. No remote `guest`/`guest`
    /// is ever created.
    ///
    /// Returns `true` if a user was created, `false` if users already exist.
    pub fn bootstrap_admin_if_empty(&self, mode: BootstrapMode) -> Result<bool> {
        if self.store.user_count()? > 0 {
            return Ok(false);
        }

        let (username, password, from_env) = match load_bootstrap_credentials(mode)? {
            Some(c) => c,
            None => return Err(AuthError::BootstrapCredentialsMissing),
        };

        self.bootstrap_admin(&username, &password, from_env)?;
        Ok(true)
    }

    /// Create the administrator + full-access permission atomically.
    fn bootstrap_admin(&self, username: &str, password: &str, from_env: bool) -> Result<User> {
        validate_username(username)?;
        // Policy enforced inside hash_password; re-validate for clearer bootstrap errors.
        validate_password_policy(password)?;

        let password_hash = hash_password(password)?;
        let user = User::new(username, password_hash, vec![UserTag::Administrator]);
        let permission = Permission::full_access(user.name.as_str(), DEFAULT_VHOST);
        self.store.create_user_with_permission(&user, &permission)?;

        if from_env {
            info!(
                user = %user.name,
                vhost = DEFAULT_VHOST,
                "bootstrapped administrator from environment"
            );
        } else {
            warn!(
                user = %user.name,
                "bootstrapped administrator with --dev-bootstrap defaults; \
                 set QUEUEFORGE_ADMIN_USER/PASSWORD and rotate before any remote exposure"
            );
        }
        Ok(user)
    }
}

/// `(username, password, from_env)`.
fn load_bootstrap_credentials(mode: BootstrapMode) -> Result<Option<(String, String, bool)>> {
    let env_user = std::env::var(ENV_ADMIN_USER).ok().filter(|s| !s.is_empty());
    let env_pass = std::env::var(ENV_ADMIN_PASSWORD)
        .ok()
        .filter(|s| !s.is_empty());
    resolve_bootstrap_credentials(mode, env_user, env_pass)
}

/// Resolve bootstrap credentials from already-read env values (testable).
///
/// Returns `Ok(None)` when production mode has no env credentials (caller
/// should surface [`AuthError::BootstrapCredentialsMissing`]).
pub fn resolve_bootstrap_credentials(
    mode: BootstrapMode,
    env_user: Option<String>,
    env_pass: Option<String>,
) -> Result<Option<(String, String, bool)>> {
    match (env_user, env_pass) {
        (Some(u), Some(p)) => Ok(Some((u, p, true))),
        (None, None) => match mode {
            BootstrapMode::EnvRequired => Ok(None),
            BootstrapMode::DevFallback => Ok(Some((
                DEV_BOOTSTRAP_USER.to_string(),
                DEV_BOOTSTRAP_PASSWORD.to_string(),
                false,
            ))),
        },
        // One of the two set but not both — treat as misconfiguration.
        _ => Err(AuthError::BootstrapCredentialsMissing),
    }
}

fn validate_username(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(AuthError::InvalidUsername(
            "username must not be empty".into(),
        ));
    }
    if name.contains('\0') {
        return Err(AuthError::InvalidUsername(
            "username must not contain NUL".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_auth() -> (TempDir, MetadataStore) {
        let dir = TempDir::new().unwrap();
        let store = MetadataStore::open(dir.path()).unwrap();
        (dir, store)
    }

    #[test]
    fn authenticate_and_check_permission() {
        let (_dir, store) = open_auth();
        let auth = AuthService::new(&store);
        auth.create_user("bob", "password1234", vec![UserTag::Management])
            .unwrap();
        auth.set_permission("bob", DEFAULT_VHOST, "^q\\.", "^q\\.", ".*")
            .unwrap();

        assert!(auth.authenticate("bob", "password1234").unwrap().is_some());
        assert!(auth
            .authenticate("bob", "wrong-password")
            .unwrap()
            .is_none());
        assert!(auth.authenticate("nope", "password1234").unwrap().is_none());

        assert!(auth
            .check_permission(
                "bob",
                DEFAULT_VHOST,
                "q.orders",
                ResourceKind::Queue,
                PermissionKind::Configure
            )
            .unwrap());
        assert!(!auth
            .check_permission(
                "bob",
                DEFAULT_VHOST,
                "other",
                ResourceKind::Queue,
                PermissionKind::Configure
            )
            .unwrap());
        assert!(auth
            .check_queue_bind("bob", DEFAULT_VHOST, "q.jobs", "amq.topic")
            .unwrap());
        assert!(!auth
            .check_queue_bind("bob", DEFAULT_VHOST, "not-q", "amq.topic")
            .unwrap());
    }

    #[test]
    fn bootstrap_dev_creates_admin_once_atomically() {
        let (_dir, store) = open_auth();
        let auth = AuthService::new(&store);
        assert!(auth
            .bootstrap_admin_if_empty(BootstrapMode::DevFallback)
            .unwrap());
        assert!(!auth
            .bootstrap_admin_if_empty(BootstrapMode::DevFallback)
            .unwrap());

        let user = auth
            .authenticate(DEV_BOOTSTRAP_USER, DEV_BOOTSTRAP_PASSWORD)
            .unwrap()
            .expect("dev admin");
        assert!(user.is_administrator());
        // Permission row must exist in the same bootstrap (atomic).
        assert!(store
            .get_permission(DEV_BOOTSTRAP_USER, DEFAULT_VHOST)
            .unwrap()
            .is_some());
        assert!(auth
            .check_permission(
                DEV_BOOTSTRAP_USER,
                DEFAULT_VHOST,
                "anything",
                ResourceKind::Queue,
                PermissionKind::Configure
            )
            .unwrap());
    }

    #[test]
    fn bootstrap_atomic_user_and_permission_via_store_api() {
        let (_dir, store) = open_auth();
        let user = User::new(
            "admin2",
            "$argon2id$placeholder",
            vec![UserTag::Administrator],
        );
        let perm = Permission::full_access("admin2", DEFAULT_VHOST);
        store.create_user_with_permission(&user, &perm).unwrap();
        assert!(store.get_user("admin2").unwrap().is_some());
        assert!(store
            .get_permission("admin2", DEFAULT_VHOST)
            .unwrap()
            .is_some());

        // Duplicate user fails without partial second write.
        let err = store.create_user_with_permission(&user, &perm).unwrap_err();
        assert!(matches!(err, queueforge_store::StoreError::UserExists(_)));
    }

    #[test]
    fn resolve_bootstrap_credentials_matrix() {
        // Production, no env → None (caller errors).
        assert!(matches!(
            resolve_bootstrap_credentials(BootstrapMode::EnvRequired, None, None),
            Ok(None)
        ));

        // Production, both set → env creds.
        let got = resolve_bootstrap_credentials(
            BootstrapMode::EnvRequired,
            Some("root".into()),
            Some("supersecret12".into()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(got.0, "root");
        assert_eq!(got.1, "supersecret12");
        assert!(got.2);

        // Production, only user → error.
        assert!(matches!(
            resolve_bootstrap_credentials(BootstrapMode::EnvRequired, Some("root".into()), None),
            Err(AuthError::BootstrapCredentialsMissing)
        ));
        // Production, only password → error.
        assert!(matches!(
            resolve_bootstrap_credentials(
                BootstrapMode::EnvRequired,
                None,
                Some("supersecret12".into())
            ),
            Err(AuthError::BootstrapCredentialsMissing)
        ));

        // Dev fallback, no env → defaults.
        let dev = resolve_bootstrap_credentials(BootstrapMode::DevFallback, None, None)
            .unwrap()
            .unwrap();
        assert_eq!(dev.0, DEV_BOOTSTRAP_USER);
        assert_eq!(dev.1, DEV_BOOTSTRAP_PASSWORD);
        assert!(!dev.2);

        // Dev fallback still prefers env when both set.
        let env_wins = resolve_bootstrap_credentials(
            BootstrapMode::DevFallback,
            Some("fromenv".into()),
            Some("fromenvpass12".into()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(env_wins.0, "fromenv");
        assert!(env_wins.2);
    }

    #[test]
    fn bootstrap_env_required_fails_when_credentials_missing() {
        let (_dir, store) = open_auth();
        let auth = AuthService::new(&store);
        // Drive EnvRequired through resolve path without depending on process env:
        // if env happens to be set, skip the full bootstrap call and only assert resolve.
        let resolved =
            resolve_bootstrap_credentials(BootstrapMode::EnvRequired, None, None).unwrap();
        assert!(resolved.is_none());

        // When ambient env is empty, full bootstrap must error.
        if std::env::var(ENV_ADMIN_USER).is_err() && std::env::var(ENV_ADMIN_PASSWORD).is_err() {
            let err = auth
                .bootstrap_admin_if_empty(BootstrapMode::EnvRequired)
                .unwrap_err();
            assert!(matches!(err, AuthError::BootstrapCredentialsMissing));
        }
    }
}
