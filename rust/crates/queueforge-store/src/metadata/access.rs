//! Vhosts, users, and permissions.

use queueforge_core::{Permission, User, Vhost};
use redb::ReadableTable;
use tracing::debug;

use super::MetadataStore;
use crate::error::{Result, StoreError};
use crate::tables::{PERMISSIONS, USERS, VHOSTS};

impl MetadataStore {
    /// Create a vhost and its builtin exchanges.
    ///
    /// Returns [`StoreError::VhostExists`] if the name is already present.
    pub fn create_vhost(&self, name: &str) -> Result<Vhost> {
        let txn = self.write_txn()?;
        {
            let mut vhosts = txn.open_table(VHOSTS)?;
            let exists = vhosts.get(name)?.is_some();
            if exists {
                return Err(StoreError::VhostExists(name.to_string()));
            }
            let vhost = Vhost::new(name);
            let bytes = serde_json::to_vec(&vhost)?;
            vhosts.insert(name, bytes.as_slice())?;
        }
        Self::write_builtin_exchanges(&txn, name)?;
        txn.commit()?;
        debug!(vhost = name, "created vhost with builtin exchanges");
        Ok(Vhost::new(name))
    }

    /// Fetch a vhost by name.
    pub fn get_vhost(&self, name: &str) -> Result<Option<Vhost>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(VHOSTS)?;
        match table.get(name)? {
            Some(guard) => Ok(Some(serde_json::from_slice(guard.value())?)),
            None => Ok(None),
        }
    }

    /// List all vhosts, ordered by name (redb key order).
    pub fn list_vhosts(&self) -> Result<Vec<Vhost>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(VHOSTS)?;
        let mut out = Vec::new();
        for item in table.iter()? {
            let (_, value) = item?;
            out.push(serde_json::from_slice(value.value())?);
        }
        Ok(out)
    }

    /// Delete a vhost and all of its exchanges, queues, bindings, and permissions.
    ///
    /// Returns `true` if the vhost existed and was removed.
    pub fn delete_vhost(&self, name: &str) -> Result<bool> {
        let txn = self.write_txn()?;
        let existed = {
            let mut vhosts = txn.open_table(VHOSTS)?;
            let removed = vhosts.remove(name)?;
            removed.is_some()
        };
        if existed {
            Self::delete_bindings_for_vhost(&txn, name)?;
            Self::delete_exchanges_for_vhost(&txn, name)?;
            Self::delete_queues_for_vhost(&txn, name)?;
            Self::delete_permissions_for_vhost(&txn, name)?;
        }
        txn.commit()?;
        if existed {
            debug!(vhost = name, "deleted vhost");
        }
        Ok(existed)
    }

    /// Create a user. Fails if a user with the same name exists.
    pub fn create_user(&self, user: &User) -> Result<()> {
        let txn = self.write_txn()?;
        {
            let mut users = txn.open_table(USERS)?;
            let exists = users.get(user.name.as_str())?.is_some();
            if exists {
                return Err(StoreError::UserExists(user.name.to_string()));
            }
            let bytes = serde_json::to_vec(user)?;
            users.insert(user.name.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        debug!(user = %user.name, "created user");
        Ok(())
    }

    /// Atomically create a user and an initial permission row in one write txn.
    ///
    /// Used by admin bootstrap so a crash cannot leave an administrator without
    /// permissions. Fails if the user already exists or the permission's vhost
    /// is missing. `permission.user` must equal `user.name`.
    pub fn create_user_with_permission(&self, user: &User, permission: &Permission) -> Result<()> {
        if permission.user.as_str() != user.name.as_str() {
            return Err(StoreError::UserNotFound(permission.user.to_string()));
        }
        let txn = self.write_txn()?;
        {
            let mut users = txn.open_table(USERS)?;
            let exists = users.get(user.name.as_str())?.is_some();
            if exists {
                return Err(StoreError::UserExists(user.name.to_string()));
            }
            let bytes = serde_json::to_vec(user)?;
            users.insert(user.name.as_str(), bytes.as_slice())?;
        }
        {
            let vhosts = txn.open_table(VHOSTS)?;
            if vhosts.get(permission.vhost.as_str())?.is_none() {
                return Err(StoreError::VhostNotFound(permission.vhost.to_string()));
            }
        }
        {
            let mut perms = txn.open_table(PERMISSIONS)?;
            let key = (permission.user.as_str(), permission.vhost.as_str());
            let bytes = serde_json::to_vec(permission)?;
            perms.insert(key, bytes.as_slice())?;
        }
        txn.commit()?;
        debug!(
            user = %user.name,
            vhost = %permission.vhost,
            "created user with permission (atomic)"
        );
        Ok(())
    }

    /// Upsert a user (create or replace).
    pub fn put_user(&self, user: &User) -> Result<()> {
        let txn = self.write_txn()?;
        {
            let mut users = txn.open_table(USERS)?;
            let bytes = serde_json::to_vec(user)?;
            users.insert(user.name.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Fetch a user by name.
    pub fn get_user(&self, name: &str) -> Result<Option<User>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(USERS)?;
        match table.get(name)? {
            Some(guard) => Ok(Some(serde_json::from_slice(guard.value())?)),
            None => Ok(None),
        }
    }

    /// List all users, ordered by name (redb key order).
    pub fn list_users(&self) -> Result<Vec<User>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(USERS)?;
        let mut out = Vec::new();
        for item in table.iter()? {
            let (_, value) = item?;
            out.push(serde_json::from_slice(value.value())?);
        }
        Ok(out)
    }

    /// Delete a user and all of their permissions.
    ///
    /// Returns `true` if the user existed and was removed.
    pub fn delete_user(&self, name: &str) -> Result<bool> {
        let txn = self.write_txn()?;
        let existed = {
            let mut users = txn.open_table(USERS)?;
            let removed = users.remove(name)?;
            removed.is_some()
        };
        if existed {
            Self::delete_permissions_for_user(&txn, name)?;
        }
        txn.commit()?;
        if existed {
            debug!(user = name, "deleted user");
        }
        Ok(existed)
    }

    /// Number of users currently stored.
    pub fn user_count(&self) -> Result<usize> {
        let txn = self.read_txn()?;
        let table = txn.open_table(USERS)?;
        // redb Table does not expose len(); iterate.
        let mut n = 0usize;
        for item in table.iter()? {
            let _ = item?;
            n += 1;
        }
        Ok(n)
    }

    /// Set (create or replace) permissions for `(user, vhost)`.
    ///
    /// The user and vhost must already exist.
    pub fn put_permission(&self, permission: &Permission) -> Result<()> {
        let txn = self.write_txn()?;
        {
            let users = txn.open_table(USERS)?;
            if users.get(permission.user.as_str())?.is_none() {
                return Err(StoreError::UserNotFound(permission.user.to_string()));
            }
        }
        {
            let vhosts = txn.open_table(VHOSTS)?;
            if vhosts.get(permission.vhost.as_str())?.is_none() {
                return Err(StoreError::VhostNotFound(permission.vhost.to_string()));
            }
        }
        {
            let mut perms = txn.open_table(PERMISSIONS)?;
            let key = (permission.user.as_str(), permission.vhost.as_str());
            let bytes = serde_json::to_vec(permission)?;
            perms.insert(key, bytes.as_slice())?;
        }
        txn.commit()?;
        debug!(
            user = %permission.user,
            vhost = %permission.vhost,
            "set permissions"
        );
        Ok(())
    }

    /// Fetch permissions for `(user, vhost)`.
    pub fn get_permission(&self, user: &str, vhost: &str) -> Result<Option<Permission>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(PERMISSIONS)?;
        match table.get((user, vhost))? {
            Some(guard) => Ok(Some(serde_json::from_slice(guard.value())?)),
            None => Ok(None),
        }
    }

    /// List all permission entries.
    pub fn list_permissions(&self) -> Result<Vec<Permission>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(PERMISSIONS)?;
        let mut out = Vec::new();
        for item in table.iter()? {
            let (_, value) = item?;
            out.push(serde_json::from_slice(value.value())?);
        }
        Ok(out)
    }

    /// List permissions for a single user.
    pub fn list_permissions_for_user(&self, user: &str) -> Result<Vec<Permission>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(PERMISSIONS)?;
        let mut out = Vec::new();
        for item in table.range((user, "")..)? {
            let (key, value) = item?;
            let (u, _) = key.value();
            if u != user {
                break;
            }
            out.push(serde_json::from_slice(value.value())?);
        }
        Ok(out)
    }

    /// Delete permissions for `(user, vhost)`.
    ///
    /// Returns `true` if an entry existed and was removed.
    pub fn delete_permission(&self, user: &str, vhost: &str) -> Result<bool> {
        let txn = self.write_txn()?;
        let removed = {
            let mut perms = txn.open_table(PERMISSIONS)?;
            let guard = perms.remove((user, vhost))?;
            guard.is_some()
        };
        txn.commit()?;
        Ok(removed)
    }
}
