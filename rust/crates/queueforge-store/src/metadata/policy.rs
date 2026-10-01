//! Policy rows in the metadata database.

use queueforge_core::Policy;
use redb::ReadableTable;

use super::MetadataStore;
use crate::error::Result;
use crate::tables::POLICIES;

impl MetadataStore {
    /// Insert or replace a policy row.
    pub fn put_policy(&self, policy: &Policy) -> Result<()> {
        let txn = self.write_txn()?;
        {
            let mut table = txn.open_table(POLICIES)?;
            let bytes = serde_json::to_vec(policy)?;
            table.insert(
                (policy.vhost.as_str(), policy.name.as_str()),
                bytes.as_slice(),
            )?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Delete a policy row. Returns whether a row was removed.
    pub fn delete_policy(&self, vhost: &str, name: &str) -> Result<bool> {
        let txn = self.write_txn()?;
        let removed = {
            let mut table = txn.open_table(POLICIES)?;
            let gone = table.remove((vhost, name))?.is_some();
            gone
        };
        txn.commit()?;
        Ok(removed)
    }

    /// Every stored policy.
    pub fn list_policies(&self) -> Result<Vec<Policy>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(POLICIES)?;
        let mut out = Vec::new();
        for item in table.iter()? {
            let (_, value) = item?;
            out.push(serde_json::from_slice(value.value())?);
        }
        Ok(out)
    }
}
