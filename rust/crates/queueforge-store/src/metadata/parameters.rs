//! Runtime parameter rows: (component, vhost, name) → value.

use redb::ReadableTable;

use super::MetadataStore;
use crate::error::Result;
use crate::tables::PARAMETERS;

impl MetadataStore {
    /// Store one parameter, replacing any earlier value.
    pub fn put_parameter(&self, component: &str, vhost: &str, name: &str, value: &[u8]) -> Result<()> {
        let txn = self.write_txn()?;
        {
            let mut table = txn.open_table(PARAMETERS)?;
            table.insert((component, vhost, name), value)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// One parameter's value, or `None`.
    pub fn get_parameter(&self, component: &str, vhost: &str, name: &str) -> Result<Option<Vec<u8>>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(PARAMETERS)?;
        Ok(table.get((component, vhost, name))?.map(|v| v.value().to_vec()))
    }

    /// Remove one parameter. A missing row is not an error.
    pub fn delete_parameter(&self, component: &str, vhost: &str, name: &str) -> Result<()> {
        let txn = self.write_txn()?;
        {
            let mut table = txn.open_table(PARAMETERS)?;
            table.remove((component, vhost, name))?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Every `(vhost, name, value)` of one component.
    pub fn list_parameters(&self, component: &str) -> Result<Vec<(String, String, Vec<u8>)>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(PARAMETERS)?;
        let mut out = Vec::new();
        for item in table.range((component, "", "")..)? {
            let (key, value) = item?;
            let (c, vhost, name) = key.value();
            if c != component {
                break;
            }
            out.push((vhost.to_string(), name.to_string(), value.value().to_vec()));
        }
        Ok(out)
    }

    /// The components that have at least one parameter, sorted.
    pub fn list_parameter_components(&self) -> Result<Vec<String>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(PARAMETERS)?;
        let mut out: Vec<String> = Vec::new();
        for item in table.iter()? {
            let (key, _) = item?;
            let (c, _, _) = key.value();
            if out.last().map(String::as_str) != Some(c) {
                out.push(c.to_string());
            }
        }
        Ok(out)
    }
}
