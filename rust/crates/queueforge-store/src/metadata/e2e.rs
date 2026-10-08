//! Exchange-to-exchange binding rows.

use redb::ReadableTable;

use super::MetadataStore;
use crate::error::Result;
use crate::tables::EXCHANGE_BINDINGS;

impl MetadataStore {
    /// Store one exchange-to-exchange binding. An existing row is kept.
    pub fn put_exchange_binding(
        &self,
        vhost: &str,
        source: &str,
        destination: &str,
        routing_key: &str,
    ) -> Result<()> {
        let txn = self.write_txn()?;
        {
            let mut table = txn.open_table(EXCHANGE_BINDINGS)?;
            table.insert((vhost, source, destination, routing_key), [].as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Remove one exchange-to-exchange binding. A missing row is not an error.
    pub fn delete_exchange_binding(
        &self,
        vhost: &str,
        source: &str,
        destination: &str,
        routing_key: &str,
    ) -> Result<()> {
        let txn = self.write_txn()?;
        {
            let mut table = txn.open_table(EXCHANGE_BINDINGS)?;
            table.remove((vhost, source, destination, routing_key))?;
        }
        txn.commit()?;
        Ok(())
    }

    /// List `(source, destination, routing_key)` for every exchange-to-exchange binding in `vhost`.
    pub fn list_exchange_bindings(&self, vhost: &str) -> Result<Vec<(String, String, String)>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(EXCHANGE_BINDINGS)?;
        let mut out = Vec::new();
        for item in table.range((vhost, "", "", "")..)? {
            let (key, _) = item?;
            let (vh, source, destination, routing_key) = key.value();
            if vh != vhost {
                break;
            }
            out.push((source.to_string(), destination.to_string(), routing_key.to_string()));
        }
        Ok(out)
    }

    /// Remove every exchange-to-exchange binding with `exchange` at either end.
    pub(super) fn delete_exchange_bindings_touching(
        txn: &redb::WriteTransaction,
        vhost: &str,
        exchange: &str,
    ) -> Result<()> {
        let mut table = txn.open_table(EXCHANGE_BINDINGS)?;
        table.retain_in((vhost, "", "", "").., |k, _| {
            let (vh, source, destination, _) = k;
            vh != vhost || (source != exchange && destination != exchange)
        })?;
        Ok(())
    }
}
