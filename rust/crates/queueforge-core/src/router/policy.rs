//! User and operator policies applied at route time.

use std::sync::Arc;

use compact_str::CompactString;

use super::ExchangeRouter;
use crate::error::{Error, Result};
use crate::policy::{
    apply_user_and_operator, effective_alternate, select_policy, Policy, PolicyTarget,
};
use crate::queue::QueueArgs;

impl ExchangeRouter {
    /// Insert or replace a policy. Rejects a pattern that is not a valid regex.
    pub fn upsert_policy(&self, policy: Policy) -> Result<()> {
        if regex::Regex::new(&policy.pattern).is_err() {
            return Err(Error::PreconditionFailed(format!(
                "invalid policy pattern '{}'",
                policy.pattern
            )));
        }
        let _g = self.write.lock().expect("router write lock poisoned");
        let mut list = (**self.policies.load()).clone();
        if let Some(slot) = list
            .iter()
            .position(|p| p.vhost == policy.vhost && p.name == policy.name)
        {
            list[slot] = policy;
        } else {
            list.push(policy);
        }
        self.policies.store(Arc::new(list));
        Ok(())
    }

    /// Remove a policy. Returns whether it existed.
    pub fn delete_policy(&self, vhost: &str, name: &str) -> bool {
        let _g = self.write.lock().expect("router write lock poisoned");
        let mut list = (**self.policies.load()).clone();
        let before = list.len();
        list.retain(|p| !(p.vhost.as_str() == vhost && p.name.as_str() == name));
        let removed = list.len() != before;
        self.policies.store(Arc::new(list));
        removed
    }

    /// Policies in one vhost, or every vhost when `vhost` is `None`.
    pub fn list_policies(&self, vhost: Option<&str>) -> Vec<Policy> {
        let list = self.policies.load();
        let mut out: Vec<Policy> = list
            .iter()
            .filter(|p| vhost.map(|v| p.vhost.as_str() == v).unwrap_or(true))
            .cloned()
            .collect();
        out.sort_by(|a, b| (&a.vhost, &a.name).cmp(&(&b.vhost, &b.name)));
        out
    }

    /// Insert or replace an operator policy.
    pub fn upsert_operator_policy(&self, policy: Policy) -> Result<()> {
        if regex::Regex::new(&policy.pattern).is_err() {
            return Err(Error::PreconditionFailed(format!(
                "invalid policy pattern '{}'",
                policy.pattern
            )));
        }
        let _g = self.write.lock().expect("router write lock poisoned");
        let mut list = (**self.operator_policies.load()).clone();
        if let Some(slot) = list
            .iter()
            .position(|p| p.vhost == policy.vhost && p.name == policy.name)
        {
            list[slot] = policy;
        } else {
            list.push(policy);
        }
        self.operator_policies.store(Arc::new(list));
        Ok(())
    }

    /// Remove an operator policy.
    pub fn delete_operator_policy(&self, vhost: &str, name: &str) -> bool {
        let _g = self.write.lock().expect("router write lock poisoned");
        let mut list = (**self.operator_policies.load()).clone();
        let before = list.len();
        list.retain(|p| !(p.vhost.as_str() == vhost && p.name.as_str() == name));
        let removed = list.len() != before;
        self.operator_policies.store(Arc::new(list));
        removed
    }

    /// Operator policies in one vhost, or every vhost when `vhost` is `None`.
    pub fn list_operator_policies(&self, vhost: Option<&str>) -> Vec<Policy> {
        let list = self.operator_policies.load();
        let mut out: Vec<Policy> = list
            .iter()
            .filter(|p| vhost.map(|v| p.vhost.as_str() == v).unwrap_or(true))
            .cloned()
            .collect();
        out.sort_by(|a, b| (&a.vhost, &a.name).cmp(&(&b.vhost, &b.name)));
        out
    }

    /// Name of the user policy and operator policy that match an exchange, if any.
    pub fn matching_exchange_policy_names(
        &self,
        vhost: &str,
        name: &str,
    ) -> (Option<String>, Option<String>) {
        let user = self.policies.load();
        let operator = self.operator_policies.load();
        (
            select_policy(user.as_slice(), vhost, name, PolicyTarget::Exchanges)
                .map(|p| p.name.to_string()),
            select_policy(operator.as_slice(), vhost, name, PolicyTarget::Exchanges)
                .map(|p| p.name.to_string()),
        )
    }

    /// Name of the user policy and operator policy that match a queue, if any.
    pub fn matching_policy_names(
        &self,
        vhost: &str,
        name: &str,
    ) -> (Option<String>, Option<String>) {
        let user = self.policies.load();
        let operator = self.operator_policies.load();
        (
            select_policy(user.as_slice(), vhost, name, PolicyTarget::Queues)
                .map(|p| p.name.to_string()),
            select_policy(operator.as_slice(), vhost, name, PolicyTarget::Queues)
                .map(|p| p.name.to_string()),
        )
    }

    /// Queue arguments after the matching policy fills keys the declare left unset.
    pub fn queue_args_with_policy(
        &self,
        vhost: &str,
        name: &str,
        declared: &QueueArgs,
    ) -> QueueArgs {
        let user = self.policies.load();
        let operator = self.operator_policies.load();
        apply_user_and_operator(
            declared,
            select_policy(user.as_slice(), vhost, name, PolicyTarget::Queues),
            select_policy(operator.as_slice(), vhost, name, PolicyTarget::Queues),
        )
    }

    /// Alternate exchange after policy fill. A non-empty declare argument wins.
    pub fn alternate_for(
        &self,
        vhost: &str,
        exchange: &str,
        declared: Option<&str>,
    ) -> Option<CompactString> {
        let operator = self.operator_policies.load();
        let from_operator = effective_alternate(
            declared,
            select_policy(
                operator.as_slice(),
                vhost,
                exchange,
                PolicyTarget::Exchanges,
            ),
        );
        if declared.filter(|s| !s.is_empty()).is_some() || from_operator.is_some() {
            return from_operator;
        }
        let list = self.policies.load();
        effective_alternate(
            declared,
            select_policy(list.as_slice(), vhost, exchange, PolicyTarget::Exchanges),
        )
    }
}
