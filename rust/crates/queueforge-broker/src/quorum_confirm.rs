//! Durable-majority rule for quorum publisher confirms.
//!
//! A persistent publish is confirmed only after a majority of the static member
//! list has the body in its own durable store. An in-memory copy does not count.
//! The AMQP path in [`crate::cluster::Cluster::quorum_enqueue`] calls
//! [`durable_majority`] after each member flush. Classic queues do not use this rule.

/// Where one cluster member holds a quorum publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberCopy {
    /// The body is only in memory. It does not count toward the confirm.
    MemoryOnly,
    /// The body is in that member's write-ahead log or SQLite file.
    Durable,
}

/// `members` is the static `[cluster].members` length.
///
/// Returns true when the number of [`MemberCopy::Durable`] entries is a majority
/// of `members`. Memory-only entries are ignored. A member list of one confirms
/// after that single durable copy.
pub fn durable_majority(members: usize, copies: &[MemberCopy]) -> bool {
    let need = members.max(1) / 2 + 1;
    copies
        .iter()
        .filter(|copy| *copy == &MemberCopy::Durable)
        .count()
        >= need
}

#[cfg(test)]
mod tests {
    use super::{durable_majority, MemberCopy};

    #[test]
    fn memory_copies_do_not_confirm_and_durable_copies_do() {
        let memory = [
            MemberCopy::MemoryOnly,
            MemberCopy::MemoryOnly,
            MemberCopy::Durable,
        ];
        assert!(
            !durable_majority(3, &memory),
            "one durable copy is not a majority of three"
        );
        let durable = [
            MemberCopy::Durable,
            MemberCopy::Durable,
            MemberCopy::MemoryOnly,
        ];
        assert!(
            durable_majority(3, &durable),
            "two durable copies confirm a three-member queue"
        );
        assert!(durable_majority(1, &[MemberCopy::Durable]));
        assert!(!durable_majority(1, &[MemberCopy::MemoryOnly]));
    }
}
