//! This is a transaction model implementation.
//!
//! # The Algorithm
//!
//! ## Voting
//!
//! The voting phase involves the transaction coordinator sending a prepare message to all the
//! transaction participants.
//!
//! ## Commit
//!
//! If the coordinator received an agreement message from all of them, the coordinator generates a
//! commit timestamp and sends a commit message to the participants.
//!
//! ## Rollback
//!
//! If the coordinator fails to receive an agreement message from all of them, the coordinator sends
//! a rollback message to all the participants.
//!
//! ## Check
//!
//! If a participant did not get any message from the coordinator, the participant checks the
//! transaction state by sending a check message to the coordinator.

use super::transaction::State as TransactionState;
use super::transaction::Transaction;

use std::hash::{Hash, Hasher};

use stateright::{Model, Property};

#[derive(Clone, Debug, Default, Eq)]
pub struct Node {
    /// The only transaction on the node.
    transaction: Transaction,

    /// The persistent storage of the node.
    #[allow(dead_code)]
    persistency: Vec<Action>,
}

impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.transaction.hash(state);
        for record in &self.persistency {
            record.hash(state);
        }
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.transaction == other.transaction && self.persistency == other.persistency
    }
}

#[derive(Clone, Debug, Eq)]
pub struct System {
    node_map: Vec<Node>,
}

impl Hash for System {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.node_map
            .iter()
            .enumerate()
            .for_each(|(node_id, node)| {
                node_id.hash(state);
                node.hash(state);
            });
    }
}

impl PartialEq for System {
    fn eq(&self, other: &Self) -> bool {
        self.node_map == other.node_map
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Action {
    /// Starts the transaction on the specified node.
    Start(usize),

    /// Makes a request of the coordinator node that the node wants to participate in the
    /// distributed transaction.
    RequestJoin(usize),

    /// Acknowledges the `RequestJoin` request.
    AckJoin(usize),

    /// Requests the participant node to prepare the transaction.
    RequestPrepare(usize),

    /// The participant node has prepared the transaction for commit.
    AckPrepare(usize),

    /// The participant node failed to prepare the transaction for commit.
    AckPrepareFail,

    /// Commits the transaction.
    Commit(usize),

    /// Commits the transaction.
    Rollback(usize),

    /// Crashes the node.
    Crash(usize),
}

/// [`TransactionModel`] implements state transitions.
#[derive(Hash)]
pub struct TransactionModel {
    /// The number of nodes.
    num_nodes: usize,
}

impl TransactionModel {
    /// Creates a new [`TransactionModel`].
    pub fn new(num_nodes: usize) -> TransactionModel {
        TransactionModel { num_nodes }
    }

    /// Determines if the node is the fixed transaction coordinator or not.
    pub const fn is_coordinator(node_id: usize) -> bool {
        node_id == 0
    }

    /// Returns the fixed coordinator node ID.
    pub const fn coordinator_node_id() -> usize {
        0
    }
}

impl TransactionModel {
    fn add_actions_per_node(node_id: usize, node: &Node, actions: &mut Vec<Action>) {
        match node.transaction.state() {
            TransactionState::Inactive => {
                if Self::is_coordinator(node_id) {
                    // If the transaction is inactive, has the node start the transaction.
                    actions.push(Action::Start(node_id));
                } else {
                    // Send a request to the coordinator to participate in the distributed transaction.
                    actions.push(Action::RequestJoin(node_id));
                }
            }
            TransactionState::Active => {
                if Self::is_coordinator(node_id) {
                    // Repeatedly send `AckJoin` to all the participants.
                    node.transaction
                        .for_each_participant(|participant_node_id| {
                            actions.push(Action::AckJoin(participant_node_id));
                        });

                    // The coordinator transaction decides when to start committing the transaction.
                    actions.push(Action::RequestPrepare(node_id));
                }

                // A transaction can be rolled back any time.
                actions.push(Action::Rollback(node_id));
            }
            TransactionState::Prepared => {
                if Self::is_coordinator(node_id) {
                    if node.transaction.is_all_prepared() {
                        // This model emulates the coordinator voting against the unanimous
                        // decision of the participants as sending `Rollback` to all the
                        // participants.
                        actions.push(Action::Commit(Self::coordinator_node_id()));
                        actions.push(Action::Rollback(Self::coordinator_node_id()));
                    } else {
                        // Send `RequestPrepare` repeatedly until it gets ACKs from all the
                        // participants.
                        node.transaction
                            .for_each_participant(|participant_node_id| {
                                actions.push(Action::RequestPrepare(participant_node_id));
                            });
                    }
                } else {
                    // This model emulates voting against commit as sending `AckPrepareFail` to the
                    // coordinator.
                    actions.push(Action::AckPrepare(node_id));
                    actions.push(Action::AckPrepareFail);
                }
            }
            TransactionState::Committed => {
                if Self::is_coordinator(node_id) {
                    // Send `Commit` messages to all the participants.
                    node.transaction
                        .for_each_participant(|participant_node_id| {
                            actions.push(Action::Commit(participant_node_id));
                        });
                }
            }
            TransactionState::RolledBack => {
                if Self::is_coordinator(node_id) {
                    // Send `Rollback` messages to all the participants.
                    node.transaction
                        .for_each_participant(|participant_node_id| {
                            actions.push(Action::Rollback(participant_node_id));
                        });
                }
            }
        }

        // Any node can crash any time.
        //
        // TODO: how to make it work??
        actions.push(Action::Crash(node_id));
    }

    fn next_system_state(last_state: &System, node_id: usize, next_node_state: Node) -> System {
        let mut next_node_map: Vec<Node> = last_state.node_map.clone();
        next_node_map[node_id] = next_node_state;
        System {
            node_map: next_node_map,
        }
    }

    fn start_transaction(node: &Node) -> Option<Node> {
        if node.transaction.state() == TransactionState::Inactive {
            let mut new_node_state = node.clone();
            new_node_state.transaction.start();
            Some(new_node_state)
        } else {
            None
        }
    }

    fn add_participant(node: &Node, participant_node_id: usize) -> Node {
        let mut new_node_state = node.clone();

        // Make sure that the transaction is active.
        if new_node_state.transaction.start() {
            new_node_state
                .transaction
                .add_participant(participant_node_id);
        }

        new_node_state
    }

    fn start_distributed_transaction(node: &Node) -> Node {
        let mut new_node_state = node.clone();

        // The coordinator knows that this node participates in the distributed transaction.
        new_node_state.transaction.start();

        new_node_state
    }

    fn prepare_distributed_transaction(node: &Node) -> Node {
        let mut new_node_state = node.clone();

        // Prepare the transaction for commit.
        new_node_state.transaction.prepare();

        new_node_state
    }

    fn mark_prepared(node: &Node, participant_node_id: usize) -> Node {
        let mut new_node_state = node.clone();
        new_node_state
            .transaction
            .report_prepared(participant_node_id);
        new_node_state
    }

    fn commit_distributed_transaction(node: &Node) -> Node {
        let mut new_node_state = node.clone();

        // Commit the transaction.
        new_node_state.transaction.commit();

        new_node_state
    }

    fn rollback_distributed_transaction(node: &Node) -> Node {
        let mut new_node_state = node.clone();

        // Rollback the transaction.
        new_node_state.transaction.rollback();

        new_node_state
    }

    fn crash_restart(node: &Node) -> Node {
        let mut new_node_state = node.clone();

        // Reset the transaction.
        new_node_state.transaction = Transaction::default();

        //
        // TODO: how to make it work??

        new_node_state
    }
}

impl Model for TransactionModel {
    type State = System;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        let mut node_map: Vec<Node> = Vec::with_capacity(self.num_nodes);
        for _ in 0..self.num_nodes {
            node_map.push(Node::default());
        }
        vec![System { node_map }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        state
            .node_map
            .iter()
            .enumerate()
            .for_each(|(node_id, node)| Self::add_actions_per_node(node_id, node, actions));
    }

    fn next_state(&self, last_state: &Self::State, action: Self::Action) -> Option<Self::State> {
        match action {
            Action::Start(node_id) => last_state
                .node_map
                .get(node_id)
                .and_then(Self::start_transaction)
                .map(|next_node_state| {
                    Self::next_system_state(last_state, node_id, next_node_state)
                }),
            Action::RequestJoin(participant_node_id) => last_state
                .node_map
                .get(Self::coordinator_node_id())
                .map(|node| Self::add_participant(node, participant_node_id))
                .map(|next_node_state| {
                    Self::next_system_state(
                        last_state,
                        Self::coordinator_node_id(),
                        next_node_state,
                    )
                }),
            Action::AckJoin(node_id) => last_state
                .node_map
                .get(node_id)
                .map(Self::start_distributed_transaction)
                .map(|next_node_state| {
                    Self::next_system_state(last_state, node_id, next_node_state)
                }),

            Action::RequestPrepare(node_id) => last_state
                .node_map
                .get(node_id)
                .map(Self::prepare_distributed_transaction)
                .map(|next_node_state| {
                    Self::next_system_state(last_state, node_id, next_node_state)
                }),
            Action::AckPrepare(participant_node_id) => last_state
                .node_map
                .get(Self::coordinator_node_id())
                .map(|node| Self::mark_prepared(node, participant_node_id))
                .map(|next_node_state| {
                    Self::next_system_state(
                        last_state,
                        Self::coordinator_node_id(),
                        next_node_state,
                    )
                }),
            Action::AckPrepareFail => last_state
                .node_map
                .get(Self::coordinator_node_id())
                .map(Self::rollback_distributed_transaction)
                .map(|next_node_state| {
                    Self::next_system_state(
                        last_state,
                        Self::coordinator_node_id(),
                        next_node_state,
                    )
                }),
            Action::Commit(node_id) => last_state
                .node_map
                .get(node_id)
                .map(Self::commit_distributed_transaction)
                .map(|next_node_state| {
                    Self::next_system_state(last_state, node_id, next_node_state)
                }),
            Action::Rollback(node_id) => last_state
                .node_map
                .get(node_id)
                .map(Self::rollback_distributed_transaction)
                .map(|next_node_state| {
                    Self::next_system_state(last_state, node_id, next_node_state)
                }),
            Action::Crash(node_id) => last_state
                .node_map
                .get(node_id)
                .map(Self::crash_restart)
                .map(|next_node_state| {
                    Self::next_system_state(last_state, node_id, next_node_state)
                }),
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![Property::<Self>::always("ACID", |_, state| {
            // If a transaction on a node has decided to commit or roll back, all the participant
            // transactions should agree on the decision.
            let mut commit_decided: Option<bool> = None;
            let mut not_unanimous = state.node_map.iter().any(|node| {
                let final_state = match node.transaction.state() {
                    TransactionState::Committed => Some(true),
                    TransactionState::RolledBack => Some(false),
                    _ => None,
                };
                match (final_state, commit_decided) {
                    (None, _) => false,
                    (Some(_), None) => {
                        commit_decided = final_state;
                        false
                    }
                    (Some(final_state), Some(other_state)) => {
                        // If they do not match, it is a consistency error.
                        final_state != other_state
                    }
                }
            });

            // If the coordinator has decided to commit, all the participants also should commit or
            // be prepared for commit.
            if !not_unanimous
                && state.node_map[Self::coordinator_node_id()]
                    .transaction
                    .state()
                    == TransactionState::Committed
            {
                state.node_map[Self::coordinator_node_id()]
                    .transaction
                    .for_each_participant(|participant_node_id| {
                        let state = state.node_map[participant_node_id].transaction.state();
                        if state != TransactionState::Prepared
                            && state != TransactionState::Committed
                        {
                            not_unanimous = true;
                        }
                    });
            }

            !not_unanimous
        })]
    }
}

#[cfg(test)]
mod model_checker {
    use super::{Model, TransactionModel};

    use std::num::NonZeroUsize;
    use std::thread::available_parallelism;

    use stateright::Checker;

    #[test]
    fn two_phase_commit() {
        let num_cpus = available_parallelism().map_or(1, NonZeroUsize::get);
        let checker = TransactionModel::new(3)
            .checker()
            .threads(num_cpus)
            .spawn_dfs()
            .join();
        checker.assert_properties();
    }
}
