use stateright::util::HashableHashMap;

/// [`Transaction`] represents a database transaction.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Transaction {
    state: State,
    participants: HashableHashMap<usize, bool>,
}

impl Transaction {
    /// Returns the state of the transaction.
    pub fn state(&self) -> State {
        self.state
    }

    /// Starts the transaction.
    ///
    /// Returns `true` if the transaction has started.
    pub fn start(&mut self) -> bool {
        if self.state == State::Inactive {
            self.state = State::Active;
            true
        } else {
            self.state == State::Active
        }
    }

    /// Adds a new participant to the transaction.
    ///
    /// Returns `true` if the participant was newly added to the transaction.
    pub fn add_participant(&mut self, node_id: usize) -> bool {
        self.participants.insert(node_id, false).is_none()
    }

    /// Executes the supplied closure on each participant node id.
    pub fn for_each_participant<F: FnMut(usize)>(&self, mut f: F) {
        self.participants
            .iter()
            .for_each(|(node_id, _prepared)| f(*node_id));
    }

    /// Returns `true` if all the participants have prepared for commit.
    pub fn is_all_prepared(&self) -> bool {
        !self.participants.iter().any(|(_, prepared)| !*prepared)
    }

    /// Prepares the transaction for commit.
    ///
    /// Returns `true` if the transaction is prepared for commit.
    pub fn prepare(&mut self) -> bool {
        if self.state == State::Active {
            self.state = State::Prepared;
            true
        } else {
            self.state == State::Prepared
        }
    }

    /// Reports that the node has prepared the transaction for commit.
    ///
    /// Returns `true` if the participant is marked prepared.
    pub fn report_prepared(&mut self, node_id: usize) -> bool {
        if self.state != State::Prepared {
            return false;
        }
        self.participants
            .get_mut(&node_id)
            .map_or(false, |prepared| {
                if *prepared {
                    false
                } else {
                    *prepared = true;
                    true
                }
            })
    }

    /// Commits the transaction.
    pub fn commit(&mut self) {
        if self.state == State::Prepared {
            self.state = State::Committed;
        }
    }

    /// Rolls back the transaction.
    pub fn rollback(&mut self) {
        if !self.state.is_terminal() {
            self.state = State::RolledBack;
        }
    }
}

impl Default for Transaction {
    fn default() -> Self {
        Self {
            state: State::Inactive,
            participants: HashableHashMap::default(),
        }
    }
}

/// The state of a transaction is expressed as [`State`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum State {
    /// The transaction has not started.
    Inactive,

    /// The transaction is running.
    Active,

    /// The transaction is prepared for commit.
    Prepared,

    /// The transaction is committed.
    Committed,

    /// The transaction is rolled back.
    RolledBack,
}

impl State {
    /// Returns `true` if it is a terminal state.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Committed | Self::RolledBack)
    }
}

#[cfg(test)]
mod test {
    use super::Transaction;

    use quickcheck::quickcheck;

    quickcheck! { fn prop_api_safety(xs: Vec<usize>) -> bool { check_api_safety(&xs) } }

    fn check_api_safety(seq: &[usize]) -> bool {
        let mut transaction = Transaction::default();
        !seq.iter().any(|op_code| {
            // Returns `true` if it detects anything illegal.
            match op_code % 4 {
                0 => {
                    // 0 => start.
                    !transaction.start()
                }
                1 => {
                    // 1 => prepare.
                    transaction.prepare();
                    false
                }
                2 => {
                    // 2 => commit.
                    transaction.commit();
                    false
                }
                _ => {
                    // 3 => rollback.
                    transaction.rollback();
                    false
                }
            }
        })
    }
}
