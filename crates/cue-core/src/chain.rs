use std::collections::HashMap;

use crate::job::EXIT_CODE_UNAVAILABLE;
use crate::pipeline::{ChainNode, JobPlan, ParallelOp, SerialOp};

/// Execution status of a single leaf job within a chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafStatus {
    Pending,
    Running,
    Done(i32),
    Failed(i32),
    Cancelled,
}

impl LeafStatus {
    /// Returns `true` if the leaf has reached a final state.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Done(_) | Self::Failed(_) | Self::Cancelled)
    }
}

/// Flattened representation of a chain leaf for easy lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlatLeaf {
    /// Index in the DFS-order leaf list.
    pub index: usize,
    /// Full job plan.
    pub plan: JobPlan,
    /// Human-readable pipeline text.
    pub pipeline_text: String,
}

impl FlatLeaf {
    /// First segment's command words.
    pub fn command(&self) -> &[String] {
        self.plan.first_command()
    }
}

/// Result of advancing a chain after a leaf reaches a terminal state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChainTransition {
    pub newly_ready: Vec<usize>,
    pub to_cancel: Vec<usize>,
}

/// Flatten a `ChainNode` into a list of leaves in left-to-right DFS order.
pub fn flatten_leaves(node: &ChainNode) -> Vec<FlatLeaf> {
    let mut out = Vec::new();
    flatten_leaves_inner(node, &mut out);
    out
}

fn flatten_leaves_inner(node: &ChainNode, out: &mut Vec<FlatLeaf>) {
    match node {
        ChainNode::Leaf(plan) => {
            out.push(FlatLeaf {
                index: out.len(),
                plan: plan.clone(),
                pipeline_text: plan.to_string(),
            });
        }
        ChainNode::Serial { left, right, .. } | ChainNode::Parallel { left, right, .. } => {
            flatten_leaves_inner(left, out);
            flatten_leaves_inner(right, out);
        }
    }
}

/// Determine which leaf indices are ready before any chain leaf has run.
pub fn initially_ready(node: &ChainNode) -> Vec<usize> {
    let mut ready = Vec::new();
    initially_ready_inner(node, 0, &mut ready);
    ready
}

fn initially_ready_inner(node: &ChainNode, offset: usize, ready: &mut Vec<usize>) {
    match node {
        ChainNode::Leaf(_) => ready.push(offset),
        ChainNode::Serial { left, .. } => initially_ready_inner(left, offset, ready),
        ChainNode::Parallel { left, right, .. } => {
            let left_count = left.leaf_count();
            initially_ready_inner(left, offset, ready);
            initially_ready_inner(right, offset + left_count, ready);
        }
    }
}

/// Advance a chain after `finished_idx` reaches a terminal state.
pub fn advance_chain(
    node: &ChainNode,
    finished_idx: usize,
    statuses: &HashMap<usize, LeafStatus>,
) -> ChainTransition {
    let mut transition = ChainTransition::default();
    advance_inner(node, 0, finished_idx, statuses, &mut transition);
    transition
}

fn advance_inner(
    node: &ChainNode,
    offset: usize,
    finished_idx: usize,
    statuses: &HashMap<usize, LeafStatus>,
    transition: &mut ChainTransition,
) {
    match node {
        ChainNode::Leaf(_) => {}
        ChainNode::Serial { left, op, right } => {
            let left_count = left.leaf_count();
            let left_range = offset..offset + left_count;
            let right_offset = offset + left_count;

            if left_range.contains(&finished_idx) {
                advance_inner(left, offset, finished_idx, statuses, transition);

                if all_leaves_terminal(left, offset, statuses) {
                    match op {
                        SerialOp::Then => {
                            if all_leaves_succeeded(left, offset, statuses) {
                                mark_ready(right, right_offset, statuses, transition);
                            } else {
                                mark_cancelled(right, right_offset, statuses, transition);
                            }
                        }
                        SerialOp::Always => mark_ready(right, right_offset, statuses, transition),
                    }
                }
            } else {
                advance_inner(right, right_offset, finished_idx, statuses, transition);
            }
        }
        ChainNode::Parallel { left, right, op } => {
            let left_count = left.leaf_count();
            let right_offset = offset + left_count;

            if finished_idx < right_offset {
                advance_inner(left, offset, finished_idx, statuses, transition);
            } else {
                advance_inner(right, right_offset, finished_idx, statuses, transition);
            }

            if *op == ParallelOp::Race {
                let right_count = right.leaf_count();
                let left_ok = all_indices_succeeded(offset..offset + left_count, statuses);
                let right_ok =
                    all_indices_succeeded(right_offset..right_offset + right_count, statuses);

                if left_ok || right_ok {
                    let cancel_range = if left_ok {
                        right_offset..right_offset + right_count
                    } else {
                        offset..offset + left_count
                    };
                    for index in cancel_range {
                        if !statuses
                            .get(&index)
                            .is_none_or(|status| status.is_terminal())
                        {
                            transition.to_cancel.push(index);
                        }
                    }
                }
            }
        }
    }
}

fn all_indices_succeeded(
    indices: impl Iterator<Item = usize>,
    statuses: &HashMap<usize, LeafStatus>,
) -> bool {
    indices
        .map(|index| statuses.get(&index))
        .all(|status| matches!(status, Some(LeafStatus::Done(0))))
}

/// Check whether every leaf in the chain has reached a terminal state.
pub fn is_chain_terminal(node: &ChainNode, statuses: &HashMap<usize, LeafStatus>) -> bool {
    all_leaves_terminal(node, 0, statuses)
}

fn all_leaves_terminal(
    node: &ChainNode,
    offset: usize,
    statuses: &HashMap<usize, LeafStatus>,
) -> bool {
    match node {
        ChainNode::Leaf(_) => statuses
            .get(&offset)
            .is_some_and(|status| status.is_terminal()),
        ChainNode::Serial { left, right, .. } | ChainNode::Parallel { left, right, .. } => {
            let left_count = left.leaf_count();
            all_leaves_terminal(left, offset, statuses)
                && all_leaves_terminal(right, offset + left_count, statuses)
        }
    }
}

fn all_leaves_succeeded(
    node: &ChainNode,
    offset: usize,
    statuses: &HashMap<usize, LeafStatus>,
) -> bool {
    match node {
        ChainNode::Leaf(_) => matches!(statuses.get(&offset), Some(LeafStatus::Done(0))),
        ChainNode::Serial { left, right, .. } | ChainNode::Parallel { left, right, .. } => {
            let left_count = left.leaf_count();
            all_leaves_succeeded(left, offset, statuses)
                && all_leaves_succeeded(right, offset + left_count, statuses)
        }
    }
}

fn mark_ready(
    node: &ChainNode,
    offset: usize,
    statuses: &HashMap<usize, LeafStatus>,
    transition: &mut ChainTransition,
) {
    match node {
        ChainNode::Leaf(_) => {
            if matches!(statuses.get(&offset), Some(LeafStatus::Pending) | None) {
                transition.newly_ready.push(offset);
            }
        }
        ChainNode::Serial { left, .. } => mark_ready(left, offset, statuses, transition),
        ChainNode::Parallel { left, right, .. } => {
            let left_count = left.leaf_count();
            mark_ready(left, offset, statuses, transition);
            mark_ready(right, offset + left_count, statuses, transition);
        }
    }
}

fn mark_cancelled(
    node: &ChainNode,
    offset: usize,
    statuses: &HashMap<usize, LeafStatus>,
    transition: &mut ChainTransition,
) {
    match node {
        ChainNode::Leaf(_) => {
            if matches!(statuses.get(&offset), Some(LeafStatus::Pending) | None) {
                transition.to_cancel.push(offset);
            }
        }
        ChainNode::Serial { left, right, .. } | ChainNode::Parallel { left, right, .. } => {
            let left_count = left.leaf_count();
            mark_cancelled(left, offset, statuses, transition);
            mark_cancelled(right, offset + left_count, statuses, transition);
        }
    }
}

/// Compute the chain exit code from terminal leaf statuses.
pub fn aggregate_chain_exit_code(node: &ChainNode, statuses: &HashMap<usize, LeafStatus>) -> i32 {
    let mut last = 0;
    for index in 0..node.leaf_count() {
        match statuses.get(&index) {
            Some(LeafStatus::Done(code)) => last = *code,
            Some(LeafStatus::Failed(code)) => return *code,
            Some(LeafStatus::Cancelled) => return EXIT_CODE_UNAVAILABLE,
            Some(LeafStatus::Pending | LeafStatus::Running) | None => return EXIT_CODE_UNAVAILABLE,
        }
    }
    last
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::JobPlan;

    fn leaf(command: &str) -> ChainNode {
        ChainNode::Leaf(JobPlan::simple(vec![command.into()]))
    }

    fn statuses(entries: &[(usize, LeafStatus)]) -> HashMap<usize, LeafStatus> {
        entries.iter().copied().collect()
    }

    #[test]
    fn flatten_leaves_preserves_dfs_order() {
        let chain = ChainNode::Serial {
            left: Box::new(leaf("a")),
            op: SerialOp::Then,
            right: Box::new(ChainNode::Parallel {
                left: Box::new(leaf("b")),
                op: ParallelOp::All,
                right: Box::new(leaf("c")),
            }),
        };

        let leaves = flatten_leaves(&chain);

        assert_eq!(
            leaves
                .iter()
                .map(|leaf| (leaf.index, leaf.pipeline_text.as_str()))
                .collect::<Vec<_>>(),
            vec![(0, "a"), (1, "b"), (2, "c")]
        );
    }

    #[test]
    fn initially_ready_respects_serial_and_parallel_boundaries() {
        let serial = ChainNode::Serial {
            left: Box::new(leaf("a")),
            op: SerialOp::Then,
            right: Box::new(leaf("b")),
        };
        let parallel = ChainNode::Parallel {
            left: Box::new(leaf("a")),
            op: ParallelOp::All,
            right: Box::new(leaf("b")),
        };

        assert_eq!(initially_ready(&serial), vec![0]);
        assert_eq!(initially_ready(&parallel), vec![0, 1]);
    }

    #[test]
    fn then_advances_right_only_after_success() {
        let chain = ChainNode::Serial {
            left: Box::new(leaf("a")),
            op: SerialOp::Then,
            right: Box::new(ChainNode::Parallel {
                left: Box::new(leaf("b")),
                op: ParallelOp::All,
                right: Box::new(leaf("c")),
            }),
        };

        let transition = advance_chain(&chain, 0, &statuses(&[(0, LeafStatus::Done(0))]));

        assert_eq!(transition.newly_ready, vec![1, 2]);
        assert!(transition.to_cancel.is_empty());
    }

    #[test]
    fn then_cancels_right_after_failure() {
        let chain = ChainNode::Serial {
            left: Box::new(leaf("a")),
            op: SerialOp::Then,
            right: Box::new(leaf("b")),
        };

        let transition = advance_chain(
            &chain,
            0,
            &statuses(&[(0, LeafStatus::Failed(2)), (1, LeafStatus::Pending)]),
        );

        assert!(transition.newly_ready.is_empty());
        assert_eq!(transition.to_cancel, vec![1]);
    }

    #[test]
    fn always_advances_after_failure() {
        let chain = ChainNode::Serial {
            left: Box::new(leaf("a")),
            op: SerialOp::Always,
            right: Box::new(leaf("b")),
        };

        let transition = advance_chain(
            &chain,
            0,
            &statuses(&[(0, LeafStatus::Failed(2)), (1, LeafStatus::Pending)]),
        );

        assert_eq!(transition.newly_ready, vec![1]);
        assert!(transition.to_cancel.is_empty());
    }

    #[test]
    fn race_waits_for_entire_branch_success() {
        let chain = ChainNode::Parallel {
            left: Box::new(ChainNode::Serial {
                left: Box::new(leaf("compile")),
                op: SerialOp::Then,
                right: Box::new(leaf("test")),
            }),
            op: ParallelOp::Race,
            right: Box::new(leaf("lint")),
        };

        let transition = advance_chain(
            &chain,
            0,
            &statuses(&[
                (0, LeafStatus::Done(0)),
                (1, LeafStatus::Pending),
                (2, LeafStatus::Running),
            ]),
        );

        assert_eq!(transition.newly_ready, vec![1]);
        assert!(transition.to_cancel.is_empty());
    }

    #[test]
    fn race_cancels_other_branch_after_branch_success() {
        let chain = ChainNode::Parallel {
            left: Box::new(ChainNode::Serial {
                left: Box::new(leaf("compile")),
                op: SerialOp::Then,
                right: Box::new(leaf("test")),
            }),
            op: ParallelOp::Race,
            right: Box::new(leaf("lint")),
        };

        let transition = advance_chain(
            &chain,
            1,
            &statuses(&[
                (0, LeafStatus::Done(0)),
                (1, LeafStatus::Done(0)),
                (2, LeafStatus::Running),
            ]),
        );

        assert!(transition.newly_ready.is_empty());
        assert_eq!(transition.to_cancel, vec![2]);
    }

    #[test]
    fn aggregate_exit_code_requires_terminal_success() {
        let chain = ChainNode::Serial {
            left: Box::new(leaf("a")),
            op: SerialOp::Always,
            right: Box::new(leaf("b")),
        };

        assert_eq!(
            aggregate_chain_exit_code(
                &chain,
                &statuses(&[(0, LeafStatus::Done(0)), (1, LeafStatus::Done(0))]),
            ),
            0
        );
        assert_eq!(
            aggregate_chain_exit_code(
                &chain,
                &statuses(&[(0, LeafStatus::Done(0)), (1, LeafStatus::Running)]),
            ),
            EXIT_CODE_UNAVAILABLE
        );
    }
}
