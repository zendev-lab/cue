//! Scheduler actor — command routing, ID assignment, chain execution, cron timer heap.

use std::collections::{HashMap, HashSet};

use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use cue_core::ipc::{CronInfo, JobInfo, OkPayload, ResponsePayload, error_code};
use cue_core::job::{CancelReason, JobStatus};
use cue_core::pipeline::{ChainNode, ParallelOp, SerialOp};
use cue_core::{AgentId, ChainId, CronId, JobId, ScopeHash};

use crate::parser::resolver::ResolvedCommand;

use super::{ActorSystem, GatewayMsg, ProcessMgrMsg, SchedulerMsg, ScopeStoreMsg};

// ── Leaf status within a chain ──────────────────────────────────────────────

/// Status of a single leaf (pipeline) within a chain.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LeafStatus {
    Pending,
    Running,
    Done(i32),
    Failed(i32),
    Cancelled,
}

impl LeafStatus {
    /// Returns `true` if the leaf has reached a final state.
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            LeafStatus::Done(_) | LeafStatus::Failed(_) | LeafStatus::Cancelled
        )
    }
}

// ── Chain state ─────────────────────────────────────────────────────────────

/// Tracks a running chain's execution state.
struct ChainState {
    #[allow(dead_code)]
    chain_id: ChainId,
    #[allow(dead_code)]
    client_id: u64,
    #[allow(dead_code)]
    request_id: u32,
    node: ChainNode,
    /// Maps each leaf index (0-based, left-to-right DFS) to its `JobId`.
    leaf_jobs: HashMap<usize, JobId>,
    /// Maps each leaf index to its current status.
    leaf_status: HashMap<usize, LeafStatus>,
    scope_hash: ScopeHash,
}

/// Flattened representation of a chain leaf for easy lookup.
struct FlatLeaf {
    /// Index in the DFS-order leaf list.
    index: usize,
    /// Command words for the first segment (used to spawn the job).
    command: Vec<String>,
    /// Human-readable pipeline text.
    pipeline_text: String,
}

// ── Job tracking ────────────────────────────────────────────────────────────

/// Scheduler-side view of every spawned job.
struct JobEntry {
    job_id: JobId,
    pipeline_text: String,
    status: JobStatus,
    exit_code: Option<i32>,
    #[allow(dead_code)]
    chain_id: Option<ChainId>,
}

// ── Cron entry ──────────────────────────────────────────────────────────────

/// A registered cron / timer entry.
struct CronEntry {
    cron_id: CronId,
    schedule_text: String,
    chain: ChainNode,
    scope_hash: ScopeHash,
    enabled: bool,
    next_trigger: Instant,
    /// Interval between triggers (`None` for one-shot entries).
    interval: Option<std::time::Duration>,
}

// ── Scheduler state (all mutable state lives here) ──────────────────────────

struct SchedulerState {
    next_job: u32,
    next_agent: u32,
    next_cron: u32,
    next_chain: u32,

    /// Active chains keyed by `ChainId`.
    chains: HashMap<ChainId, ChainState>,
    /// Reverse lookup: `JobId` → `(ChainId, leaf_index)`.
    job_to_chain: HashMap<JobId, (ChainId, usize)>,
    /// All jobs the scheduler knows about.
    jobs: HashMap<JobId, JobEntry>,
    /// Registered cron entries.
    crons: HashMap<CronId, CronEntry>,
}

impl SchedulerState {
    fn new() -> Self {
        Self {
            next_job: 1,
            next_agent: 1,
            next_cron: 1,
            next_chain: 1,
            chains: HashMap::new(),
            job_to_chain: HashMap::new(),
            jobs: HashMap::new(),
            crons: HashMap::new(),
        }
    }

    fn alloc_job(&mut self) -> JobId {
        let id = JobId(self.next_job);
        self.next_job += 1;
        id
    }

    fn alloc_agent(&mut self) -> AgentId {
        let id = AgentId(self.next_agent);
        self.next_agent += 1;
        id
    }

    fn alloc_cron(&mut self) -> CronId {
        let id = CronId(self.next_cron);
        self.next_cron += 1;
        id
    }

    fn alloc_chain(&mut self) -> ChainId {
        let id = ChainId(self.next_chain);
        self.next_chain += 1;
        id
    }
}

// ── Spawn the actor ─────────────────────────────────────────────────────────

/// Spawn the Scheduler actor task.
pub fn spawn(mut rx: mpsc::Receiver<SchedulerMsg>, sys: ActorSystem) {
    tokio::spawn(async move {
        let mut state = SchedulerState::new();
        debug!("scheduler: started");

        loop {
            // Compute the sleep deadline from the nearest enabled cron trigger.
            let next_cron_deadline = state
                .crons
                .values()
                .filter(|c| c.enabled)
                .map(|c| c.next_trigger)
                .min();

            let sleep = match next_cron_deadline {
                Some(deadline) => tokio::time::sleep_until(deadline),
                // No crons → sleep "forever" (will be cancelled by select).
                None => tokio::time::sleep(std::time::Duration::from_secs(86400 * 365)),
            };
            tokio::pin!(sleep);

            tokio::select! {
                biased;

                msg = rx.recv() => {
                    let Some(msg) = msg else { break };
                    match msg {
                        SchedulerMsg::Eval { client_id, request_id, command } => {
                            let response = handle_command(command, &mut state, &sys).await;
                            let _ = sys.gateway.send(GatewayMsg::SendResponse {
                                client_id,
                                request_id,
                                payload: response,
                            }).await;
                        }

                        SchedulerMsg::JobFinished { job_id, exit_code } => {
                            handle_job_finished(job_id, exit_code, &mut state, &sys).await;
                        }

                        SchedulerMsg::Shutdown => {
                            debug!("scheduler: shutting down");

                            // FIX 4: Cancel all active chain jobs before shutting down.
                            let chain_ids: Vec<ChainId> =
                                state.chains.keys().copied().collect();
                            for chain_id in chain_ids {
                                if let Some(chain) = state.chains.get(&chain_id) {
                                    let leaf_indices: Vec<usize> =
                                        chain.leaf_status.keys().copied().collect();
                                    for idx in leaf_indices {
                                        let chain = state.chains.get(&chain_id).unwrap();
                                        let status = chain.leaf_status.get(&idx).cloned();
                                        match status {
                                            Some(LeafStatus::Running) => {
                                                if let Some(&jid) =
                                                    chain.leaf_jobs.get(&idx)
                                                {
                                                    let _ = sys
                                                        .process_mgr
                                                        .send(ProcessMgrMsg::KillJob {
                                                            job_id: jid,
                                                        })
                                                        .await;
                                                    if let Some(entry) =
                                                        state.jobs.get_mut(&jid)
                                                    {
                                                        entry.status =
                                                            JobStatus::Cancelled(
                                                                CancelReason::ChainAborted,
                                                            );
                                                    }
                                                }
                                                let chain =
                                                    state.chains.get_mut(&chain_id).unwrap();
                                                chain.leaf_status
                                                    .insert(idx, LeafStatus::Cancelled);
                                            }
                                            Some(LeafStatus::Pending) => {
                                                let chain =
                                                    state.chains.get_mut(&chain_id).unwrap();
                                                chain.leaf_status
                                                    .insert(idx, LeafStatus::Cancelled);
                                                if let Some(&jid) =
                                                    chain.leaf_jobs.get(&idx)
                                                    && let Some(entry) =
                                                        state.jobs.get_mut(&jid)
                                                {
                                                    entry.status =
                                                        JobStatus::Cancelled(
                                                            CancelReason::ChainAborted,
                                                        );
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                // Remove chain tracking.
                                if let Some(finished) = state.chains.remove(&chain_id) {
                                    for jid in finished.leaf_jobs.values() {
                                        state.job_to_chain.remove(jid);
                                    }
                                }
                            }

                            break;
                        }
                    }
                }

                () = &mut sleep => {
                    // A cron timer has fired.
                    fire_due_crons(&mut state, &sys).await;
                }
            }
        }

        debug!("scheduler: stopped");
    });
}

// ── Chain helpers ────────────────────────────────────────────────────────────

/// Count the number of leaf nodes (Pipelines) in a `ChainNode`.
fn leaf_count(node: &ChainNode) -> usize {
    match node {
        ChainNode::Leaf(_) => 1,
        ChainNode::Serial { left, right, .. } | ChainNode::Parallel { left, right, .. } => {
            leaf_count(left) + leaf_count(right)
        }
    }
}

/// Flatten a `ChainNode` into a list of `FlatLeaf` entries (DFS, left-to-right).
fn flatten_leaves(node: &ChainNode) -> Vec<FlatLeaf> {
    let mut out = Vec::new();
    flatten_leaves_inner(node, &mut out);
    out
}

fn flatten_leaves_inner(node: &ChainNode, out: &mut Vec<FlatLeaf>) {
    match node {
        ChainNode::Leaf(pipeline) => {
            let idx = out.len();
            let command = pipeline
                .segments
                .first()
                .map(|s| s.command.clone())
                .unwrap_or_default();
            let pipeline_text = pipeline_to_text(pipeline);
            out.push(FlatLeaf {
                index: idx,
                command,
                pipeline_text,
            });
        }
        ChainNode::Serial { left, right, .. } | ChainNode::Parallel { left, right, .. } => {
            flatten_leaves_inner(left, out);
            flatten_leaves_inner(right, out);
        }
    }
}

/// Convert a `Pipeline` to a human-readable string.
fn pipeline_to_text(pipeline: &cue_core::pipeline::Pipeline) -> String {
    pipeline
        .segments
        .iter()
        .map(|s| {
            let cmd = s.command.join(" ");
            match s.pipe_to_next {
                Some(cue_core::pipeline::PipeOp::Stdout) => format!("{cmd} |>"),
                Some(cue_core::pipeline::PipeOp::StdoutStderr) => format!("{cmd} |&>"),
                Some(cue_core::pipeline::PipeOp::StderrOnly) => format!("{cmd} |!>"),
                None => cmd,
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Convert a full `ChainNode` to text.
fn chain_to_text(node: &ChainNode) -> String {
    match node {
        ChainNode::Leaf(p) => pipeline_to_text(p),
        ChainNode::Serial { left, op, right } => {
            let op_str = match op {
                SerialOp::Then => "->",
                SerialOp::Always => "~>",
            };
            format!("{} {op_str} {}", chain_to_text(left), chain_to_text(right))
        }
        ChainNode::Parallel { left, op, right } => {
            let op_str = match op {
                ParallelOp::All => "||",
                ParallelOp::Race => "||?",
            };
            format!("{} {op_str} {}", chain_to_text(left), chain_to_text(right))
        }
    }
}

/// Determine which leaf indices are *initially ready* given the chain structure.
///
/// Returns a `Vec<usize>` of leaf indices that should be spawned immediately.
fn initially_ready(node: &ChainNode) -> Vec<usize> {
    let mut ready = Vec::new();
    initially_ready_inner(node, 0, &mut ready);
    ready
}

fn initially_ready_inner(node: &ChainNode, offset: usize, ready: &mut Vec<usize>) {
    match node {
        ChainNode::Leaf(_) => {
            ready.push(offset);
        }
        ChainNode::Serial { left, .. } => {
            // Only the left subtree is ready initially.
            initially_ready_inner(left, offset, ready);
        }
        ChainNode::Parallel { left, right, .. } => {
            // Both subtrees are ready.
            let left_count = leaf_count(left);
            initially_ready_inner(left, offset, ready);
            initially_ready_inner(right, offset + left_count, ready);
        }
    }
}

/// After a leaf finishes, determine which new leaves become ready
/// and whether any should be cancelled.
///
/// Returns `(newly_ready, to_cancel)` leaf indices.
fn advance_chain(
    node: &ChainNode,
    finished_idx: usize,
    statuses: &HashMap<usize, LeafStatus>,
) -> (Vec<usize>, Vec<usize>) {
    let mut ready = Vec::new();
    let mut cancel = Vec::new();
    advance_inner(node, 0, finished_idx, statuses, &mut ready, &mut cancel);
    (ready, cancel)
}

fn advance_inner(
    node: &ChainNode,
    offset: usize,
    finished_idx: usize,
    statuses: &HashMap<usize, LeafStatus>,
    ready: &mut Vec<usize>,
    cancel: &mut Vec<usize>,
) {
    match node {
        ChainNode::Leaf(_) => {
            // Nothing to advance for a bare leaf.
        }
        ChainNode::Serial { left, op, right } => {
            let left_count = leaf_count(left);
            let left_range = offset..offset + left_count;
            let right_offset = offset + left_count;

            if left_range.contains(&finished_idx) {
                // Finished leaf is in the left subtree. Recurse into left.
                advance_inner(left, offset, finished_idx, statuses, ready, cancel);

                // Check if the entire left subtree is complete.
                if all_leaves_terminal(left, offset, statuses) {
                    match op {
                        SerialOp::Then => {
                            // Right runs only if all left leaves succeeded (exit 0).
                            if all_leaves_succeeded(left, offset, statuses) {
                                mark_ready(right, right_offset, statuses, ready);
                            } else {
                                mark_cancelled(right, right_offset, statuses, cancel);
                            }
                        }
                        SerialOp::Always => {
                            // Right always runs after left completes.
                            mark_ready(right, right_offset, statuses, ready);
                        }
                    }
                }
            } else {
                // Finished leaf is in the right subtree. Recurse into right.
                advance_inner(right, right_offset, finished_idx, statuses, ready, cancel);
            }
        }
        ChainNode::Parallel { left, right, op } => {
            let left_count = leaf_count(left);
            let right_offset = offset + left_count;

            // Recurse into the subtree that owns the finished leaf.
            if finished_idx < right_offset {
                advance_inner(left, offset, finished_idx, statuses, ready, cancel);
            } else {
                advance_inner(right, right_offset, finished_idx, statuses, ready, cancel);
            }

            // FIX 3: For Race, check entire branch success (subtree terminal + all ok),
            // not individual leaf success.
            if *op == ParallelOp::Race {
                let right_count = leaf_count(right);
                let left_terminal = (offset..offset + left_count)
                    .all(|i| statuses.get(&i).is_some_and(|s| s.is_terminal()));
                let left_ok = left_terminal
                    && (offset..offset + left_count)
                        .all(|i| matches!(statuses.get(&i), Some(LeafStatus::Done(0))));

                let right_terminal = (right_offset..right_offset + right_count)
                    .all(|i| statuses.get(&i).is_some_and(|s| s.is_terminal()));
                let right_ok = right_terminal
                    && (right_offset..right_offset + right_count)
                        .all(|i| matches!(statuses.get(&i), Some(LeafStatus::Done(0))));

                if left_ok || right_ok {
                    // Cancel the OTHER branch's pending/running leaves.
                    let cancel_range = if left_ok {
                        right_offset..right_offset + right_count
                    } else {
                        offset..offset + left_count
                    };
                    for i in cancel_range {
                        if !statuses.get(&i).is_none_or(|s| s.is_terminal()) {
                            cancel.push(i);
                        }
                    }
                }
            }
        }
    }
}

/// Check whether every leaf in the subtree has reached a terminal state.
fn all_leaves_terminal(
    node: &ChainNode,
    offset: usize,
    statuses: &HashMap<usize, LeafStatus>,
) -> bool {
    match node {
        ChainNode::Leaf(_) => matches!(
            statuses.get(&offset),
            Some(LeafStatus::Done(_) | LeafStatus::Failed(_) | LeafStatus::Cancelled)
        ),
        ChainNode::Serial { left, right, .. } | ChainNode::Parallel { left, right, .. } => {
            let left_count = leaf_count(left);
            all_leaves_terminal(left, offset, statuses)
                && all_leaves_terminal(right, offset + left_count, statuses)
        }
    }
}

/// Check whether every leaf in the subtree succeeded (exit code 0).
fn all_leaves_succeeded(
    node: &ChainNode,
    offset: usize,
    statuses: &HashMap<usize, LeafStatus>,
) -> bool {
    match node {
        ChainNode::Leaf(_) => matches!(statuses.get(&offset), Some(LeafStatus::Done(0))),
        ChainNode::Serial { left, right, .. } | ChainNode::Parallel { left, right, .. } => {
            let left_count = leaf_count(left);
            all_leaves_succeeded(left, offset, statuses)
                && all_leaves_succeeded(right, offset + left_count, statuses)
        }
    }
}

/// Mark all pending leaves in the subtree as ready.
fn mark_ready(
    node: &ChainNode,
    offset: usize,
    statuses: &HashMap<usize, LeafStatus>,
    ready: &mut Vec<usize>,
) {
    match node {
        ChainNode::Leaf(_) => {
            if matches!(statuses.get(&offset), Some(LeafStatus::Pending) | None) {
                ready.push(offset);
            }
        }
        ChainNode::Serial { left, .. } => {
            // Only the left side is initially ready.
            mark_ready(left, offset, statuses, ready);
        }
        ChainNode::Parallel { left, right, .. } => {
            let left_count = leaf_count(left);
            mark_ready(left, offset, statuses, ready);
            mark_ready(right, offset + left_count, statuses, ready);
        }
    }
}

/// Mark all pending leaves in the subtree as cancelled.
fn mark_cancelled(
    node: &ChainNode,
    offset: usize,
    statuses: &HashMap<usize, LeafStatus>,
    cancel: &mut Vec<usize>,
) {
    match node {
        ChainNode::Leaf(_) => {
            if matches!(statuses.get(&offset), Some(LeafStatus::Pending) | None) {
                cancel.push(offset);
            }
        }
        ChainNode::Serial { left, right, .. } | ChainNode::Parallel { left, right, .. } => {
            let left_count = leaf_count(left);
            mark_cancelled(left, offset, statuses, cancel);
            mark_cancelled(right, offset + left_count, statuses, cancel);
        }
    }
}

// ── Cron schedule parsing (V1: simple durations) ────────────────────────────

/// Parse a simple schedule string, returning `(duration, one_shot)`.
///
/// Supported formats:
/// - `every <N><unit>` → repeating interval
/// - `in <N><unit>` → one-shot delay
///
/// Units: `s` (seconds), `m` (minutes), `h` (hours), `d` (days).
fn parse_schedule(text: &str) -> Option<(std::time::Duration, bool)> {
    let text = text.trim();
    let (rest, one_shot) = if let Some(r) = text.strip_prefix("every ") {
        (r.trim(), false)
    } else if let Some(r) = text.strip_prefix("in ") {
        (r.trim(), true)
    } else {
        return None;
    };
    let dur = parse_duration(rest)?;
    Some((dur, one_shot))
}

/// Parse a bare duration like `5m`, `30s`, `1h`, `2d`.
fn parse_duration(s: &str) -> Option<std::time::Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (num_part, unit) = s.split_at(s.len() - 1);
    let n: u64 = num_part.parse().ok()?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        "d" => n * 86400,
        _ => return None,
    };
    Some(std::time::Duration::from_secs(secs))
}

// ── Cron trigger logic ──────────────────────────────────────────────────────

/// Fire all crons whose `next_trigger` has passed.
async fn fire_due_crons(state: &mut SchedulerState, sys: &ActorSystem) {
    let now = Instant::now();
    // Collect cron IDs to fire (avoid borrow conflict).
    let due: Vec<CronId> = state
        .crons
        .values()
        .filter(|c| c.enabled && c.next_trigger <= now)
        .map(|c| c.cron_id)
        .collect();

    for cron_id in due {
        let Some(entry) = state.crons.get(&cron_id) else {
            continue;
        };
        let chain = entry.chain.clone();
        let scope_hash = entry.scope_hash;
        let is_oneshot = entry.interval.is_none();

        info!(%cron_id, "scheduler: cron triggered");

        // Spawn the chain just like `:run`.
        spawn_chain(chain, scope_hash, 0, 0, state, sys).await;

        if is_oneshot {
            state.crons.remove(&cron_id);
            debug!(%cron_id, "scheduler: one-shot cron removed");
        } else if let Some(CronEntry {
            interval: Some(interval),
            next_trigger,
            ..
        }) = state.crons.get_mut(&cron_id)
        {
            *next_trigger = now + *interval;
        }
    }
}

// ── Spawn chain / single job ────────────────────────────────────────────────

/// Spawn a chain (or a single job) from a `ChainNode`, returning the response payload.
async fn spawn_chain(
    chain: ChainNode,
    scope_hash: ScopeHash,
    client_id: u64,
    request_id: u32,
    state: &mut SchedulerState,
    sys: &ActorSystem,
) -> ResponsePayload {
    let leaves = flatten_leaves(&chain);

    if leaves.len() == 1 {
        // Single leaf → spawn as a plain job, no chain tracking.
        let leaf = &leaves[0];
        let jid = state.alloc_job();

        state.jobs.insert(
            jid,
            JobEntry {
                job_id: jid,
                pipeline_text: leaf.pipeline_text.clone(),
                status: JobStatus::Running,
                exit_code: None,
                chain_id: None,
            },
        );

        info!(%jid, pipeline = %leaf.pipeline_text, "scheduler: spawning single job");

        let _ = sys
            .process_mgr
            .send(ProcessMgrMsg::SpawnJob {
                job_id: jid,
                command_line: leaf.command.clone(),
                scope_hash,
            })
            .await;

        return ResponsePayload::Ok(OkPayload::JobCreated {
            job_id: jid.to_string(),
        });
    }

    // Multi-leaf chain.
    let chain_id = state.alloc_chain();
    let ready_indices = initially_ready(&chain);

    let mut leaf_jobs = HashMap::new();
    let mut leaf_status: HashMap<usize, LeafStatus> = HashMap::new();

    // Initialize all leaves as Pending.
    for leaf in &leaves {
        leaf_status.insert(leaf.index, LeafStatus::Pending);
    }

    // Assign JobIds to ready leaves and mark them Running.
    let mut spawned_job_ids = Vec::new();
    for &idx in &ready_indices {
        let jid = state.alloc_job();
        leaf_jobs.insert(idx, jid);
        leaf_status.insert(idx, LeafStatus::Running);

        state.jobs.insert(
            jid,
            JobEntry {
                job_id: jid,
                pipeline_text: leaves[idx].pipeline_text.clone(),
                status: JobStatus::Running,
                exit_code: None,
                chain_id: Some(chain_id),
            },
        );
        state.job_to_chain.insert(jid, (chain_id, idx));
        spawned_job_ids.push(jid);
    }

    let chain_state = ChainState {
        chain_id,
        client_id,
        request_id,
        node: chain,
        leaf_jobs,
        leaf_status,
        scope_hash,
    };
    state.chains.insert(chain_id, chain_state);

    // Spawn the ready jobs via ProcessMgr.
    for &jid in &spawned_job_ids {
        let (chain_id_val, leaf_idx) = state.job_to_chain[&jid];
        let chain_st = &state.chains[&chain_id_val];
        let leaf = &leaves[leaf_idx];
        info!(%chain_id_val, %jid, leaf_idx, pipeline = %leaf.pipeline_text, "scheduler: spawning chain leaf");
        let _ = sys
            .process_mgr
            .send(ProcessMgrMsg::SpawnJob {
                job_id: jid,
                command_line: leaf.command.clone(),
                scope_hash: chain_st.scope_hash,
            })
            .await;
    }

    ResponsePayload::Ok(OkPayload::ChainCreated {
        chain_id: chain_id.to_string(),
        job_ids: spawned_job_ids.iter().map(|j| j.to_string()).collect(),
    })
}

// ── Job finished handler ────────────────────────────────────────────────────

async fn handle_job_finished(
    job_id: JobId,
    exit_code: i32,
    state: &mut SchedulerState,
    sys: &ActorSystem,
) {
    info!(%job_id, exit_code, "scheduler: job finished");

    // FIX 2: If the job is already in a terminal state (e.g. Killed or Cancelled
    // by :kill/:cancel), don't overwrite the status — just update exit_code.
    if let Some(entry) = state.jobs.get_mut(&job_id) {
        if entry.status.is_terminal() {
            entry.exit_code = Some(exit_code);
        } else if exit_code == 0 {
            entry.status = JobStatus::Done;
            entry.exit_code = Some(exit_code);
        } else {
            entry.status = JobStatus::Failed;
            entry.exit_code = Some(exit_code);
        }
    }

    // If this job belongs to a chain, advance the chain.
    let Some(&(chain_id, leaf_idx)) = state.job_to_chain.get(&job_id) else {
        return;
    };

    // Update chain leaf status.
    // FIX 2: If leaf_status is already terminal (set by :kill/:cancel), keep it.
    {
        let Some(chain) = state.chains.get_mut(&chain_id) else {
            return;
        };
        let current = chain.leaf_status.get(&leaf_idx);
        if !current.is_some_and(|s| s.is_terminal()) {
            let leaf_st = if exit_code == 0 {
                LeafStatus::Done(exit_code)
            } else {
                LeafStatus::Failed(exit_code)
            };
            chain.leaf_status.insert(leaf_idx, leaf_st);
        }
    }

    // Determine newly ready and cancelled leaves.
    let (newly_ready, to_cancel) = {
        let chain = &state.chains[&chain_id];
        advance_chain(&chain.node, leaf_idx, &chain.leaf_status)
    };

    // Delegate cancellation, spawning, and cleanup to shared helper.
    process_chain_advance(chain_id, &newly_ready, &to_cancel, state, sys).await;
}

/// Shared logic for processing chain advancement results (cancels + spawns + cleanup).
///
/// Used by `handle_job_finished`, `:kill`, and `:cancel` handlers.
async fn process_chain_advance(
    chain_id: ChainId,
    newly_ready: &[usize],
    to_cancel: &[usize],
    state: &mut SchedulerState,
    sys: &ActorSystem,
) {
    let cancel_set: HashSet<usize> = to_cancel.iter().copied().collect();

    // Cancel leaves.
    for &idx in to_cancel {
        let Some(chain) = state.chains.get_mut(&chain_id) else {
            return;
        };
        if matches!(chain.leaf_status.get(&idx), Some(LeafStatus::Running))
            && let Some(&jid) = chain.leaf_jobs.get(&idx)
        {
            let _ = sys
                .process_mgr
                .send(ProcessMgrMsg::KillJob { job_id: jid })
                .await;
            if let Some(entry) = state.jobs.get_mut(&jid) {
                entry.status = JobStatus::Cancelled(CancelReason::ChainAborted);
            }
        }
        chain.leaf_status.insert(idx, LeafStatus::Cancelled);
    }

    // Spawn newly ready leaves (filtering out cancelled ones).
    let leaves = {
        let Some(chain) = state.chains.get(&chain_id) else {
            return;
        };
        flatten_leaves(&chain.node)
    };

    for &idx in newly_ready {
        if cancel_set.contains(&idx) {
            continue;
        }
        let jid = state.alloc_job();
        let chain = state.chains.get_mut(&chain_id).unwrap();
        chain.leaf_jobs.insert(idx, jid);
        chain.leaf_status.insert(idx, LeafStatus::Running);
        let scope_hash = chain.scope_hash;

        state.job_to_chain.insert(jid, (chain_id, idx));
        state.jobs.insert(
            jid,
            JobEntry {
                job_id: jid,
                pipeline_text: leaves[idx].pipeline_text.clone(),
                status: JobStatus::Running,
                exit_code: None,
                chain_id: Some(chain_id),
            },
        );

        info!(%chain_id, %jid, leaf_idx = idx, "scheduler: spawning next chain leaf");
        let _ = sys
            .process_mgr
            .send(ProcessMgrMsg::SpawnJob {
                job_id: jid,
                command_line: leaves[idx].command.clone(),
                scope_hash,
            })
            .await;
    }

    // Check if chain is fully complete.
    if let Some(chain) = state.chains.get(&chain_id)
        && all_leaves_terminal(&chain.node, 0, &chain.leaf_status)
    {
        info!(%chain_id, "scheduler: chain complete");
        let finished = state.chains.remove(&chain_id).unwrap();
        for jid in finished.leaf_jobs.values() {
            state.job_to_chain.remove(jid);
        }
    }
}

// ── Command dispatch ────────────────────────────────────────────────────────

async fn handle_command(
    cmd: ResolvedCommand,
    state: &mut SchedulerState,
    sys: &ActorSystem,
) -> ResponsePayload {
    match cmd {
        ResolvedCommand::Run { chain, .. } => {
            // Get current HEAD scope hash.
            let scope_hash = match get_head_scope(sys).await {
                Ok(h) => h,
                Err(resp) => return resp,
            };
            spawn_chain(chain, scope_hash, 0, 0, state, sys).await
        }

        ResolvedCommand::Ask { text, .. } => {
            let aid = state.alloc_agent();
            info!(%aid, %text, "scheduler: agent spawned (stub)");
            ResponsePayload::Ok(OkPayload::AgentSpawned {
                agent_id: aid.to_string(),
            })
        }

        ResolvedCommand::Cron {
            schedule_text,
            chain,
            ..
        } => {
            let Some((dur, one_shot)) = parse_schedule(&schedule_text) else {
                return ResponsePayload::err(
                    error_code::INVALID_SYNTAX,
                    format!("cannot parse schedule: {schedule_text}"),
                );
            };

            let scope_hash = match get_head_scope(sys).await {
                Ok(h) => h,
                Err(resp) => return resp,
            };

            let cron_id = state.alloc_cron();
            let now = Instant::now();
            let entry = CronEntry {
                cron_id,
                schedule_text: schedule_text.clone(),
                chain,
                scope_hash,
                enabled: true,
                next_trigger: now + dur,
                interval: if one_shot { None } else { Some(dur) },
            };
            state.crons.insert(cron_id, entry);

            info!(%cron_id, %schedule_text, "scheduler: cron added");
            ResponsePayload::Ok(OkPayload::CronAdded {
                cron_id: cron_id.to_string(),
            })
        }

        ResolvedCommand::Spawn { text, .. } => {
            let aid = state.alloc_agent();
            info!(%aid, %text, "scheduler: executor spawned (stub)");
            ResponsePayload::Ok(OkPayload::AgentSpawned {
                agent_id: aid.to_string(),
            })
        }

        ResolvedCommand::Kill { id } => {
            if let Some(jid) = parse_job_id(&id) {
                if let Some(entry) = state.jobs.get_mut(&jid) {
                    if entry.status == JobStatus::Running {
                        let _ = sys
                            .process_mgr
                            .send(ProcessMgrMsg::KillJob { job_id: jid })
                            .await;
                        entry.status = JobStatus::Killed;
                        info!(%jid, "scheduler: job killed");

                        // FIX 2: Update chain leaf_status so that a later
                        // JobFinished won't overwrite the Killed status.
                        if let Some(&(chain_id, leaf_idx)) = state.job_to_chain.get(&jid) {
                            if let Some(chain) = state.chains.get_mut(&chain_id) {
                                chain.leaf_status.insert(leaf_idx, LeafStatus::Done(-1));
                            }
                            // Advance the chain so downstream leaves react.
                            let (newly_ready, to_cancel) = {
                                let chain = &state.chains[&chain_id];
                                advance_chain(&chain.node, leaf_idx, &chain.leaf_status)
                            };
                            process_chain_advance(chain_id, &newly_ready, &to_cancel, state, sys)
                                .await;
                        }

                        return ResponsePayload::ack();
                    }
                    return ResponsePayload::err(
                        error_code::INVALID_STATE,
                        format!("job {jid} is not running"),
                    );
                }
                ResponsePayload::err(error_code::NOT_FOUND, format!("job {id} not found"))
            } else {
                warn!(%id, "scheduler: kill target not found");
                ResponsePayload::err(error_code::NOT_FOUND, format!("{id} not found"))
            }
        }

        ResolvedCommand::Cancel { id } => {
            if let Some(jid) = parse_job_id(&id) {
                if let Some(entry) = state.jobs.get_mut(&jid) {
                    match entry.status {
                        JobStatus::Pending => {
                            entry.status = JobStatus::Cancelled(CancelReason::User);
                            info!(%jid, "scheduler: job cancelled (was pending)");

                            // FIX 2: Update chain leaf_status and advance chain.
                            if let Some(&(chain_id, leaf_idx)) = state.job_to_chain.get(&jid) {
                                if let Some(chain) = state.chains.get_mut(&chain_id) {
                                    chain.leaf_status.insert(leaf_idx, LeafStatus::Cancelled);
                                }
                                let (newly_ready, to_cancel) = {
                                    let chain = &state.chains[&chain_id];
                                    advance_chain(&chain.node, leaf_idx, &chain.leaf_status)
                                };
                                process_chain_advance(
                                    chain_id,
                                    &newly_ready,
                                    &to_cancel,
                                    state,
                                    sys,
                                )
                                .await;
                            }

                            return ResponsePayload::ack();
                        }
                        JobStatus::Running => {
                            let _ = sys
                                .process_mgr
                                .send(ProcessMgrMsg::KillJob { job_id: jid })
                                .await;
                            entry.status = JobStatus::Cancelled(CancelReason::User);
                            info!(%jid, "scheduler: job cancelled (was running)");

                            // FIX 2: Update chain leaf_status and advance chain.
                            if let Some(&(chain_id, leaf_idx)) = state.job_to_chain.get(&jid) {
                                if let Some(chain) = state.chains.get_mut(&chain_id) {
                                    chain.leaf_status.insert(leaf_idx, LeafStatus::Cancelled);
                                }
                                let (newly_ready, to_cancel) = {
                                    let chain = &state.chains[&chain_id];
                                    advance_chain(&chain.node, leaf_idx, &chain.leaf_status)
                                };
                                process_chain_advance(
                                    chain_id,
                                    &newly_ready,
                                    &to_cancel,
                                    state,
                                    sys,
                                )
                                .await;
                            }

                            return ResponsePayload::ack();
                        }
                        _ => {
                            return ResponsePayload::err(
                                error_code::INVALID_STATE,
                                format!("job {jid} is already terminal"),
                            );
                        }
                    }
                }
                ResponsePayload::err(error_code::NOT_FOUND, format!("job {id} not found"))
            } else {
                ResponsePayload::err(error_code::NOT_FOUND, format!("{id} not found"))
            }
        }

        ResolvedCommand::Pause { id } => {
            if let Some(cid) = parse_cron_id(&id) {
                if let Some(entry) = state.crons.get_mut(&cid) {
                    entry.enabled = false;
                    info!(%cid, "scheduler: cron paused");
                    return ResponsePayload::ack();
                }
                ResponsePayload::err(error_code::NOT_FOUND, format!("cron {id} not found"))
            } else {
                ResponsePayload::err(
                    error_code::NOT_SUPPORTED,
                    "pause only supports cron IDs (C<n>)",
                )
            }
        }

        ResolvedCommand::Resume { id } => {
            if let Some(cid) = parse_cron_id(&id) {
                if let Some(entry) = state.crons.get_mut(&cid) {
                    entry.enabled = true;
                    // Reschedule from now if interval-based.
                    if let Some(interval) = entry.interval {
                        entry.next_trigger = Instant::now() + interval;
                    }
                    info!(%cid, "scheduler: cron resumed");
                    return ResponsePayload::ack();
                }
                ResponsePayload::err(error_code::NOT_FOUND, format!("cron {id} not found"))
            } else {
                ResponsePayload::err(
                    error_code::NOT_SUPPORTED,
                    "resume only supports cron IDs (C<n>)",
                )
            }
        }

        ResolvedCommand::Jobs => {
            let list: Vec<JobInfo> = state
                .jobs
                .values()
                .map(|e| JobInfo {
                    id: e.job_id.to_string(),
                    status: e.status.clone(),
                    pipeline: e.pipeline_text.clone(),
                    exit_code: e.exit_code,
                })
                .collect();
            ResponsePayload::Ok(OkPayload::JobList(list))
        }

        ResolvedCommand::Agents => ResponsePayload::Ok(OkPayload::AgentList(vec![])),

        ResolvedCommand::Crons => {
            let list: Vec<CronInfo> = state
                .crons
                .values()
                .map(|c| CronInfo {
                    id: c.cron_id.to_string(),
                    schedule: c.schedule_text.clone(),
                    command: chain_to_text(&c.chain),
                    enabled: c.enabled,
                })
                .collect();
            ResponsePayload::Ok(OkPayload::CronList(list))
        }

        ResolvedCommand::Scopes => ResponsePayload::Ok(OkPayload::EvalText {
            text: "scope listing not yet implemented".into(),
        }),

        ResolvedCommand::Help { topic } => {
            let text = match topic.as_deref() {
                Some(t) => format!("help for '{t}' — not yet implemented"),
                None => "cue-shell help — not yet implemented".into(),
            };
            ResponsePayload::Ok(OkPayload::EvalText { text })
        }

        ResolvedCommand::Clear => ResponsePayload::ack(),

        ResolvedCommand::Quit => {
            info!("scheduler: quit requested, initiating shutdown");
            let _ = sys.gateway.send(GatewayMsg::Shutdown).await;
            ResponsePayload::ack()
        }

        ResolvedCommand::Cd { path } => {
            let delta = cue_core::scope::EnvDelta {
                set: std::collections::BTreeMap::new(),
                unset: vec![],
                cwd: Some(std::path::PathBuf::from(&path)),
            };
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = sys
                .scope_store
                .send(ScopeStoreMsg::Fork { delta, reply: tx })
                .await;
            match rx.await {
                Ok(Ok(hash)) => ResponsePayload::Ok(OkPayload::ScopeCreated {
                    hash: hash.to_string(),
                    label: Some(format!("cd {path}")),
                }),
                Ok(Err(e)) => ResponsePayload::err(error_code::INTERNAL, e.to_string()),
                Err(_) => ResponsePayload::err(error_code::INTERNAL, "scope_store unreachable"),
            }
        }

        // Stubs for commands not yet implemented.
        ResolvedCommand::Retry { id }
        | ResolvedCommand::Out { id }
        | ResolvedCommand::Err { id }
        | ResolvedCommand::Fg { id }
        | ResolvedCommand::Wait { id }
        | ResolvedCommand::Send { id }
        | ResolvedCommand::Probe { id } => {
            warn!(%id, "scheduler: command not yet implemented");
            ResponsePayload::err(error_code::NOT_SUPPORTED, "command not yet implemented")
        }

        _ => {
            warn!("scheduler: unhandled command variant");
            ResponsePayload::err(error_code::NOT_SUPPORTED, "command not yet implemented")
        }
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Get the HEAD scope hash from the scope store.
async fn get_head_scope(sys: &ActorSystem) -> Result<ScopeHash, ResponsePayload> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = sys
        .scope_store
        .send(ScopeStoreMsg::GetHead { reply: tx })
        .await;
    rx.await
        .map_err(|_| ResponsePayload::err(error_code::INTERNAL, "scope_store unreachable"))
}

/// Parse a string like `"J5"` into a `JobId`.
fn parse_job_id(s: &str) -> Option<JobId> {
    let s = s.trim();
    s.strip_prefix('J')
        .and_then(|n| n.parse::<u32>().ok())
        .map(JobId)
}

/// Parse a string like `"C3"` into a `CronId`.
fn parse_cron_id(s: &str) -> Option<CronId> {
    let s = s.trim();
    s.strip_prefix('C')
        .and_then(|n| n.parse::<u32>().ok())
        .map(CronId)
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use cue_core::pipeline::{PipeSegment, Pipeline};
    use tokio::sync::mpsc;

    /// Helper: build a simple leaf from a command string.
    fn leaf(cmd: &str) -> ChainNode {
        ChainNode::Leaf(Pipeline {
            segments: vec![PipeSegment {
                command: cmd.split_whitespace().map(String::from).collect(),
                pipe_to_next: None,
            }],
        })
    }

    type TestActorSystem = (
        ActorSystem,
        mpsc::Receiver<GatewayMsg>,
        mpsc::Receiver<SchedulerMsg>,
        mpsc::Receiver<ProcessMgrMsg>,
        mpsc::Receiver<ScopeStoreMsg>,
        mpsc::Receiver<super::super::EventBusMsg>,
    );

    /// Create an `ActorSystem` wired to test receivers.
    fn test_actor_system() -> TestActorSystem {
        let (gw_tx, gw_rx) = mpsc::channel(64);
        let (sched_tx, sched_rx) = mpsc::channel(64);
        let (pm_tx, pm_rx) = mpsc::channel(64);
        let (ss_tx, ss_rx) = mpsc::channel(64);
        let (eb_tx, eb_rx) = mpsc::channel(64);
        let sys = ActorSystem {
            gateway: gw_tx,
            scheduler: sched_tx,
            process_mgr: pm_tx,
            scope_store: ss_tx,
            event_bus: eb_tx,
        };
        (sys, gw_rx, sched_rx, pm_rx, ss_rx, eb_rx)
    }

    /// Spawn a fake scope_store that always replies with a zero hash.
    fn spawn_fake_scope_store(mut rx: mpsc::Receiver<ScopeStoreMsg>) {
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    ScopeStoreMsg::GetHead { reply } => {
                        let _ = reply.send(ScopeHash([0u8; 32]));
                    }
                    ScopeStoreMsg::Shutdown => break,
                    _ => {}
                }
            }
        });
    }

    /// Drain all `SpawnJob` messages from the ProcessMgr receiver.
    async fn drain_spawn_jobs(rx: &mut mpsc::Receiver<ProcessMgrMsg>) -> Vec<JobId> {
        let mut ids = Vec::new();
        // Yield to let messages propagate.
        tokio::task::yield_now().await;
        while let Ok(msg) = rx.try_recv() {
            if let ProcessMgrMsg::SpawnJob { job_id, .. } = msg {
                ids.push(job_id);
            }
        }
        ids
    }

    #[tokio::test]
    async fn serial_then_chain_spawns_left_first() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let mut state = SchedulerState::new();
        let chain = ChainNode::Serial {
            left: Box::new(leaf("echo a")),
            op: SerialOp::Then,
            right: Box::new(leaf("echo b")),
        };

        let resp = spawn_chain(chain, ScopeHash([0; 32]), 1, 1, &mut state, &sys).await;

        // Should create a chain, not a single job.
        assert!(matches!(
            resp,
            ResponsePayload::Ok(OkPayload::ChainCreated { .. })
        ));

        // Only one job should be spawned initially (the left leaf).
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 1);

        // Left leaf should be Running, right should be Pending.
        let chain_st = state.chains.values().next().unwrap();
        assert!(matches!(chain_st.leaf_status[&0], LeafStatus::Running));
        assert!(matches!(chain_st.leaf_status[&1], LeafStatus::Pending));
    }

    #[tokio::test]
    async fn serial_then_left_fail_cancels_right() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let mut state = SchedulerState::new();
        let chain = ChainNode::Serial {
            left: Box::new(leaf("false")),
            op: SerialOp::Then,
            right: Box::new(leaf("echo b")),
        };

        let _ = spawn_chain(chain, ScopeHash([0; 32]), 1, 1, &mut state, &sys).await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 1);
        let left_jid = spawned[0];

        // Simulate left failing.
        handle_job_finished(left_jid, 1, &mut state, &sys).await;

        // Right should NOT have been spawned.
        let after_finish = drain_spawn_jobs(&mut pm_rx).await;
        assert!(after_finish.is_empty());

        // Chain should be cleaned up (complete).
        assert!(state.chains.is_empty());
    }

    #[tokio::test]
    async fn serial_then_left_success_spawns_right() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let mut state = SchedulerState::new();
        let chain = ChainNode::Serial {
            left: Box::new(leaf("echo a")),
            op: SerialOp::Then,
            right: Box::new(leaf("echo b")),
        };

        let _ = spawn_chain(chain, ScopeHash([0; 32]), 1, 1, &mut state, &sys).await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        let left_jid = spawned[0];

        // Simulate left succeeding.
        handle_job_finished(left_jid, 0, &mut state, &sys).await;

        // Right should be spawned now.
        let after_finish = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(after_finish.len(), 1);
    }

    #[tokio::test]
    async fn serial_always_runs_right_after_left_fails() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let mut state = SchedulerState::new();
        let chain = ChainNode::Serial {
            left: Box::new(leaf("false")),
            op: SerialOp::Always,
            right: Box::new(leaf("cleanup")),
        };

        let _ = spawn_chain(chain, ScopeHash([0; 32]), 1, 1, &mut state, &sys).await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        let left_jid = spawned[0];

        // Left fails.
        handle_job_finished(left_jid, 1, &mut state, &sys).await;

        // Right should still spawn (Always semantics).
        let after = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(after.len(), 1);
    }

    #[tokio::test]
    async fn parallel_all_spawns_both_immediately() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let mut state = SchedulerState::new();
        let chain = ChainNode::Parallel {
            left: Box::new(leaf("cargo test")),
            op: ParallelOp::All,
            right: Box::new(leaf("cargo clippy")),
        };

        let _ = spawn_chain(chain, ScopeHash([0; 32]), 1, 1, &mut state, &sys).await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 2);
    }

    #[tokio::test]
    async fn cron_add_and_list() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let mut state = SchedulerState::new();
        let cmd = ResolvedCommand::Cron {
            schedule_text: "every 5m".into(),
            chain: leaf("backup.sh"),
            params: cue_core::command::ModeParams::new(),
        };
        let resp = handle_command(cmd, &mut state, &sys).await;
        assert!(matches!(
            resp,
            ResponsePayload::Ok(OkPayload::CronAdded { .. })
        ));
        assert_eq!(state.crons.len(), 1);

        // List crons.
        let list_resp = handle_command(ResolvedCommand::Crons, &mut state, &sys).await;
        if let ResponsePayload::Ok(OkPayload::CronList(list)) = list_resp {
            assert_eq!(list.len(), 1);
            assert_eq!(list[0].schedule, "every 5m");
            assert!(list[0].enabled);
        } else {
            panic!("expected CronList");
        }
    }

    #[tokio::test]
    async fn cron_pause_and_resume() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let mut state = SchedulerState::new();
        let cmd = ResolvedCommand::Cron {
            schedule_text: "every 1h".into(),
            chain: leaf("check.sh"),
            params: cue_core::command::ModeParams::new(),
        };
        let _ = handle_command(cmd, &mut state, &sys).await;

        // Pause.
        let pause =
            handle_command(ResolvedCommand::Pause { id: "C1".into() }, &mut state, &sys).await;
        assert!(matches!(pause, ResponsePayload::Ok(OkPayload::Ack {})));
        assert!(!state.crons[&CronId(1)].enabled);

        // Resume.
        let resume = handle_command(
            ResolvedCommand::Resume { id: "C1".into() },
            &mut state,
            &sys,
        )
        .await;
        assert!(matches!(resume, ResponsePayload::Ok(OkPayload::Ack {})));
        assert!(state.crons[&CronId(1)].enabled);
    }

    #[tokio::test]
    async fn job_tracking_after_spawn_and_finish() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let mut state = SchedulerState::new();
        let chain = leaf("ls -la");

        let resp = spawn_chain(chain, ScopeHash([0; 32]), 1, 1, &mut state, &sys).await;
        assert!(matches!(
            resp,
            ResponsePayload::Ok(OkPayload::JobCreated { .. })
        ));

        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 1);
        let jid = spawned[0];

        // Job should appear in :jobs listing as Running.
        let list_resp = handle_command(ResolvedCommand::Jobs, &mut state, &sys).await;
        if let ResponsePayload::Ok(OkPayload::JobList(list)) = &list_resp {
            assert_eq!(list.len(), 1);
            assert_eq!(list[0].status, JobStatus::Running);
        } else {
            panic!("expected JobList");
        }

        // Finish the job.
        handle_job_finished(jid, 0, &mut state, &sys).await;

        // Job should now be Done.
        let list_resp2 = handle_command(ResolvedCommand::Jobs, &mut state, &sys).await;
        if let ResponsePayload::Ok(OkPayload::JobList(list)) = &list_resp2 {
            assert_eq!(list[0].status, JobStatus::Done);
            assert_eq!(list[0].exit_code, Some(0));
        } else {
            panic!("expected JobList");
        }
    }

    #[tokio::test]
    async fn single_leaf_no_chain_tracking() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let mut state = SchedulerState::new();
        let chain = leaf("echo hello");

        let resp = spawn_chain(chain, ScopeHash([0; 32]), 1, 1, &mut state, &sys).await;
        // Single leaf → JobCreated, not ChainCreated.
        assert!(matches!(
            resp,
            ResponsePayload::Ok(OkPayload::JobCreated { .. })
        ));
        assert!(state.chains.is_empty());

        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 1);
    }

    #[test]
    fn parse_schedule_every() {
        let (dur, one_shot) = parse_schedule("every 5m").unwrap();
        assert_eq!(dur, std::time::Duration::from_secs(300));
        assert!(!one_shot);
    }

    #[test]
    fn parse_schedule_in() {
        let (dur, one_shot) = parse_schedule("in 30s").unwrap();
        assert_eq!(dur, std::time::Duration::from_secs(30));
        assert!(one_shot);
    }

    #[test]
    fn parse_schedule_hours() {
        let (dur, one_shot) = parse_schedule("every 2h").unwrap();
        assert_eq!(dur, std::time::Duration::from_secs(7200));
        assert!(!one_shot);
    }

    #[test]
    fn parse_schedule_invalid() {
        assert!(parse_schedule("at 09:00").is_none());
        assert!(parse_schedule("cron * * * * *").is_none());
        assert!(parse_schedule("every").is_none());
    }

    #[test]
    fn parse_job_id_valid() {
        assert_eq!(parse_job_id("J1"), Some(JobId(1)));
        assert_eq!(parse_job_id("J42"), Some(JobId(42)));
    }

    #[test]
    fn parse_job_id_invalid() {
        assert_eq!(parse_job_id("C1"), None);
        assert_eq!(parse_job_id("foo"), None);
    }

    #[test]
    fn parse_cron_id_valid() {
        assert_eq!(parse_cron_id("C1"), Some(CronId(1)));
        assert_eq!(parse_cron_id("C99"), Some(CronId(99)));
    }

    #[test]
    fn flatten_leaves_serial() {
        let chain = ChainNode::Serial {
            left: Box::new(leaf("a")),
            op: SerialOp::Then,
            right: Box::new(leaf("b")),
        };
        let leaves = flatten_leaves(&chain);
        assert_eq!(leaves.len(), 2);
        assert_eq!(leaves[0].index, 0);
        assert_eq!(leaves[1].index, 1);
    }

    #[test]
    fn initially_ready_serial() {
        let chain = ChainNode::Serial {
            left: Box::new(leaf("a")),
            op: SerialOp::Then,
            right: Box::new(leaf("b")),
        };
        let ready = initially_ready(&chain);
        assert_eq!(ready, vec![0]); // Only left is ready.
    }

    #[test]
    fn initially_ready_parallel() {
        let chain = ChainNode::Parallel {
            left: Box::new(leaf("a")),
            op: ParallelOp::All,
            right: Box::new(leaf("b")),
        };
        let ready = initially_ready(&chain);
        assert_eq!(ready, vec![0, 1]); // Both ready.
    }

    // ── FIX 1 test: Race + Serial — cancelled leaf must not be re-spawned ──

    /// `(a -> b) ||? c` — when `c` succeeds, Race should cancel both `a`/`b`.
    /// When `a` also succeeds, `b` should NOT be spawned because it was cancelled.
    #[tokio::test]
    async fn race_serial_cancelled_leaf_not_respawned() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let mut state = SchedulerState::new();
        // (a -> b) ||? c
        // Leaves: 0=a, 1=b, 2=c
        let chain = ChainNode::Parallel {
            left: Box::new(ChainNode::Serial {
                left: Box::new(leaf("a")),
                op: SerialOp::Then,
                right: Box::new(leaf("b")),
            }),
            op: ParallelOp::Race,
            right: Box::new(leaf("c")),
        };

        let _ = spawn_chain(chain, ScopeHash([0; 32]), 1, 1, &mut state, &sys).await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        // Initially ready: a (idx 0) and c (idx 2).
        assert_eq!(spawned.len(), 2);
        let a_jid = spawned[0]; // leaf 0 = a
        let c_jid = spawned[1]; // leaf 2 = c

        // c succeeds first → Race fires, cancels a (running) and b (pending).
        handle_job_finished(c_jid, 0, &mut state, &sys).await;

        // a was killed via cancel; drain the KillJob.
        let mut kill_ids = Vec::new();
        tokio::task::yield_now().await;
        while let Ok(msg) = pm_rx.try_recv() {
            if let ProcessMgrMsg::KillJob { job_id } = msg {
                kill_ids.push(job_id);
            }
        }
        assert!(kill_ids.contains(&a_jid), "a should have been killed");

        // Now a finishes (process exits after kill signal).
        handle_job_finished(a_jid, 0, &mut state, &sys).await;

        // b should NOT be spawned — it was already cancelled by Race.
        let after = drain_spawn_jobs(&mut pm_rx).await;
        assert!(after.is_empty(), "b must not be spawned after cancellation");

        // Chain should be complete.
        assert!(state.chains.is_empty(), "chain should be cleaned up");
    }

    // ── FIX 3 test: Race waits for entire branch, not single leaf ──

    /// `(compile -> test) ||? lint`
    /// When `compile` succeeds but `test` hasn't run yet, Race should NOT fire.
    #[tokio::test]
    async fn race_does_not_fire_on_partial_branch_success() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let mut state = SchedulerState::new();
        // (compile -> test) ||? lint
        // Leaves: 0=compile, 1=test, 2=lint
        let chain = ChainNode::Parallel {
            left: Box::new(ChainNode::Serial {
                left: Box::new(leaf("compile")),
                op: SerialOp::Then,
                right: Box::new(leaf("test")),
            }),
            op: ParallelOp::Race,
            right: Box::new(leaf("lint")),
        };

        let _ = spawn_chain(chain, ScopeHash([0; 32]), 1, 1, &mut state, &sys).await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        // Initially ready: compile (idx 0) and lint (idx 2).
        assert_eq!(spawned.len(), 2);
        let compile_jid = spawned[0]; // leaf 0 = compile

        // compile succeeds → test should become ready, Race must NOT fire yet.
        handle_job_finished(compile_jid, 0, &mut state, &sys).await;

        // test should have been spawned.
        let after_compile = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(after_compile.len(), 1, "test should be spawned");

        // lint should still be running (not cancelled by Race).
        let chain_st = state.chains.values().next().unwrap();
        assert!(
            matches!(chain_st.leaf_status.get(&2), Some(LeafStatus::Running)),
            "lint should still be running — Race should not have fired yet"
        );
    }

    // ── FIX 2 test: :cancel updates chain leaf_status and advances chain ──

    #[tokio::test]
    async fn cancel_chain_leaf_updates_leaf_status() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let mut state = SchedulerState::new();
        // a -> b
        let chain = ChainNode::Serial {
            left: Box::new(leaf("a")),
            op: SerialOp::Always,
            right: Box::new(leaf("b")),
        };

        let _ = spawn_chain(chain, ScopeHash([0; 32]), 1, 1, &mut state, &sys).await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 1);
        let a_jid = spawned[0];

        // Cancel a via :cancel.
        let resp = handle_command(
            ResolvedCommand::Cancel {
                id: format!("J{}", a_jid.0),
            },
            &mut state,
            &sys,
        )
        .await;
        assert!(matches!(resp, ResponsePayload::Ok(OkPayload::Ack {})));

        // Since the op is Always, b should become ready after a is cancelled.
        // The process_chain_advance sends both KillJob and SpawnJob to pm_rx.
        // Drain all messages and check.
        let after = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(
            after.len(),
            1,
            "b should be spawned via Always after cancel"
        );
    }

    // ── FIX 2 test: :kill does not get overwritten by later JobFinished ──

    #[tokio::test]
    async fn kill_status_not_overwritten_by_job_finished() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let mut state = SchedulerState::new();
        let chain = leaf("long-running");

        let _ = spawn_chain(chain, ScopeHash([0; 32]), 1, 1, &mut state, &sys).await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        let jid = spawned[0];

        // Kill the job.
        let resp = handle_command(
            ResolvedCommand::Kill {
                id: format!("J{}", jid.0),
            },
            &mut state,
            &sys,
        )
        .await;
        assert!(matches!(resp, ResponsePayload::Ok(OkPayload::Ack {})));
        assert_eq!(state.jobs[&jid].status, JobStatus::Killed);

        // Now the process exits (JobFinished arrives).
        handle_job_finished(jid, -9, &mut state, &sys).await;

        // Status should still be Killed, not overwritten to Failed.
        assert_eq!(
            state.jobs[&jid].status,
            JobStatus::Killed,
            "Killed status must not be overwritten by JobFinished"
        );
    }
}
