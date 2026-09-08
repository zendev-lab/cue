use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use cue_client::MultiplexedClient;
use cue_core::OutputStream;
use cue_protocol::{Query, ResultPayload};

use crate::workbench::State;

pub(crate) type CompletedRequest = Result<Result<ResultPayload>, tokio::task::JoinError>;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Command,
    Wait,
    Refresh,
    Output(u64),
    Page,
    Inspect,
}

#[derive(Default)]
pub(crate) struct PendingRequests {
    pub commands: tokio::task::JoinSet<Result<ResultPayload>>,
    pub waits: tokio::task::JoinSet<Result<ResultPayload>>,
    refreshes: tokio::task::JoinSet<Result<ResultPayload>>,
    output_generation: u64,
    outputs: tokio::task::JoinSet<Result<ResultPayload>>,
    pages: tokio::task::JoinSet<Result<ResultPayload>>,
    inspections: tokio::task::JoinSet<Result<ResultPayload>>,
    pub refresh_again: bool,
}

pub(crate) async fn query(client: &MultiplexedClient, query: Query) -> Result<ResultPayload> {
    tokio::time::timeout(Duration::from_secs(5), client.query(query))
        .await
        .context("daemon query timed out; refresh to retry")?
}

pub(crate) async fn list_executions(client: &MultiplexedClient) -> Result<ResultPayload> {
    query(
        client,
        Query::ListExecutions {
            before: None,
            limit: 100,
        },
    )
    .await
}

impl PendingRequests {
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
            && self.waits.is_empty()
            && self.refreshes.is_empty()
            && self.outputs.is_empty()
            && self.pages.is_empty()
            && self.inspections.is_empty()
    }

    pub async fn join_next(&mut self) -> Option<(Kind, CompletedRequest)> {
        if self.is_empty() {
            return None;
        }
        tokio::select! {
            result = self.commands.join_next(), if !self.commands.is_empty() => result.map(|result| (Kind::Command, result)),
            result = self.waits.join_next(), if !self.waits.is_empty() => result.map(|result| (Kind::Wait, result)),
            result = self.refreshes.join_next(), if !self.refreshes.is_empty() => result.map(|result| (Kind::Refresh, result)),
            result = self.outputs.join_next(), if !self.outputs.is_empty() => result.map(|result| (Kind::Output(self.output_generation), result)),
            result = self.pages.join_next(), if !self.pages.is_empty() => result.map(|result| (Kind::Page, result)),
            result = self.inspections.join_next(), if !self.inspections.is_empty() => result.map(|result| (Kind::Inspect, result)),
        }
    }

    pub fn request_refresh(&mut self, client: &Arc<MultiplexedClient>) {
        if !self.refreshes.is_empty() {
            self.refresh_again = true;
            return;
        }
        self.refresh_again = false;
        let client = client.clone();
        self.refreshes
            .spawn(async move { list_executions(&client).await });
    }

    pub fn request_output(&mut self, client: &Arc<MultiplexedClient>, state: &State) {
        let Some(step) = state.step() else {
            return;
        };
        if !self.outputs.is_empty() {
            return;
        }
        self.output_generation = state.output_generation;
        let request = Query::ReadOutput {
            step,
            stdout: state.range(OutputStream::Stdout),
            stderr: state.range(OutputStream::Stderr),
            terminal: state.range(OutputStream::Terminal),
        };
        let client = client.clone();
        self.outputs
            .spawn(async move { query(&client, request).await });
    }

    pub fn inspect_selected(&mut self, client: &Arc<MultiplexedClient>, state: &State) {
        let Some(id) = state.selection else {
            return;
        };
        if !self.inspections.is_empty() {
            return;
        }
        let client = client.clone();
        self.inspections
            .spawn(async move { query(&client, Query::GetExecution { id }).await });
    }

    pub fn older(&mut self, client: &Arc<MultiplexedClient>, state: &State) {
        let Some(before) = state.next_page else {
            return;
        };
        if !self.pages.is_empty() {
            return;
        }
        let client = client.clone();
        self.pages.spawn(async move {
            query(
                &client,
                Query::ListExecutions {
                    before: Some(before),
                    limit: 100,
                },
            )
            .await
        });
    }
}
