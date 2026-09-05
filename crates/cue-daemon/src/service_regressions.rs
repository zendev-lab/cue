use super::tests::{committed_running_task, hello, scope, spec};
use super::*;
use cue_core::{
    BuiltinCommand, BuiltinSuccess, CancelMode, ExecutionPlan, ExecutionSpec, FileModeMask, IoMode,
};
use std::time::Duration;
use tokio::io::AsyncWriteExt as _;

fn query(request: u64, query: Query) -> Message {
    Message::Query {
        request_id: RequestId::new(request).unwrap(),
        query,
    }
}

fn command(request: u64, command: Command) -> Message {
    Message::Command {
        request_id: RequestId::new(request).unwrap(),
        operation_id: OperationId::new(format!("regression:{request}")).unwrap(),
        command,
    }
}

async fn receive(client: &mut tokio::io::DuplexStream) -> Message {
    tokio::time::timeout(Duration::from_secs(3), read_wire_message(client))
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

async fn exchange(client: &mut tokio::io::DuplexStream, message: Message) -> ResponsePayload {
    client
        .write_all(&encode_message(&message).unwrap())
        .await
        .unwrap();
    let Message::Response {
        request_id,
        payload,
    } = receive(client).await
    else {
        panic!("expected response")
    };
    let expected = match message {
        Message::Query { request_id, .. } | Message::Command { request_id, .. } => request_id,
        _ => unreachable!(),
    };
    assert_eq!(request_id, expected);
    payload
}

async fn socket(
    service: &Arc<DaemonService>,
) -> (
    tokio::io::DuplexStream,
    tokio::task::JoinHandle<Result<(), RuntimeError>>,
) {
    let (mut client, server) = tokio::io::duplex(2 * 1024 * 1024);
    let serving = tokio::spawn(serve_stream(service.clone(), server));
    assert!(matches!(
        exchange(
            &mut client,
            query(
                1,
                Query::Hello(Hello {
                    protocol_version: PROTOCOL_VERSION,
                    client_id: ClientId::new("regression-client").unwrap(),
                })
            )
        )
        .await,
        ResponsePayload::Ok(_)
    ));
    (client, serving)
}

#[tokio::test]
async fn fragmented_frames_survive_unrelated_fact_and_output_events() {
    let service = DaemonService::in_memory().unwrap();
    let (mut client, serving) = socket(&service).await;
    let ping = encode_message(&query(2, Query::Ping)).unwrap();
    let mut start = 0;
    for end in [2, 7, ping.len()] {
        client.write_all(&ping[start..end]).await.unwrap();
        start = end;
        if end == ping.len() {
            break;
        }
        // Only the reader is ready initially, so it consumes this fragment.
        tokio::time::sleep(Duration::from_millis(20)).await;
        service
            .store
            .output_events
            .send(LiveOutput {
                step: StepId {
                    execution: ExecutionId(99),
                    index: 1,
                },
                stream: OutputStream::Terminal,
                offset: 0,
                data: b"unrelated".to_vec(),
            })
            .unwrap_or_else(|_| panic!("output receiver closed"));
        service
            .store
            .events
            .send(FactEvent {
                id: EventId::new(1).unwrap(),
                occurred_at_ms: 1,
                fact: Fact::ExecutionCreated {
                    id: ExecutionId(99),
                    scope: scope(false).compute_hash(),
                },
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !service.store.output_events.is_empty() || !service.store.events.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    assert!(
        matches!(receive(&mut client).await, Message::Response { request_id, payload: ResponsePayload::Ok(_) } if request_id == RequestId::new(2).unwrap())
    );
    assert!(matches!(
        exchange(&mut client, query(3, Query::Ping)).await,
        ResponsePayload::Ok(_)
    ));
    drop(client);
    serving.await.unwrap().unwrap();
}

#[tokio::test]
async fn wait_does_not_block_ping_or_cancel_on_the_same_stream() {
    let service = DaemonService::in_memory().unwrap();
    let task = committed_running_task(
        &service,
        spec(scope(false).compute_hash(), "/bin/sleep", &["30"])
            .plan()
            .clone(),
    )
    .await;
    service.schedule_runtime(task.clone()).unwrap();
    let (mut client, serving) = socket(&service).await;
    client
        .write_all(&encode_message(&query(2, Query::WaitExecution { id: task.id })).unwrap())
        .await
        .unwrap();
    assert!(matches!(
        exchange(&mut client, query(3, Query::Ping)).await,
        ResponsePayload::Ok(_)
    ));
    client
        .write_all(
            &encode_message(&command(
                4,
                Command::CancelExecution {
                    id: task.id,
                    mode: CancelMode::Force,
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    let mut ids = Vec::new();
    for _ in 0..2 {
        let Message::Response {
            request_id,
            payload: ResponsePayload::Ok(ResultPayload::Execution { execution }),
        } = receive(&mut client).await
        else {
            panic!("expected execution response")
        };
        if request_id == RequestId::new(2).unwrap() {
            assert_eq!(execution.state, ExecutionState::Cancelled);
        }
        ids.push(request_id);
    }
    ids.sort();
    assert_eq!(
        ids,
        vec![RequestId::new(2).unwrap(), RequestId::new(4).unwrap()]
    );
    drop(client);
    serving.await.unwrap().unwrap();
}

#[tokio::test]
async fn worker_retries_builtin_run_completion_and_ack_without_respawning() {
    for phase in ["builtin", "run-completion", "run-ack"] {
        let uri = format!(
            "file:retry-{}?mode=memory&cache=shared",
            uuid::Uuid::new_v4()
        );
        let service = DaemonService::from_store(
            Store::from_connection(rusqlite::Connection::open(&uri).unwrap()).unwrap(),
        )
        .unwrap();
        let injection = rusqlite::Connection::open(&uri).unwrap();
        let plan = if phase == "builtin" {
            ExecutionPlan::builtin(BuiltinCommand::Umask(FileModeMask::new(0o077).unwrap()))
        } else {
            spec(scope(false).compute_hash(), "/usr/bin/printf", &["once"])
                .plan()
                .clone()
        };
        let task = committed_running_task(&service, plan).await;
        let step = StepId {
            execution: task.id,
            index: 1,
        };
        let trigger = if phase == "run-ack" {
            "CREATE TRIGGER reject_delivery BEFORE UPDATE ON runtime_work WHEN OLD.claimed_generation IS NOT NULL AND NEW.claimed_generation IS NULL BEGIN SELECT RAISE(ABORT, 'injected ack failure'); END;"
        } else {
            "CREATE TRIGGER reject_delivery BEFORE INSERT ON facts WHEN json_extract(NEW.fact_json, '$.kind') = 'execution_finished' BEGIN SELECT RAISE(ABORT, 'injected completion failure'); END;"
        };
        injection.execute_batch(trigger).unwrap();
        let worker = tokio::spawn(service.clone().realize(task.clone(), step));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!worker.is_finished(), "{phase}");
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(100), task.state.lock())
                .await
                .unwrap()
                .execution
                .state(),
            ExecutionState::Running
        );
        // A duplicate delivery cannot take the existing owner or rerun the process.
        service.clone().realize(task.clone(), step).await.unwrap();
        injection
            .execute_batch("DROP TRIGGER reject_delivery")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            task.state.lock().await.execution.state(),
            ExecutionState::Succeeded
        );
        assert!(
            service
                .store
                .lock_store()
                .unwrap()
                .pending_runtime_steps()
                .unwrap()
                .is_empty()
        );
        if phase != "builtin" {
            assert_eq!(
                service
                    .output
                    .read(step, OutputStream::Stdout, 0, 1024)
                    .unwrap()
                    .data,
                b"once"
            );
        }
        let facts = service.store.facts_after(task.id, None, u16::MAX).unwrap();
        assert_eq!(
            facts
                .iter()
                .filter(|event| matches!(event.fact, Fact::ExecutionFinished { .. }))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn cancel_replay_survives_terminal_recovery_with_conflict_and_tombstone() {
    let uri = format!(
        "file:replay-{}?mode=memory&cache=shared",
        uuid::Uuid::new_v4()
    );
    let service = DaemonService::from_store(
        Store::from_connection(rusqlite::Connection::open(&uri).unwrap()).unwrap(),
    )
    .unwrap();
    let task = committed_running_task(
        &service,
        ExecutionPlan::builtin(BuiltinCommand::Umask(FileModeMask::new(0o077).unwrap())),
    )
    .await;
    let mut connection = service.connection();
    hello(&mut connection).await;
    let cancel = command(
        10,
        Command::CancelExecution {
            id: task.id,
            mode: CancelMode::Force,
        },
    );
    let original = connection.handle(cancel.clone()).await;
    assert!(matches!(
        original,
        Message::Response {
            payload: ResponsePayload::Ok(_),
            ..
        }
    ));
    service.wait_execution(task.id).await.unwrap();
    let restarted = DaemonService::from_store(
        Store::from_connection(rusqlite::Connection::open(&uri).unwrap()).unwrap(),
    )
    .unwrap();
    restarted.recover().await.unwrap();
    assert!(restarted.tasks.lock().await.is_empty());
    let mut resumed = restarted.connection();
    hello(&mut resumed).await;
    assert_eq!(resumed.handle(cancel.clone()).await, original);
    let conflict = resumed
        .handle(command(
            10,
            Command::CancelExecution {
                id: ExecutionId(999),
                mode: CancelMode::Graceful,
            },
        ))
        .await;
    assert!(
        matches!(conflict, Message::Response { payload: ResponsePayload::Error(error), .. } if error.code == ProtocolErrorCode::Conflict)
    );
    restarted
        .store
        .lock_store()
        .unwrap()
        .tombstone_operation_responses_before(i64::MAX)
        .unwrap();
    assert!(
        matches!(resumed.handle(cancel).await, Message::Response { payload: ResponsePayload::Error(error), .. } if error.code == ProtocolErrorCode::Conflict)
    );
}

#[tokio::test]
async fn recovery_walks_past_a_full_page_of_newer_terminal_executions() {
    let service = DaemonService::in_memory().unwrap();
    let input = scope(false);
    {
        let store = service.store.lock_store().unwrap();
        store.put_scope(&input, 1).unwrap();
        for id in 1..=u64::from(STORE_PAGE_SIZE) + 1 {
            let mut execution = Execution::new(
                ExecutionId(id),
                ExecutionSpec::new(
                    input.compute_hash(),
                    ExecutionPlan::builtin(BuiltinCommand::Umask(
                        FileModeMask::new(0o077).unwrap(),
                    )),
                )
                .unwrap(),
            );
            store
                .commit_execution(
                    &projection(&execution, 1, 1),
                    &[FactDraft {
                        occurred_at_ms: 1,
                        fact: Fact::ExecutionCreated {
                            id: execution.id(),
                            scope: input.compute_hash(),
                        },
                    }],
                )
                .unwrap();
            if id == 1 {
                continue;
            }
            let before = execution.snapshot();
            execution.advance().unwrap();
            store
                .commit_execution(
                    &projection(&execution, 1, 1),
                    &transition_facts(&before, &ExecutionState::Pending, &execution, 1),
                )
                .unwrap();
            let before = execution.snapshot();
            let transition = execution
                .complete_builtin(
                    StepId {
                        execution: execution.id(),
                        index: 1,
                    },
                    &input,
                    Ok(BuiltinSuccess::Umask),
                )
                .unwrap();
            for scope in transition.new_scopes {
                store.put_scope(&scope, 1).unwrap();
            }
            store
                .commit_execution(
                    &projection(&execution, 1, 1),
                    &transition_facts(&before, &ExecutionState::Running, &execution, 1),
                )
                .unwrap();
        }
    }
    service.recover().await.unwrap();
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(3),
            service.wait_execution(ExecutionId(1))
        )
        .await
        .unwrap()
        .unwrap()
        .state,
        ExecutionState::Succeeded
    );
    assert_eq!(service.tasks.lock().await.len(), 1);
}

#[tokio::test]
async fn watch_replays_all_pages_and_deduplicates_the_live_boundary() {
    let service = DaemonService::in_memory().unwrap();
    let task = committed_running_task(
        &service,
        spec(scope(false).compute_hash(), "/bin/true", &[])
            .plan()
            .clone(),
    )
    .await;
    let step = StepId {
        execution: task.id,
        index: 1,
    };
    let facts = (0..u64::from(STORE_PAGE_SIZE) * 2 + 1)
        .map(|offset| FactDraft {
            occurred_at_ms: 1,
            fact: Fact::OutputAppended {
                step,
                stream: OutputStream::Stdout,
                start_offset: offset,
                end_offset: offset + 1,
            },
        })
        .collect::<Vec<_>>();
    let projection = service.store.load_execution(task.id).unwrap().unwrap();
    service.store.commit(&projection, &facts).unwrap();
    let expected = service.store.facts_after(task.id, None, u16::MAX).unwrap();
    let mut connection = service.connection();
    hello(&mut connection).await;
    let response = connection
        .handle(command(
            10,
            Command::WatchExecution {
                id: task.id,
                after_event: None,
            },
        ))
        .await;
    assert!(
        matches!(response, Message::Response { payload: ResponsePayload::Ok(ResultPayload::Watching { latest_event, .. }), .. } if latest_event == expected.last().map(|event| event.id))
    );
    assert_eq!(
        connection.drain_replayed_facts().collect::<Vec<_>>(),
        expected
    );
    for event in &expected {
        assert!(!connection.accepts_fact(event));
    }
    let mut next = facts.last().unwrap().clone();
    next.fact = Fact::OutputAppended {
        step,
        stream: OutputStream::Stdout,
        start_offset: 3000,
        end_offset: 3001,
    };
    let live = service
        .store
        .commit(&projection, &[next])
        .unwrap()
        .pop()
        .unwrap();
    assert!(connection.accepts_fact(&live));
    assert!(!connection.accepts_fact(&live));
}

#[tokio::test]
async fn force_from_another_connection_interrupts_backpressured_pty_input() {
    backpressured_pty_input(false).await;
}

#[tokio::test]
async fn drain_interrupts_backpressured_pty_input() {
    backpressured_pty_input(true).await;
}

async fn backpressured_pty_input(draining: bool) {
    let service = DaemonService::in_memory().unwrap();
    let captured = spec(
        scope(false).compute_hash(),
        "/bin/sh",
        &[
            "-c",
            "/bin/stty raw -echo; printf ready; exec /bin/sleep 30",
        ],
    );
    let step = StepId {
        execution: ExecutionId(1),
        index: 1,
    };
    let StepAction::Run { pipeline, .. } = Execution::new(step.execution, captured)
        .action(step)
        .unwrap()
    else {
        unreachable!()
    };
    let task = committed_running_task(&service, ExecutionPlan::run(pipeline, IoMode::Pty)).await;
    service.schedule_runtime(task.clone()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while service
            .output
            .tail(step, OutputStream::Terminal, 1024)
            .unwrap()
            .data
            != b"ready"
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let (mut a, serving_a) = socket(&service).await;
    let (mut b, serving_b) = socket(&service).await;
    let ResponsePayload::Ok(ResultPayload::PtyAttached { attachment, .. }) = exchange(
        &mut a,
        command(
            300,
            Command::AttachPty {
                step,
                replay_bytes: 0,
            },
        ),
    )
    .await
    else {
        panic!("attach")
    };
    assert!(matches!(
        exchange(
            &mut a,
            command(301, Command::ClaimPtyControl { attachment })
        )
        .await,
        ResponsePayload::Ok(_)
    ));
    a.write_all(
        &encode_message(&command(
            302,
            Command::PtyInput {
                attachment,
                data: vec![b'x'; 128 * 1024],
            },
        ))
        .unwrap(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    if draining {
        tokio::time::timeout(Duration::from_secs(3), service.drain())
            .await
            .unwrap()
            .unwrap();
    } else {
        assert!(matches!(
            exchange(
                &mut b,
                command(
                    303,
                    Command::CancelExecution {
                        id: task.id,
                        mode: CancelMode::Force
                    }
                )
            )
            .await,
            ResponsePayload::Ok(_)
        ));
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), service.wait_execution(task.id))
            .await
            .unwrap()
            .unwrap()
            .state,
        ExecutionState::Cancelled
    );
    assert!(
        matches!(receive(&mut a).await, Message::Response { request_id, payload: ResponsePayload::Error(_) } if request_id == RequestId::new(302).unwrap())
    );
    drop(a);
    drop(b);
    serving_a.await.unwrap().unwrap();
    serving_b.await.unwrap().unwrap();
}

#[tokio::test]
async fn pty_controller_lease_ends_on_eof_error_and_task_abort() {
    let service = DaemonService::in_memory().unwrap();
    let captured = spec(scope(false).compute_hash(), "/bin/sleep", &["30"]);
    let StepAction::Run { pipeline, .. } = Execution::new(ExecutionId(1), captured)
        .action(StepId {
            execution: ExecutionId(1),
            index: 1,
        })
        .unwrap()
    else {
        unreachable!()
    };
    let task = committed_running_task(&service, ExecutionPlan::run(pipeline, IoMode::Pty)).await;
    service.schedule_runtime(task.clone()).unwrap();
    let step = StepId {
        execution: task.id,
        index: 1,
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        while !task.controls.lock().await.contains_key(&step) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for (index, exit) in ["eof", "error", "abort"].into_iter().enumerate() {
        let (mut a, serving_a) = socket(&service).await;
        let (mut b, serving_b) = socket(&service).await;
        let base = 100 + index as u64 * 10;
        let ResponsePayload::Ok(ResultPayload::PtyAttached { attachment, .. }) = exchange(
            &mut a,
            command(
                base,
                Command::AttachPty {
                    step,
                    replay_bytes: 0,
                },
            ),
        )
        .await
        else {
            panic!("attach")
        };
        assert!(matches!(
            exchange(
                &mut a,
                command(base + 1, Command::ClaimPtyControl { attachment })
            )
            .await,
            ResponsePayload::Ok(_)
        ));
        assert!(matches!(
            exchange(&mut b, command(base + 2, Command::DetachPty { attachment })).await,
            ResponsePayload::Error(_)
        ));
        match exit {
            "eof" => {
                drop(a);
                serving_a.await.unwrap().unwrap();
            }
            "error" => {
                a.write_all(&u32::MAX.to_be_bytes()).await.unwrap();
                assert!(serving_a.await.unwrap().is_err());
            }
            _ => {
                serving_a.abort();
                assert!(serving_a.await.unwrap_err().is_cancelled());
            }
        }
        assert!(service.lock_attachments().unwrap().is_empty());
        let ResponsePayload::Ok(ResultPayload::PtyAttached {
            attachment,
            control_available,
            ..
        }) = exchange(
            &mut b,
            command(
                base + 3,
                Command::AttachPty {
                    step,
                    replay_bytes: 0,
                },
            ),
        )
        .await
        else {
            panic!("reattach")
        };
        assert!(control_available);
        assert!(matches!(
            exchange(
                &mut b,
                command(base + 4, Command::ClaimPtyControl { attachment })
            )
            .await,
            ResponsePayload::Ok(_)
        ));
        drop(b);
        serving_b.await.unwrap().unwrap();
    }
    let mut connection = service.connection();
    hello(&mut connection).await;
    connection
        .handle(command(
            200,
            Command::CancelExecution {
                id: task.id,
                mode: CancelMode::Force,
            },
        ))
        .await;
    service.wait_execution(task.id).await.unwrap();
}
