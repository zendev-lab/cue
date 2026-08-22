# Conceptual model

Cue has four durable concepts: execution, session, scope, and schedule.

An **execution** is one submitted typed plan and the only aggregate lifecycle
unit. A **step** identifies a process-bearing pipeline leaf inside it. A
**session** is a reconnectable human/agent attachment context whose cursor
points to a **scope**. A **schedule** is a trigger template that submits a fresh
execution; it does not own running state.

The important ownership relation is:

```text
session -> scope cursor
schedule --trigger--> new execution -> stable process steps
execution reducer --ready node--> process manager
process manager --outcome--> execution reducer
```

Scopes are immutable, content-addressed snapshots. An explicit `ContextDelta`
creates a successor scope in reducer order. Process-local `NAME=value`
overrides are attached to a pipeline segment and do not mutate the cursor.

Cue deliberately stops here. Workflow retry policy, agent intent, approvals,
secret management, remote fleet orchestration, and general DAG semantics belong
to clients or higher layers.
