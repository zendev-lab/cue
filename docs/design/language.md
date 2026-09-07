# Cue language boundary

`cue-language` lowers surface syntax into the closed `ExecutionPlan`.
The daemon never parses source text and the compiler never reads ambient cwd or
environment. A caller first submits an explicit `Scope` with `PutScope`, then
passes the returned `ScopeHash` to the compiler and submits the resulting
`ExecutionSpec`.

This is a value flow, not a session handshake or cursor:

```text
client Scope --PutScope--> ScopeHash
                              |
surface source --compile------+--> ExecutionSpec --SubmitExecution--> daemon
```

## Builtins

Core has exactly three builtin families:

- `cd PATH` produces `BuiltinCommand::Cd`;
- `env set KEY=VALUE ...` and `env unset KEY ...` produce one
  `BuiltinCommand::Env` mutation;
- `umask OCTAL` produces `BuiltinCommand::Umask`.

They are ordinary observable leaves with `StepId`, result, duration, and Scope
transition. They are composed with `->`; they cannot be pipeline processes and
cannot carry process-local assignment prefixes. There is no hidden
`ContextDelta`, `ApplyScopeDelta`, or session update path.

## Assignment and pipeline rules

An assignment is recognized only before the first command word of one pipeline
segment:

```text
A=B command arg         -> command Process.env[A] = Set("B")
A=B left |> right       -> only left receives A=B
command A=B             -> "A=B" remains literal argv
A=B                     -> rejected because no executable follows
```

Process patches are independently applied to the input Scope environment.
There is no left-to-right environment inheritance inside a Pipeline. Persistent
changes use an Env builtin in a Sequence:

```text
env set A=B -> left |> right
```

Both processes see the Scope produced by the builtin, while their own local
patches remain independent.

## PTY resolution

`IoMode` belongs to each `Run`, and one mode applies to that Run's complete
Pipeline. Builtins have no I/O mode. Core accepts only `Captured` or `Pty`;
frontend heuristics resolve common interactive commands when no override is
given, and `:run(pty=true|false)` is an explicit surface override.

Different Run leaves in one `.cue` file may therefore use different modes.
PTY attachment size, controller, observers, and input ownership are runtime
state and never enter `ExecutionSpec`.

## External owners

Schedule, retry, resource/workspace policy, scope history, session state, and
configuration do not lower into kernel variants. Their surface commands fail
with an external-owner diagnostic. A producer or host may resolve those
policies and submit an ordinary `ExecutionSpec`; it cannot extend the execution
algebra.

Legacy launch parameters follow the same rule. `cwd` is written explicitly as
`cd PATH -> ...`; workspace, wrapper, and resource parameters are resolved by
Composition before compilation. `ExecutionSpec` has only `scope` and `plan`—it
contains no retry, launch context, source metadata, or ambient session field.
