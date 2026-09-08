use std::collections::BTreeMap;

use cue_core::{BuiltinCommand, ExecutionId, ExecutionPlan, IoMode, OutputStream, StepId};
use cue_protocol::{ExecutionView, OutputChunk, OutputRange};
use ratatui::layout::Rect;
use ratatui::widgets::ListState;

use crate::editor::Editor;

pub(crate) const STREAM_LIMIT: usize = 1024 * 1024;

#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Focus {
    #[default]
    Input,
    Sidebar,
    Output,
}

#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tab {
    #[default]
    Output,
    Stdout,
    Stderr,
    Terminal,
    Details,
    Activity,
}

impl Tab {
    pub const ALL: [Self; 6] = [
        Self::Output,
        Self::Stdout,
        Self::Stderr,
        Self::Terminal,
        Self::Details,
        Self::Activity,
    ];
    pub fn label(self) -> &'static str {
        match self {
            Self::Output => "Output",
            Self::Stdout => "Stdout",
            Self::Stderr => "Stderr",
            Self::Terminal => "Terminal",
            Self::Details => "Details",
            Self::Activity => "Activity",
        }
    }
    pub fn stream(self) -> Option<OutputStream> {
        match self {
            Self::Stdout => Some(OutputStream::Stdout),
            Self::Stderr => Some(OutputStream::Stderr),
            Self::Terminal => Some(OutputStream::Terminal),
            _ => None,
        }
    }
}

#[derive(Default)]
pub(crate) struct OutputBuffer {
    pub offset: u64,
    pub bytes: Vec<u8>,
}

impl OutputBuffer {
    pub fn range(&self) -> OutputRange {
        OutputRange {
            offset: self.offset + self.bytes.len() as u64,
            max_bytes: 128 * 1024,
        }
    }

    pub fn append(&mut self, chunk: OutputChunk) {
        let end = self.offset + self.bytes.len() as u64;
        if chunk.offset > end || chunk.offset < self.offset {
            self.bytes.clear();
            self.offset = chunk.offset;
        }
        let overlap = (self.offset + self.bytes.len() as u64).saturating_sub(chunk.offset) as usize;
        if overlap < chunk.data.len() {
            self.bytes.extend_from_slice(&chunk.data[overlap..]);
        }
        if self.bytes.len() > STREAM_LIMIT {
            let discarded = self.bytes.len() - STREAM_LIMIT;
            self.bytes.drain(..discarded);
            self.offset += discarded as u64;
        }
    }
}

pub(crate) struct State {
    pub editor: Editor,
    pub executions: Vec<ExecutionView>,
    pub selection: Option<ExecutionId>,
    pub step_index: usize,
    pub sidebar: ListState,
    pub sidebar_visible: bool,
    pub focus: Focus,
    pub tab: Tab,
    pub output: BTreeMap<OutputStream, OutputBuffer>,
    pub output_loaded: bool,
    pub output_generation: u64,
    pub history_pending: Vec<String>,
    pub scroll: usize,
    pub horizontal: u16,
    pub follow: bool,
    pub log: Vec<String>,
    pub notice: String,
    pub connected: bool,
    pub help: bool,
    pub next_page: Option<ExecutionId>,
    pub foreground: Option<(StepId, bool)>,
    pub sidebar_area: Rect,
    pub output_area: Rect,
    pub tabs_area: Rect,
    pub tab_hits: Vec<(Tab, Rect)>,
    pub input_area: Rect,
}

impl Default for State {
    fn default() -> Self {
        Self {
            editor: Editor::default(),
            executions: Vec::new(),
            selection: None,
            step_index: 0,
            sidebar: ListState::default(),
            sidebar_visible: true,
            focus: Focus::Input,
            tab: Tab::Output,
            output: BTreeMap::new(),
            output_loaded: false,
            output_generation: 0,
            history_pending: Vec::new(),
            scroll: 0,
            horizontal: 0,
            follow: true,
            log: Vec::new(),
            notice: "Ready".into(),
            connected: true,
            help: false,
            next_page: None,
            foreground: None,
            sidebar_area: Rect::default(),
            output_area: Rect::default(),
            tabs_area: Rect::default(),
            tab_hits: Vec::new(),
            input_area: Rect::default(),
        }
    }
}

impl State {
    pub fn log(&mut self, text: impl Into<String>) {
        self.notice = text.into();
        self.log.push(self.notice.clone());
        if self.log.len() > 500 {
            self.log.drain(..self.log.len() - 500);
        }
    }

    pub fn selected(&self) -> Option<&ExecutionView> {
        self.executions
            .iter()
            .find(|execution| Some(execution.snapshot.id) == self.selection)
    }

    pub fn step(&self) -> Option<StepId> {
        self.selected()?
            .snapshot
            .steps
            .get(self.step_index)
            .map(|step| step.id())
    }

    pub fn reset_output(&mut self) {
        self.output_generation = self.output_generation.wrapping_add(1);
        self.output.clear();
        self.output_loaded = false;
        self.scroll = 0;
        self.horizontal = 0;
        self.follow = true;
    }

    pub fn select(&mut self, id: ExecutionId) {
        if self.selection != Some(id) {
            self.selection = Some(id);
            self.step_index = self
                .selected()
                .map(|execution| {
                    leaves(execution)
                        .iter()
                        .position(|(_, io)| io.is_some())
                        .unwrap_or(0)
                })
                .unwrap_or(0);
            self.reset_output();
        }
        self.sidebar.select(
            self.executions
                .iter()
                .position(|execution| execution.snapshot.id == id),
        );
    }

    pub fn move_selection(&mut self, delta: isize) {
        let index = self
            .sidebar
            .selected()
            .unwrap_or(0)
            .saturating_add_signed(delta)
            .min(self.executions.len().saturating_sub(1));
        if let Some(id) = self
            .executions
            .get(index)
            .map(|execution| execution.snapshot.id)
        {
            self.select(id);
        }
    }

    pub fn move_step(&mut self, delta: isize) {
        let count = self
            .selected()
            .map_or(0, |execution| execution.snapshot.steps.len());
        let next = self
            .step_index
            .saturating_add_signed(delta)
            .min(count.saturating_sub(1));
        if next != self.step_index {
            self.step_index = next;
            self.reset_output();
        }
    }

    pub fn merge_executions(&mut self, executions: Vec<ExecutionView>) {
        for incoming in executions {
            if let Some(old) = self
                .executions
                .iter_mut()
                .find(|old| old.snapshot.id == incoming.snapshot.id)
            {
                if incoming.updated_at_ms >= old.updated_at_ms {
                    *old = incoming;
                }
            } else {
                self.executions.push(incoming);
            }
        }
        self.executions
            .sort_by_key(|execution| std::cmp::Reverse(execution.snapshot.id));
        if self.selection.is_none()
            && let Some(id) = self
                .executions
                .first()
                .map(|execution| execution.snapshot.id)
        {
            self.select(id);
        }
        if let Some(id) = self.selection {
            self.select(id);
        }
    }

    pub fn range(&self, stream: OutputStream) -> OutputRange {
        self.output.get(&stream).map_or(
            OutputRange {
                offset: 0,
                max_bytes: 128 * 1024,
            },
            OutputBuffer::range,
        )
    }

    pub fn apply_output(&mut self, chunks: Vec<OutputChunk>) {
        for chunk in chunks {
            // Replies can race selection changes. Never show the old task's bytes.
            if Some(chunk.step) != self.step() {
                continue;
            }
            self.output_loaded = true;
            self.output.entry(chunk.stream).or_default().append(chunk);
        }
    }

    pub fn output_text(&self) -> String {
        if self.tab == Tab::Activity {
            return self.log.join("\n");
        }
        if self.tab == Tab::Details {
            return self.details();
        }
        let mut sections = Vec::new();
        for stream in [
            OutputStream::Stdout,
            OutputStream::Stderr,
            OutputStream::Terminal,
        ] {
            if self.tab.stream().is_some_and(|selected| selected != stream) {
                continue;
            }
            let Some(buffer) = self.output.get(&stream) else {
                continue;
            };
            if buffer.bytes.is_empty() && buffer.offset == 0 {
                continue;
            }
            if self.tab == Tab::Output {
                sections.push(format!("--- {stream:?} ---"));
            }
            if buffer.offset > 0 {
                sections.push(format!(
                    "[truncated: first retained byte {}]",
                    buffer.offset
                ));
            }
            sections.push(String::from_utf8_lossy(&buffer.bytes).replace("\r\n", "\n"));
        }
        if sections.is_empty() {
            if self.selection.is_none() {
                return "No executions yet. Type a command below and press Enter.".into();
            }
            if !self.output_loaded {
                return "Loading output…".into();
            }
            return "No retained output in this stream.\nOutput is kept in memory and is unavailable after a daemon restart.\nUse [ / ] to inspect another step, or Details for failure reasons.".into();
        }
        sections.join("\n")
    }

    fn details(&self) -> String {
        let Some(execution) = self.selected() else {
            return "Select an execution to inspect its steps.".into();
        };
        let descriptions = leaves(execution);
        let mut lines = vec![format!("{}  {:?}", execution.snapshot.id, execution.state)];
        for (index, step) in execution.snapshot.steps.iter().enumerate() {
            lines.push(format!(
                "\n{}  {}",
                step.id(),
                descriptions
                    .get(index)
                    .map_or("", |(label, _)| label.as_str())
            ));
            lines.push(format!("{:?}", step.state()));
        }
        lines.join("\n")
    }
}

pub(crate) fn leaves(execution: &ExecutionView) -> Vec<(String, Option<IoMode>)> {
    let mut result = Vec::new();
    let mut stack = vec![execution.snapshot.spec.plan()];
    while let Some(plan) = stack.pop() {
        match plan {
            ExecutionPlan::Run { pipeline, io } => result.push((
                pipeline
                    .processes()
                    .map(|process| process.argv().words().join(" "))
                    .collect::<Vec<_>>()
                    .join(" |> "),
                Some(*io),
            )),
            ExecutionPlan::Builtin { command } => result.push((
                match command {
                    BuiltinCommand::Cd(path) => format!("cd {}", path.as_path().display()),
                    BuiltinCommand::Env(_) => "env".into(),
                    BuiltinCommand::Umask(mask) => format!("umask {mask:?}"),
                },
                None,
            )),
            ExecutionPlan::Sequence { first, then, .. } => {
                stack.push(then);
                stack.push(first);
            }
            ExecutionPlan::Parallel { branches, .. } => stack.extend(branches.iter().rev()),
        }
    }
    for (label, _) in &mut result {
        *label = label.replace(|ch: char| ch.is_control(), " ");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(offset: u64, data: &[u8]) -> OutputChunk {
        OutputChunk {
            step: StepId {
                execution: ExecutionId(1),
                index: 1,
            },
            stream: OutputStream::Stdout,
            offset,
            data: data.to_vec(),
            eof: false,
        }
    }

    #[test]
    fn overlapping_and_evicted_output_preserves_absolute_offsets() {
        let mut buffer = OutputBuffer::default();
        buffer.append(chunk(0, b"abc"));
        buffer.append(chunk(1, b"bcdef"));
        assert_eq!(buffer.bytes, b"abcdef");
        assert_eq!(buffer.range().offset, 6);
        buffer.append(chunk(20, b"after eviction"));
        assert_eq!(buffer.offset, 20);
        assert_eq!(buffer.bytes, b"after eviction");
        buffer.append(chunk(34, &vec![b'x'; STREAM_LIMIT + 7]));
        assert_eq!(buffer.bytes.len(), STREAM_LIMIT);
        assert_eq!(buffer.offset, 41);
        assert_eq!(buffer.range().offset, 41 + STREAM_LIMIT as u64);
    }

    #[test]
    fn changing_selection_invalidates_in_flight_output_even_on_return() {
        let mut state = State::default();
        state.select(ExecutionId(1));
        let generation = state.output_generation;
        state.output.insert(
            OutputStream::Stdout,
            OutputBuffer {
                offset: 0,
                bytes: b"old task".to_vec(),
            },
        );
        state.select(ExecutionId(2));
        state.select(ExecutionId(1));
        assert_ne!(generation, state.output_generation);
        assert!(state.output.is_empty());
    }
}
