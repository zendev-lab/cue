//! App state and TEA update loop.
//!
//! Central state machine: all mutations flow through [`AppState::update`]
//! which pattern-matches on app-level messages and delegates to components.

use std::collections::{BTreeMap, HashMap};

use crossterm::event::{KeyCode, KeyEventKind, KeyModifiers, MouseButton, MouseEventKind};
#[cfg(test)]
use crossterm::event::{KeyEvent, MouseEvent};

use cue_core::EventChannel;
use cue_core::cron::CronStatus;
use cue_core::ipc::{
    EventPayload, ExecutionInfo, ForegroundAttachmentInfo, ForegroundRole, JobOpenHint, OkPayload,
    RequestPayload, ResponsePayload, ScheduleInfo,
};
use cue_core::job::JobStatus;
use cue_language::{CompiledCommand, FrontendAction, Mode, compile_command, render_help};
use ratatui::layout::Rect;
use tui_term::vt100;

use crate::card_action::{self, CardAction, CardJob};
use crate::client::{ConnectionController, RestartHandle, WriterHandle};
use crate::clipboard::{self, CopyTarget};
use crate::completion::{self, CompletionScope};
use crate::component::Component;
use crate::component::input_line::{InputLine, InputMsg};
use crate::component::main_view::{CardStatus, MainView, MainViewMsg};
use crate::component::sidebar::{
    CronSidebarRecord, JobSidebarRecord, OverviewCounts, Sidebar, SidebarMsg, overview_counts,
};
use crate::component::status_bar::{StatusBar, StatusBarMsg, compact_session_badge};
use crate::display::{DisplayPane, DisplayPreview, DisplayTabHit};
use crate::focus::{FocusArea, is_mode_switch_key};
use crate::footer::{self, FooterContext};
use crate::foreground;
use crate::geometry::{UiRegions, contains};
use crate::job_picker::{
    CronPickerRecord, JobPickerItem, JobPickerRecord, JobPickerState, job_picker_content_rect,
    job_picker_popup_rect,
};
use crate::message::AppMsg;
use crate::mouse_mode::MouseMode;
use crate::record_format::{self, JobRecord};
use crate::session_binding::connector_with_named_session;
use crate::sidebar_action;
use crate::status_view;
use crate::submission::{self, LocalCommand, PendingSubmission};
#[cfg(test)]
use crate::target_config::{TargetProfileKind, TargetProfileSource, TargetSettingsSnapshot};
use crate::target_config::{connector_for_profile, load_target_settings, save_default_profile};
use crate::target_settings::{
    TargetProfileSaveAction, TargetSettingsState, TargetSettingsView, format_target_settings_view,
    saved_target_profile_feedback, target_profile_supports_live_reconnect,
    target_settings_content_rect, target_settings_popup_rect, target_settings_profile_hit,
};

#[derive(Debug, Clone)]
struct JobRow {
    id: String,
    label: String,
    status: JobStatus,
    start_scope: Option<String>,
    end_scope: Option<String>,
    open_hint: JobOpenHint,
    warnings: Vec<String>,
    pending_reason: Option<String>,
}

#[derive(Debug, Clone)]
struct CronRow {
    id: String,
    label: String,
    status: CronStatus,
}

fn execution_job_status(state: &cue_core::execution::ExecutionState) -> JobStatus {
    match state {
        cue_core::execution::ExecutionState::Queued => JobStatus::Pending,
        cue_core::execution::ExecutionState::Running => JobStatus::Running,
        cue_core::execution::ExecutionState::Succeeded => JobStatus::Done,
        cue_core::execution::ExecutionState::Failed => JobStatus::Failed,
        cue_core::execution::ExecutionState::Cancelled {
            reason: cue_core::execution::ExecutionCancelReason::Forced,
        } => JobStatus::Killed,
        cue_core::execution::ExecutionState::Cancelled { .. } => {
            JobStatus::Cancelled(cue_core::job::CancelReason::User)
        }
    }
}

fn execution_open_hint(execution: &ExecutionInfo) -> JobOpenHint {
    let _ = execution;
    // Foreground control is step-scoped in IPC v3; an execution-level row can
    // always open aggregate output but cannot guess which PTY step to control.
    JobOpenHint::Stream
}

fn execution_spec_label(spec: &cue_core::execution::ExecutionSpec) -> String {
    let mut pipelines = Vec::new();
    collect_execution_pipelines(&spec.plan, &mut pipelines);
    if pipelines.is_empty() {
        "scope update".into()
    } else {
        pipelines.join(" → ")
    }
}

fn execution_label(execution: &ExecutionInfo) -> String {
    execution_spec_label(&execution.spec)
}

fn collect_execution_pipelines(
    plan: &cue_core::execution::ExecutionPlan,
    pipelines: &mut Vec<String>,
) {
    match plan {
        cue_core::execution::ExecutionPlan::Pipeline { pipeline } => {
            pipelines.push(pipeline.to_string());
        }
        cue_core::execution::ExecutionPlan::OnSuccess { left, right }
        | cue_core::execution::ExecutionPlan::OnFailure { left, right }
        | cue_core::execution::ExecutionPlan::Always { left, right } => {
            collect_execution_pipelines(left, pipelines);
            collect_execution_pipelines(right, pipelines);
        }
        cue_core::execution::ExecutionPlan::ParallelAll { branches }
        | cue_core::execution::ExecutionPlan::AnySuccess { branches } => {
            for branch in branches {
                collect_execution_pipelines(branch, pipelines);
            }
        }
        cue_core::execution::ExecutionPlan::ContextDelta { .. } => {}
    }
}

fn format_execution_info(execution: &ExecutionInfo) -> String {
    let mut lines = vec![
        execution.id.to_string(),
        format!("status: {:?}", execution.state).to_ascii_lowercase(),
    ];
    for step in &execution.steps {
        lines.push(format!("{} [{:?}] {}", step.id, step.state, step.pipeline));
    }
    lines.join("\n")
}

fn format_execution_output(steps: &[cue_core::ipc::StepOutput]) -> String {
    let mut sections = Vec::new();
    for step in steps {
        let mut lines = vec![step.id.to_string()];
        if !step.stdout.data.is_empty() {
            lines.push(step.stdout.data.clone());
        }
        if !step.stderr.data.is_empty() {
            lines.push(format!("stderr:\n{}", step.stderr.data));
        }
        sections.push(lines.join("\n"));
    }
    sections.join("\n\n")
}

enum FgSessionKind {
    Job {
        card_index: Option<usize>,
        parser: Box<vt100::Parser>,
    },
}

struct FgSession {
    id: String,
    attachment_id: u64,
    role: ForegroundRole,
    control_available: bool,
    snapshot_truncated: bool,
    role_error: Option<String>,
    kind: FgSessionKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FgRoleRequestKind {
    Claim,
    Release,
}

impl FgRoleRequestKind {
    fn description(self) -> &'static str {
        match self {
            Self::Claim => "claim control",
            Self::Release => "release control",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingFgRoleRequest {
    job_id: String,
    attachment_id: u64,
    kind: FgRoleRequestKind,
}

// ── App state ──

/// Root application state.  Owns all component state and connection info.
pub(crate) struct AppState {
    // Components
    pub(crate) input: InputLine,
    pub(crate) main_view: MainView,
    pub(crate) sidebar: Sidebar,
    pub(crate) status_bar: StatusBar,

    // Connection
    pub(crate) writer: Option<WriterHandle>,
    pub(crate) connected: bool,
    /// Controller for live reconnect / target-switch commands.
    connection_controller: Option<ConnectionController>,
    restart_handle: Option<RestartHandle>,
    /// Profile name we are reconnecting to; set when the user triggers a live
    /// reconnect, cleared and applied to `session_profile_name` on success.
    pending_reconnect_profile_name: Option<String>,

    // Entity state mirrored into the mode-specific sidebar.
    jobs: Vec<JobRow>,
    crons: Vec<CronRow>,
    job_cards: HashMap<String, usize>,
    execution_output: HashMap<String, String>,
    fg_session: Option<FgSession>,
    display: DisplayPane,
    job_picker: Option<JobPickerState>,
    target_settings: Option<TargetSettingsState>,
    target_settings_error: Option<String>,
    pending_submissions: BTreeMap<u32, PendingSubmission>,
    pending_fg_role_requests: BTreeMap<u32, PendingFgRoleRequest>,
    session_profile_name: Option<String>,
    named_session_selector: Option<String>,
    named_session_refresh: bool,

    // UI state
    pub(crate) mode: Mode,
    /// `None` = auto (show when width ≥ 100), `Some` = manual override.
    pub(crate) show_sidebar: Option<bool>,
    pub(crate) focus: FocusArea,
    pub(crate) mouse_mode: MouseMode,
    pub(crate) should_quit: bool,
    pub(crate) terminal_width: u16,
    pub(crate) terminal_height: u16,
}

impl AppState {
    pub(crate) fn new() -> Self {
        let mut state = Self {
            input: InputLine::new(),
            main_view: MainView::new(),
            sidebar: Sidebar::new(),
            status_bar: StatusBar::new(),
            writer: None,
            connected: false,
            connection_controller: None,
            restart_handle: None,
            pending_reconnect_profile_name: None,
            jobs: Vec::new(),
            crons: Vec::new(),
            job_cards: HashMap::new(),
            execution_output: HashMap::new(),
            fg_session: None,
            display: DisplayPane::default(),
            job_picker: None,
            target_settings: None,
            target_settings_error: None,
            pending_submissions: BTreeMap::new(),
            pending_fg_role_requests: BTreeMap::new(),
            session_profile_name: None,
            named_session_selector: None,
            named_session_refresh: false,
            mode: Mode::default(),
            show_sidebar: None,
            focus: FocusArea::Input,
            mouse_mode: MouseMode::UiCapture,
            should_quit: false,
            terminal_width: 80,
            terminal_height: 24,
        };
        state.sync_mode_views();
        state.set_focus(FocusArea::Input);
        state
            .status_bar
            .update(StatusBarMsg::MouseMode(state.mouse_mode));
        state.refresh_clear_action();
        state
    }

    pub(crate) fn set_session_profile_name(&mut self, session_profile_name: Option<String>) {
        self.session_profile_name = session_profile_name;
    }

    pub(crate) fn set_named_session_selector(&mut self, selector: Option<String>) {
        self.named_session_selector = selector.clone();
        self.status_bar.update(StatusBarMsg::NamedSession(selector));
    }

    pub(crate) fn set_named_session_refresh(&mut self, refresh: bool) {
        self.named_session_refresh = refresh;
    }

    /// Store the connection manager controller so the TUI can trigger live
    /// target switches.
    pub(crate) fn set_connection_controller(&mut self, controller: ConnectionController) {
        self.connection_controller = Some(controller);
    }

    pub(crate) fn set_restart_handle(&mut self, restart_handle: Option<RestartHandle>) {
        self.restart_handle = restart_handle;
    }

    /// Whether the sidebar should be visible for the current terminal width.
    pub(crate) fn sidebar_visible(&self) -> bool {
        match self.show_sidebar {
            Some(v) => v,
            None => self.terminal_width >= 100,
        }
    }

    pub(crate) fn debug_job_count(&self) -> usize {
        self.jobs.len()
    }

    pub(crate) fn debug_cron_count(&self) -> usize {
        self.crons.len()
    }

    pub(crate) fn fg_active(&self) -> bool {
        self.fg_session.is_some()
    }

    pub(crate) fn fg_id(&self) -> Option<&str> {
        self.fg_session.as_ref().map(|session| session.id.as_str())
    }

    fn fg_is_controller(&self) -> bool {
        self.fg_session
            .as_ref()
            .is_some_and(|session| session.role == ForegroundRole::Controller)
    }

    pub(crate) fn fg_title(&self) -> String {
        let Some(session) = self.fg_session.as_ref() else {
            return " FG ".to_string();
        };
        let status = match session.role {
            ForegroundRole::Controller => "CONTROL",
            ForegroundRole::Observer if session.control_available => "WATCH · control available",
            ForegroundRole::Observer => "WATCH · control busy",
        };
        let truncated = if session.snapshot_truncated {
            " · partial history"
        } else {
            ""
        };
        let named_session = self
            .named_session_selector
            .as_deref()
            .map(|selector| format!("{} · ", compact_session_badge(selector)))
            .unwrap_or_default();
        format!(" FG {named_session}{} · {status}{truncated} ", session.id)
    }

    pub(crate) fn fg_footer_text(&self) -> String {
        let Some(session) = self.fg_session.as_ref() else {
            return String::new();
        };
        let role_hint = match self.pending_fg_role_kind(&session.id, session.attachment_id) {
            Some(FgRoleRequestKind::Claim) => "[watch · claiming control…]",
            Some(FgRoleRequestKind::Release) => "[control · releasing control…]",
            None => match session.role {
                ForegroundRole::Controller => "[control] Ctrl+] release control",
                ForegroundRole::Observer if session.control_available => {
                    "[watch · control available] Ctrl+] take control"
                }
                ForegroundRole::Observer => "[watch · control busy]",
            },
        };
        let truncated = if session.snapshot_truncated {
            "  ·  history truncated"
        } else {
            ""
        };
        let error = session
            .role_error
            .as_deref()
            .map(|error| format!("  ·  {error}"))
            .unwrap_or_default();
        format!(" {role_hint}  ·  Ctrl+Y copy  ·  Ctrl+Z detach{truncated}{error} ")
    }

    pub(crate) fn fg_screen(&self) -> Option<&vt100::Screen> {
        let session = self.fg_session.as_ref()?;
        let FgSessionKind::Job { parser, .. } = &session.kind;
        Some(parser.screen())
    }

    pub(crate) fn display_pane_title(&self) -> String {
        " Display ".to_string()
    }

    pub(crate) fn display_pane_content(&self) -> &str {
        self.display.content()
    }

    pub(crate) fn display_pane_has_target(&self) -> bool {
        self.display.has_target()
    }

    pub(crate) fn target_settings_open(&self) -> bool {
        self.target_settings.is_some() || self.target_settings_error.is_some()
    }

    pub(crate) fn target_settings_can_save(&self) -> bool {
        self.target_settings
            .as_ref()
            .is_some_and(TargetSettingsState::selected_profile_can_be_saved)
    }

    pub(crate) fn footer_text(&self) -> String {
        footer::footer_text(self.footer_context())
    }

    fn footer_context(&self) -> FooterContext {
        if self.job_picker_open() {
            return FooterContext::JobPicker { mode: self.mode };
        }

        if self.target_settings_open() {
            return FooterContext::TargetSettings {
                can_save: self.target_settings_can_save(),
                has_pending_reconnect: self
                    .target_settings
                    .as_ref()
                    .is_some_and(TargetSettingsState::has_pending_reconnect),
            };
        }

        FooterContext::Main {
            focus: self.focus,
            mode: self.mode,
            display_has_target: self.display_pane_has_target(),
        }
    }

    pub(crate) fn display_tab_labels(&self) -> Vec<String> {
        self.display.labels()
    }

    pub(crate) fn active_display_tab(&self) -> Option<usize> {
        self.display.active()
    }

    fn copy_target(&self) -> Option<CopyTarget> {
        let foreground_target = self
            .fg_id()
            .filter(|_| self.fg_active())
            .zip(self.fg_screen())
            .map(|(job_id, screen)| {
                CopyTarget::new(format!("fg {job_id}"), screen.contents().to_string())
            });
        let target_settings = self
            .target_settings_open()
            .then(|| self.render_target_settings_content())
            .flatten()
            .map(|content| CopyTarget::new("targets", content));
        let display_target = self
            .display
            .copy_target()
            .map(|target| CopyTarget::new(target.label, target.content));
        let latest_record = self.main_view.cards.last().map(|card| {
            CopyTarget::new(
                card.label
                    .clone()
                    .unwrap_or_else(|| "command-record".to_string()),
                record_format::format_card_preview(card),
            )
        });

        clipboard::first_available_target([
            foreground_target,
            target_settings,
            display_target,
            latest_record,
        ])
    }

    pub(crate) fn job_picker_open(&self) -> bool {
        self.job_picker.is_some()
    }

    pub(crate) fn job_picker_selected(&self) -> Option<usize> {
        self.job_picker.as_ref().and_then(JobPickerState::selected)
    }

    pub(crate) fn job_picker_title(&self) -> &'static str {
        crate::job_picker::title(self.mode)
    }

    pub(crate) fn job_picker_empty_text(&self) -> &'static str {
        crate::job_picker::empty_text(self.mode)
    }

    pub(crate) fn job_picker_submit_label(&self) -> &'static str {
        crate::job_picker::submit_label(self.mode)
    }

    pub(crate) fn job_picker_items(&self) -> Vec<JobPickerItem> {
        match self.mode {
            Mode::Job => self
                .jobs
                .iter()
                .filter_map(|job| crate::job_picker::job_picker_item(job_picker_record(job)))
                .collect(),
            Mode::Cron => self
                .crons
                .iter()
                .map(|cron| crate::job_picker::cron_picker_item(cron_picker_record(cron)))
                .collect(),
        }
    }

    pub(crate) fn target_settings_content(&self) -> Option<String> {
        self.render_target_settings_content()
    }

    fn fg_application_cursor(&self) -> bool {
        self.fg_screen()
            .is_some_and(vt100::Screen::application_cursor)
    }

    fn fg_bracketed_paste(&self) -> bool {
        self.fg_screen().is_some_and(vt100::Screen::bracketed_paste)
    }

    fn resize_fg_session(&mut self, cols: u16, rows: u16) {
        if let Some(FgSession {
            kind: FgSessionKind::Job { parser, .. },
            ..
        }) = self.fg_session.as_mut()
        {
            parser.screen_mut().set_size(rows, cols);
        }
    }

    fn pending_fg_role_kind(&self, job_id: &str, attachment_id: u64) -> Option<FgRoleRequestKind> {
        self.pending_fg_role_requests
            .values()
            .find(|request| request.job_id == job_id && request.attachment_id == attachment_id)
            .map(|request| request.kind)
    }

    fn sync_mode_views(&mut self) {
        self.input.update(InputMsg::SetMode(self.mode));
        self.main_view.update(MainViewMsg::SetMode(self.mode));
        self.sidebar.update(SidebarMsg::Mode(self.mode));
        self.status_bar.update(StatusBarMsg::Mode(self.mode));
        self.sync_sidebar_items();
    }

    fn set_focus(&mut self, focus: FocusArea) {
        self.focus = focus;
        self.sidebar
            .update(SidebarMsg::Focused(focus == FocusArea::Sidebar));
    }

    fn layout_regions(&self) -> UiRegions {
        crate::geometry::layout_regions(
            self.terminal_width,
            self.terminal_height,
            self.input.desired_height(),
            self.sidebar_visible(),
        )
    }

    fn sync_sidebar_items(&mut self) {
        let items = match self.mode {
            Mode::Job => self
                .jobs
                .iter()
                .rev()
                .map(|job| crate::component::sidebar::job_sidebar_item(job_sidebar_record(job)))
                .collect(),
            Mode::Cron => self
                .crons
                .iter()
                .rev()
                .map(|cron| crate::component::sidebar::cron_sidebar_item(cron_sidebar_record(cron)))
                .collect(),
        };
        self.sidebar.update(SidebarMsg::Items(items));
        self.refresh_overview();
    }

    fn refresh_overview(&mut self) {
        self.set_overview(overview_counts(
            self.jobs.iter().map(|job| &job.status),
            self.crons.len(),
        ));
    }

    fn refresh_clear_action(&mut self) {
        self.status_bar.update(StatusBarMsg::ClearEnabled(
            self.pending_submissions.is_empty(),
        ));
    }

    fn fg_terminal_size(&self) -> (u16, u16) {
        foreground::terminal_size(self.terminal_width, self.terminal_height)
    }

    fn show_submission_result(
        &mut self,
        pending: &PendingSubmission,
        body: String,
        status: CardStatus,
        label: Option<String>,
    ) -> usize {
        let card_index = pending.card_index().unwrap_or_else(|| {
            self.main_view
                .push_card(pending.input().to_string(), pending.mode())
        });
        if let Some(label) = label {
            self.main_view.set_card_label(card_index, label);
        }
        self.main_view
            .set_card_output(card_index, pending.decorated_output(body));
        self.main_view.set_card_status(card_index, status);
        card_index
    }

    fn handle_pending_error(&mut self, pending: &PendingSubmission, code: &str, message: &str) {
        if let Some(description) = pending.silent_description() {
            tracing::warn!(
                request = %description,
                code = %code,
                message = %message,
                "silent request failed"
            );
        }
    }

    fn open_preview_display(&mut self, preview: DisplayPreview) {
        self.display.open_preview(preview);
    }

    fn append_execution_output(&mut self, id: &cue_core::StepId, data: &str) {
        let execution_id = id.execution.to_string();
        self.execution_output
            .entry(execution_id.clone())
            .or_default()
            .push_str(data);
        if let Some(card_index) = self.job_cards.get(&execution_id).copied() {
            self.main_view.append_card_output(card_index, data);
        }
    }

    fn clear_display_pane(&mut self) {
        self.display.clear();
        self.close_target_settings();
    }

    fn activate_display_tab(&mut self, index: usize) {
        self.display.activate(index);
    }

    fn close_display_tab(&mut self, index: usize) {
        self.display.close(index);
    }

    fn display_tab_hit(&self, display_area: Rect, point: Rect) -> Option<DisplayTabHit> {
        self.display.hit(display_area, point)
    }

    fn inspect_card(&mut self, index: usize) {
        let Some(card) = self.main_view.cards.get(index) else {
            return;
        };
        let job = self.job_for_card(index);

        match card_action::inspect_card_action(index, card, job) {
            CardAction::Foreground { job_id } => {
                self.update(AppMsg::Submit(format!(":fg {job_id}")))
            }
            CardAction::Tail { job_id } => self.update(AppMsg::Submit(format!(":tail {job_id}"))),
            CardAction::Preview(preview) => self.open_preview_display(preview),
        }
    }

    fn job_for_card(&self, index: usize) -> Option<CardJob<'_>> {
        let job_id = self
            .job_cards
            .iter()
            .find(|&(_, &card_idx)| card_idx == index)
            .map(|(id, _)| id.as_str())?;
        let job = self.jobs.iter().find(|job| job.id == job_id)?;
        Some(CardJob {
            id: &job.id,
            status: &job.status,
            open_hint: job.open_hint,
        })
    }

    fn open_job_picker(&mut self) {
        self.close_target_settings();
        let items = self.job_picker_items();
        self.job_picker = Some(JobPickerState::open(items.len()));
    }

    fn open_target_settings(&mut self) {
        if self.target_settings_open() {
            self.close_target_settings();
            return;
        }
        self.close_job_picker();
        self.load_target_settings(None);
        self.set_focus(FocusArea::MainView);
    }

    fn close_target_settings(&mut self) {
        self.target_settings = None;
        self.target_settings_error = None;
    }

    fn load_target_settings(&mut self, notice: Option<String>) {
        let preferred_selection = self
            .target_settings
            .as_ref()
            .and_then(|state| state.selected_profile_name().map(str::to_string));
        match load_target_settings() {
            Ok(snapshot) => {
                let mut state = match notice {
                    Some(notice) => TargetSettingsState::with_notice(snapshot, notice),
                    None => TargetSettingsState::new(snapshot),
                };
                if let Some(profile_name) = preferred_selection.as_deref() {
                    state.select_profile_name(profile_name);
                }
                self.target_settings = Some(state);
                self.target_settings_error = None;
            }
            Err(error) => {
                self.target_settings = None;
                self.target_settings_error =
                    Some(format!("failed to load target settings: {error}"));
            }
        }
    }

    fn render_target_settings_content(&self) -> Option<String> {
        if let Some(error) = &self.target_settings_error {
            return Some(error.clone());
        }
        self.render_target_settings_view().map(|view| view.content)
    }

    fn render_target_settings_view(&self) -> Option<TargetSettingsView> {
        self.target_settings
            .as_ref()
            .map(|state| format_target_settings_view(state, self.session_profile_name.as_deref()))
    }

    fn target_settings_popup_rect(&self) -> Rect {
        target_settings_popup_rect(Rect::new(0, 0, self.terminal_width, self.terminal_height))
    }

    fn target_settings_content_rect(&self) -> Option<Rect> {
        self.target_settings_open()
            .then(|| target_settings_content_rect(self.target_settings_popup_rect()))
    }

    fn move_target_selection(&mut self, delta: isize) {
        let Some(target_settings) = self.target_settings.as_mut() else {
            return;
        };
        target_settings.move_selection(delta);
    }

    fn move_target_selection_to_edge(&mut self, last: bool) {
        let Some(target_settings) = self.target_settings.as_mut() else {
            return;
        };
        if last {
            target_settings.select_last();
        } else {
            target_settings.select_first();
        }
    }

    fn reload_target_settings(&mut self) {
        self.load_target_settings(Some("reloaded target profiles from disk".into()));
        self.set_focus(FocusArea::MainView);
    }

    fn select_target_profile(&mut self, index: usize) {
        let Some(target_settings) = self.target_settings.as_mut() else {
            return;
        };
        target_settings.select_index(index);
    }

    fn target_settings_profile_hit(&self, point: Rect) -> Option<usize> {
        let content_area = self.target_settings_content_rect()?;
        let view = self.render_target_settings_view()?;
        target_settings_profile_hit(&view, content_area, point)
    }

    fn save_selected_target_profile(&mut self) {
        let Some(action) = self
            .target_settings
            .as_ref()
            .and_then(TargetSettingsState::selected_profile_save_action)
        else {
            return;
        };

        let (snapshot, profile_name) = match action {
            TargetProfileSaveAction::Save {
                snapshot,
                profile_name,
            } => (snapshot, profile_name),
            TargetProfileSaveAction::Notice(notice) => {
                if let Some(state) = self.target_settings.as_mut() {
                    state.set_notice_without_pending_reconnect(notice);
                }
                return;
            }
        };

        match save_default_profile(&profile_name, &snapshot) {
            Ok(snapshot) => {
                let selected_profile = snapshot.profiles.iter().find(|p| p.name == profile_name);
                let can_live_reconnect = self.connection_controller.is_some()
                    && selected_profile.is_some_and(target_profile_supports_live_reconnect);
                let feedback = saved_target_profile_feedback(
                    &snapshot,
                    &profile_name,
                    self.session_profile_name.as_deref(),
                    can_live_reconnect,
                );
                self.target_settings = Some(TargetSettingsState::with_save_feedback(
                    snapshot,
                    &profile_name,
                    feedback,
                ));
                self.target_settings_error = None;
            }
            Err(error) => {
                if let Some(state) = self.target_settings.as_mut() {
                    state.set_notice_without_pending_reconnect(format!("save failed: {error}"));
                }
            }
        }
    }

    /// Dispatch a live `SwitchTarget` command to the connection manager for
    /// the profile stored in `pending_reconnect_profile`.
    fn trigger_reconnect_now(&mut self) {
        let profile_name = self
            .target_settings
            .as_ref()
            .and_then(|state| state.pending_reconnect_profile_name().map(str::to_owned));
        let Some(profile_name) = profile_name else {
            return;
        };

        let connector = match connector_for_profile(&profile_name) {
            Ok(c) => c,
            Err(error) => {
                if let Some(state) = self.target_settings.as_mut() {
                    state
                        .set_notice_without_pending_reconnect(format!("reconnect failed: {error}"));
                }
                return;
            }
        };
        let connector = connector_with_named_session(
            connector,
            self.named_session_selector.clone(),
            self.named_session_refresh,
        );

        if let Some(ref controller) = self.connection_controller {
            if controller.switch_target(connector).is_err() {
                if let Some(state) = self.target_settings.as_mut() {
                    state.set_notice_without_pending_reconnect(
                        "reconnect command could not be sent; try again",
                    );
                }
                return;
            }
            self.pending_reconnect_profile_name = Some(profile_name.clone());
            if let Some(state) = self.target_settings.as_mut() {
                state.set_notice_without_pending_reconnect(format!(
                    "reconnecting to `{profile_name}`…"
                ));
            }
        }
    }

    fn restart_daemon(&mut self) {
        let card_index = self.main_view.push_card(":restart".into(), self.mode);
        match &self.restart_handle {
            Some(handle) => match handle.restart() {
                Ok(()) => {
                    self.main_view.set_card_output(
                        card_index,
                        "daemon restart requested; waiting for reconnect".into(),
                    );
                    self.main_view
                        .set_card_status(card_index, CardStatus::Pending);
                }
                Err(error) => {
                    self.main_view.set_card_output(
                        card_index,
                        format!("Error [restart]: failed to restart daemon: {error:#}"),
                    );
                    self.main_view
                        .set_card_status(card_index, CardStatus::Error);
                }
            },
            None => {
                self.main_view.set_card_output(
                    card_index,
                    "Error [restart]: restart is unavailable for this session".into(),
                );
                self.main_view
                    .set_card_status(card_index, CardStatus::Error);
            }
        }
    }

    fn close_job_picker(&mut self) {
        self.job_picker = None;
    }

    fn move_job_picker(&mut self, delta: isize) {
        let items_len = self.job_picker_items().len();
        if let Some(picker) = self.job_picker.as_mut() {
            picker.move_selection(delta, items_len);
        }
    }

    fn kill_selected_job_from_picker(&mut self) {
        let Some(selected) = self.job_picker.as_ref().and_then(JobPickerState::selected) else {
            self.close_job_picker();
            return;
        };
        let items = self.job_picker_items();
        let Some(target_id) = items.get(selected).map(|item| item.id.clone()) else {
            self.close_job_picker();
            return;
        };
        self.close_job_picker();
        self.update(AppMsg::Submit(format!(":kill {target_id}")));
    }

    fn enqueue_silent_request(
        &mut self,
        payload: RequestPayload,
        description: &str,
        pending: PendingSubmission,
    ) -> bool {
        let Some(writer) = &self.writer else {
            return false;
        };
        match writer.try_send(payload) {
            Ok(request_id) => {
                self.track_pending_submission(request_id, pending);
                true
            }
            Err(error) => {
                tracing::warn!("failed to send {description}: {error}");
                false
            }
        }
    }

    fn subscribe_core_channels(&mut self) {
        let _ = self.enqueue_silent_request(
            RequestPayload::subscribe(&[EventChannel::Executions, EventChannel::System]),
            "core subscriptions",
            PendingSubmission::silent_request("core subscriptions"),
        );
    }

    fn request_sidebar_snapshots(&mut self) {
        for (description, payload) in [
            (
                ":executions",
                RequestPayload::ListExecutions { limit: None },
            ),
            (":schedules", RequestPayload::ListSchedules { limit: None }),
        ] {
            if !self.enqueue_silent_request(
                payload,
                description,
                PendingSubmission::silent_request(description),
            ) {
                break;
            }
        }
    }

    fn track_pending_submission(&mut self, request_id: u32, pending: PendingSubmission) {
        self.pending_submissions.insert(request_id, pending);
    }

    fn take_pending_submission(&mut self, request_id: u32) -> Option<PendingSubmission> {
        self.pending_submissions.remove(&request_id)
    }

    fn start_fg_session(
        &mut self,
        attachment: ForegroundAttachmentInfo,
        card_index: Option<usize>,
    ) {
        let (cols, rows) = self.fg_terminal_size();
        let mut parser = Box::new(vt100::Parser::new(rows, cols, 0));
        parser.process(&attachment.snapshot);
        self.pending_fg_role_requests.clear();
        self.fg_session = Some(FgSession {
            id: attachment.id.to_string(),
            attachment_id: attachment.attachment_id,
            role: attachment.role,
            control_available: attachment.control_available,
            snapshot_truncated: attachment.snapshot_truncated,
            role_error: None,
            kind: FgSessionKind::Job { card_index, parser },
        });
    }

    fn append_fg_output(&mut self, id: &str, attachment_id: u64, data: &[u8]) {
        let Some(FgSession {
            id: attached_id,
            attachment_id: active_attachment_id,
            kind: FgSessionKind::Job { parser, .. },
            ..
        }) = self.fg_session.as_mut()
        else {
            return;
        };
        if !foreground_event_matches(attached_id, *active_attachment_id, id, attachment_id) {
            tracing::debug!(event_job_id = %id, event_attachment_id = attachment_id, %attached_id, active_attachment_id = *active_attachment_id, "ignoring foreign foreground output");
            return;
        }
        parser.process(data);
    }

    fn update_fg_control_available(
        &mut self,
        id: &str,
        attachment_id: u64,
        control_available: bool,
    ) {
        let Some(session) = self.fg_session.as_mut() else {
            return;
        };
        if !foreground_event_matches(&session.id, session.attachment_id, id, attachment_id) {
            return;
        }
        session.control_available = control_available;
    }

    fn apply_fg_role_changed(
        &mut self,
        id: &str,
        attachment_id: u64,
        role: ForegroundRole,
        control_available: bool,
    ) {
        let Some(session) = self.fg_session.as_mut() else {
            return;
        };
        if !foreground_event_matches(&session.id, session.attachment_id, id, attachment_id) {
            tracing::debug!(response_job_id = %id, response_attachment_id = attachment_id, attached_job_id = %session.id, active_attachment_id = session.attachment_id, "ignoring stale foreground role response");
            return;
        }
        session.role = role;
        session.control_available = control_available;
        session.role_error = None;

        if role == ForegroundRole::Controller {
            let (cols, rows) = self.fg_terminal_size();
            self.send_fg_resize(cols, rows);
        }
    }

    fn note_fg_role_failure(&mut self, request: &PendingFgRoleRequest, code: &str, message: &str) {
        let Some(session) = self.fg_session.as_mut() else {
            return;
        };
        if session.id != request.job_id || session.attachment_id != request.attachment_id {
            return;
        }
        session.role_error = Some(format!(
            "{} failed [{code}]: {message}",
            request.kind.description()
        ));
    }

    fn finish_fg_session(&mut self, id: &str, attachment_id: u64, reason: &str) {
        let Some(session) = self.fg_session.take() else {
            return;
        };
        if !foreground_event_matches(&session.id, session.attachment_id, id, attachment_id) {
            self.fg_session = Some(session);
            return;
        }

        self.pending_fg_role_requests.retain(|_, request| {
            request.job_id != session.id || request.attachment_id != session.attachment_id
        });

        let FgSessionKind::Job { card_index, parser } = session.kind;
        if let Some(card_index) = card_index {
            let status = if reason == "done" || reason == "detached" {
                CardStatus::Success
            } else {
                CardStatus::Error
            };
            self.main_view.set_card_status(card_index, status);
            let rendered =
                String::from_utf8_lossy(&parser.screen().contents_formatted()).into_owned();
            if !rendered.is_empty() {
                self.main_view.set_card_output(card_index, rendered);
            }
        }
    }

    fn detach_fg_session(&mut self) {
        let is_job = self
            .fg_session
            .as_ref()
            .is_some_and(|session| matches!(session.kind, FgSessionKind::Job { .. }));
        if is_job {
            self.send_fg_detach();
        } else {
            self.fg_session = None;
            self.sync_mode_views();
        }
    }

    fn send_fg_detach(&self) {
        self.send_foreground_request(RequestPayload::StepDetach {}, "foreground detach");
    }

    fn send_fg_input(&self, data: Vec<u8>) {
        if !self.fg_is_controller() {
            return;
        }
        self.send_foreground_request(RequestPayload::StepInput { data }, "foreground input");
    }

    fn send_fg_resize(&self, cols: u16, rows: u16) {
        if !self.fg_is_controller() {
            return;
        }
        self.send_foreground_request(
            RequestPayload::StepResize { cols, rows },
            "foreground resize",
        );
    }

    fn toggle_fg_control(&mut self) {
        let Some(session) = self.fg_session.as_ref() else {
            return;
        };
        if self
            .pending_fg_role_kind(&session.id, session.attachment_id)
            .is_some()
        {
            return;
        }
        let job_id = session.id.clone();
        let attachment_id = session.attachment_id;
        let kind = match session.role {
            ForegroundRole::Controller => FgRoleRequestKind::Release,
            ForegroundRole::Observer if session.control_available => FgRoleRequestKind::Claim,
            ForegroundRole::Observer => {
                if let Some(session) = self.fg_session.as_mut() {
                    session.role_error = Some("control is currently busy".to_string());
                }
                return;
            }
        };
        let payload = match kind {
            FgRoleRequestKind::Claim => RequestPayload::StepClaimControl {},
            FgRoleRequestKind::Release => RequestPayload::StepReleaseControl {},
        };
        let Some(writer) = &self.writer else {
            if let Some(session) = self.fg_session.as_mut() {
                session.role_error =
                    Some(format!("{} failed: cued is offline", kind.description()));
            }
            return;
        };
        match writer.try_send(payload) {
            Ok(request_id) => {
                if let Some(session) = self.fg_session.as_mut() {
                    session.role_error = None;
                }
                self.pending_fg_role_requests.insert(
                    request_id,
                    PendingFgRoleRequest {
                        job_id,
                        attachment_id,
                        kind,
                    },
                );
            }
            Err(error) => {
                if let Some(session) = self.fg_session.as_mut() {
                    session.role_error = Some(format!("{} failed: {error}", kind.description()));
                }
            }
        }
    }

    fn send_foreground_request(&self, payload: RequestPayload, description: &str) {
        if let Some(writer) = &self.writer
            && let Err(error) = writer.send(payload)
        {
            tracing::warn!("failed to send {description}: {error}");
        }
    }

    fn copy_focus(&self) {
        let Some(target) = self.copy_target() else {
            return;
        };
        if let Err(error) = clipboard::copy_to_clipboard(&target.content) {
            tracing::warn!(%error, target = %target.label, "failed to copy content");
        }
    }

    fn fail_pending_submissions(&mut self, message: &str) {
        let pending = std::mem::take(&mut self.pending_submissions);
        for (_, pending) in pending {
            if !pending.is_user_visible() {
                continue;
            }
            self.show_submission_result(&pending, message.to_string(), CardStatus::Error, None);
        }
        self.refresh_clear_action();
    }

    fn upsert_job(
        &mut self,
        id: String,
        label: String,
        status: JobStatus,
        start_scope: Option<String>,
        open_hint: JobOpenHint,
        warnings: Vec<String>,
    ) {
        if let Some(job) = self.jobs.iter_mut().find(|job| job.id == id) {
            if !label.is_empty() {
                job.label = label;
            }
            job.status = status;
            if start_scope.is_some() {
                job.start_scope = start_scope;
            }
            job.open_hint = open_hint;
            if !warnings.is_empty() {
                job.warnings = warnings;
            }
            if job.status != JobStatus::Pending {
                job.pending_reason = None;
            }
            return;
        }

        self.jobs.push(JobRow {
            id,
            label,
            status,
            start_scope,
            end_scope: None,
            open_hint,
            warnings,
            pending_reason: None,
        });
    }

    fn upsert_execution(&mut self, execution: &ExecutionInfo) {
        self.upsert_job(
            execution.id.to_string(),
            execution_label(execution),
            execution_job_status(&execution.state),
            execution.spec.start_scope.map(|scope| scope.to_string()),
            execution_open_hint(execution),
            Vec::new(),
        );
    }

    fn update_job_status(&mut self, id: &str, status: JobStatus, end_scope: Option<String>) {
        if let Some(index) = self.jobs.iter().position(|job| job.id == id) {
            self.jobs[index].status = status;
            if end_scope.is_some() {
                self.jobs[index].end_scope = end_scope;
            }
            if self.jobs[index].status != JobStatus::Pending {
                self.jobs[index].pending_reason = None;
            }
            if let Some(card_index) = self.job_cards.get(id).copied() {
                let job = self.jobs[index].clone();
                self.refresh_job_card(card_index, &job);
            }
        } else {
            self.jobs.push(JobRow {
                id: id.to_string(),
                label: id.to_string(),
                status,
                start_scope: None,
                end_scope,
                open_hint: JobOpenHint::Stream,
                warnings: Vec::new(),
                pending_reason: None,
            });
        }
    }

    fn refresh_job_card(&mut self, card_index: usize, job: &JobRow) {
        self.main_view.set_card_output(
            card_index,
            record_format::format_job_record(job_record(job)),
        );
        self.main_view
            .set_card_status(card_index, status_view::job_card_status(&job.status));
    }

    fn upsert_cron(&mut self, id: String, label: String, status: CronStatus) {
        if let Some(cron) = self.crons.iter_mut().find(|cron| cron.id == id) {
            if !label.is_empty() {
                cron.label = label;
            }
            cron.status = status;
            return;
        }

        self.crons.push(CronRow { id, label, status });
    }

    fn replace_executions(&mut self, executions: Vec<ExecutionInfo>) {
        self.jobs.clear();
        for execution in &executions {
            self.upsert_execution(execution);
        }
    }

    fn replace_schedules(&mut self, schedules: Vec<ScheduleInfo>) {
        self.crons = schedules
            .into_iter()
            .map(|schedule| CronRow {
                id: schedule.id.to_string(),
                label: format!(
                    "{} {}",
                    schedule.schedule.display(),
                    execution_spec_label(&schedule.execution)
                ),
                status: schedule.status,
            })
            .collect();
    }

    fn open_cron_row(&mut self, row: usize) {
        let Some(cron) = self.crons.get(row).cloned() else {
            return;
        };
        self.open_preview_display(DisplayPreview::new(
            format!("cron:{}", cron.id),
            format!("cron {}", cron.id),
            record_format::format_cron_preview(&cron.id, &cron.label, cron.status),
        ));
    }

    fn activate_sidebar_row(&mut self, row: usize) {
        match self.mode {
            Mode::Job => {
                let Some(idx) = sidebar_action::display_row_to_index(row, self.jobs.len()) else {
                    return;
                };
                let Some(job) = self.jobs.get(idx) else {
                    return;
                };
                self.update(AppMsg::Submit(sidebar_action::job_open_command(
                    &job.id,
                    &job.status,
                    job.open_hint,
                )));
            }
            Mode::Cron => {
                if let Some(idx) = sidebar_action::display_row_to_index(row, self.crons.len()) {
                    self.open_cron_row(idx);
                }
            }
        }
    }

    fn selected_sidebar_kill_command(&self) -> Option<String> {
        let row = self.sidebar.selected?;
        match self.mode {
            Mode::Job => {
                let idx = sidebar_action::display_row_to_index(row, self.jobs.len())?;
                let job = self.jobs.get(idx)?;
                sidebar_action::running_job_kill_command(&job.id, &job.status)
            }
            Mode::Cron => {
                let idx = sidebar_action::display_row_to_index(row, self.crons.len())?;
                let cron = self.crons.get(idx)?;
                Some(sidebar_action::cron_kill_command(&cron.id))
            }
        }
    }

    fn complete_input(&mut self) {
        let range = self.input.current_word_range();
        let candidates = match self.completion_candidates() {
            Ok(candidates) => candidates,
            Err(error) => {
                self.show_completion_error(error);
                return;
            }
        };
        let word = self.input.content[range.clone()].to_string();
        let Some(replacement) = completion::completion_replacement(&candidates, &word) else {
            return;
        };
        self.input.replace_range(range, &replacement);
    }

    fn show_completion_error(&mut self, error: anyhow::Error) {
        let record = completion::completion_error_record(&error);
        let card_index = self.main_view.push_card(record.input, self.mode);
        self.main_view.set_card_output(card_index, record.output);
        self.main_view
            .set_card_status(card_index, CardStatus::Error);
        self.main_view.set_card_label(card_index, record.label);
    }

    fn completion_candidates(&self) -> anyhow::Result<Vec<String>> {
        let execution_ids = self
            .jobs
            .iter()
            .map(|job| job.id.clone())
            .collect::<Vec<_>>();
        let schedule_ids = self
            .crons
            .iter()
            .map(|cron| cron.id.clone())
            .collect::<Vec<_>>();
        completion::completion_candidates(CompletionScope {
            mode: self.mode,
            content: &self.input.content,
            cursor: self.input.cursor,
            word_range: self.input.current_word_range(),
            execution_ids: &execution_ids,
            step_ids: &[],
            schedule_ids: &schedule_ids,
        })
    }

    /// TEA update: apply a message to the state.
    pub(crate) fn update(&mut self, msg: AppMsg) {
        match msg {
            AppMsg::FatalError { .. } => {
                self.should_quit = true;
            }

            AppMsg::Quit => {
                self.should_quit = true;
            }

            AppMsg::Resize(w, h) => {
                self.terminal_width = w;
                self.terminal_height = h;
                if self.fg_active() {
                    let (cols, rows) = self.fg_terminal_size();
                    self.resize_fg_session(cols, rows);
                    self.send_fg_resize(cols, rows);
                }
            }

            AppMsg::Tick => {
                // Status bar re-renders on every draw, clock updates automatically.
            }

            AppMsg::ModeSwitch => {
                if self.fg_active() {
                    return;
                }
                self.mode = self.mode.next();
                self.sync_mode_views();
            }

            AppMsg::ToggleSidebar => {
                let currently_visible = self.sidebar_visible();
                self.show_sidebar = Some(!currently_visible);
            }

            AppMsg::ToggleMouseMode => {
                self.mouse_mode = self.mouse_mode.toggle();
                self.status_bar
                    .update(StatusBarMsg::MouseMode(self.mouse_mode));
            }

            AppMsg::CopyFocus => {
                self.copy_focus();
            }

            AppMsg::ClearDisplay => {
                if self.pending_submissions.is_empty() {
                    self.main_view.clear_all();
                    self.clear_display_pane();
                    self.job_cards.clear();
                    self.refresh_clear_action();
                }
            }

            AppMsg::OpenJobPicker => {
                self.open_job_picker();
            }

            AppMsg::KillSelection => {
                if let Some(command) = self.selected_sidebar_kill_command() {
                    self.update(AppMsg::Submit(command));
                }
            }

            AppMsg::OpenTargetSettings => {
                self.open_target_settings();
            }

            AppMsg::Paste(text) => {
                if self.fg_active() {
                    self.send_fg_input(foreground::paste_bytes(&text, self.fg_bracketed_paste()));
                    return;
                }
                if self.job_picker_open() {
                    return;
                }
                self.set_focus(FocusArea::Input);
                self.input.insert_text(&text);
            }

            AppMsg::Submit(text) => {
                if let Some(local) = submission::parse_local_command(&text) {
                    self.input.update(InputMsg::Clear);
                    match local {
                        LocalCommand::Clear => self.update(AppMsg::ClearDisplay),
                        LocalCommand::Quit => self.update(AppMsg::Quit),
                        LocalCommand::Restart => self.restart_daemon(),
                    }
                    return;
                }

                let warnings = submission::operator_spacing_warnings(&text);
                let card_index = submission::precreates_card(&text, self.mode, &warnings)
                    .then(|| self.main_view.push_card(text.clone(), self.mode));
                self.input.update(InputMsg::Clear);

                let compiled = match compile_command(&text, self.mode, "<tui>") {
                    Ok(compiled) => compiled,
                    Err(error) => {
                        let pending =
                            PendingSubmission::user(card_index, text, self.mode, warnings);
                        self.show_submission_result(
                            &pending,
                            format!("Error [syntax]: {error}"),
                            CardStatus::Error,
                            None,
                        );
                        return;
                    }
                };

                let (payload, pending) = match compiled {
                    CompiledCommand::Daemon(payload) => (
                        payload,
                        PendingSubmission::user(card_index, text, self.mode, warnings),
                    ),
                    CompiledCommand::Frontend(FrontendAction::Help { topic }) => {
                        let pending =
                            PendingSubmission::user(card_index, text, self.mode, warnings);
                        self.show_submission_result(
                            &pending,
                            render_help(topic.as_deref()),
                            CardStatus::Success,
                            None,
                        );
                        return;
                    }
                    CompiledCommand::Frontend(FrontendAction::Clear) => {
                        self.update(AppMsg::ClearDisplay);
                        return;
                    }
                    CompiledCommand::Frontend(FrontendAction::Quit) => {
                        self.update(AppMsg::Quit);
                        return;
                    }
                    CompiledCommand::Frontend(FrontendAction::Restart) => {
                        self.restart_daemon();
                        return;
                    }
                    CompiledCommand::Frontend(FrontendAction::Retry { id }) => (
                        RequestPayload::GetExecution { id },
                        PendingSubmission::retry(card_index, text, self.mode, warnings, id),
                    ),
                    CompiledCommand::Frontend(FrontendAction::Unsupported { message }) => {
                        let pending =
                            PendingSubmission::user(card_index, text, self.mode, warnings);
                        self.show_submission_result(
                            &pending,
                            format!("Error [unsupported]: {message}"),
                            CardStatus::Error,
                            None,
                        );
                        return;
                    }
                };

                if let Some(ref writer) = self.writer {
                    match writer.try_send(payload) {
                        Ok(request_id) => {
                            self.track_pending_submission(request_id, pending);
                            self.refresh_clear_action();
                        }
                        Err(e) => {
                            tracing::warn!("failed to send command: {e}");
                            self.show_submission_result(
                                &pending,
                                format!("Error [transport]: failed to send command: {e}"),
                                CardStatus::Error,
                                None,
                            );
                        }
                    }
                } else {
                    self.show_submission_result(
                        &pending,
                        "Error [offline]: cued is not connected".to_string(),
                        CardStatus::Error,
                        None,
                    );
                }
            }

            AppMsg::Connected => {
                self.connected = true;
                self.status_bar.update(StatusBarMsg::Connected(true));
                self.sync_mode_views();
                self.subscribe_core_channels();
                self.request_sidebar_snapshots();
                self.refresh_clear_action();
            }

            AppMsg::Disconnected => {
                self.fail_pending_submissions("Error [transport]: cued disconnected");
                self.fg_session = None;
                self.pending_fg_role_requests.clear();
                self.connected = false;
                self.writer = None;
                self.close_job_picker();
                self.status_bar.update(StatusBarMsg::Connected(false));
            }

            AppMsg::ReconnectFailed { message } => {
                self.connected = false;
                self.writer = None;
                self.status_bar.update(StatusBarMsg::Connected(false));
                if let Some(state) = self.target_settings.as_mut() {
                    state.set_notice(match self.pending_reconnect_profile_name.as_deref() {
                        Some(profile) => {
                            format!("reconnect to `{profile}` failed: {message}; retrying")
                        }
                        None => format!("reconnect failed: {message}; retrying"),
                    });
                }
            }

            AppMsg::Reconnected { writer } => {
                self.writer = Some(writer);
                self.connected = true;
                self.status_bar.update(StatusBarMsg::Connected(true));
                self.sync_mode_views();
                self.subscribe_core_channels();
                self.request_sidebar_snapshots();
                self.refresh_clear_action();
                // If this reconnect was triggered by a live target switch, apply
                // the new profile name and show confirmation.
                if let Some(profile) = self.pending_reconnect_profile_name.take() {
                    self.session_profile_name = Some(profile.clone());
                    if let Some(state) = self.target_settings.as_mut() {
                        state.set_notice(format!("connected to `{profile}`"));
                    }
                }
            }

            AppMsg::Response { id, payload } => {
                let pending_fg_role = self.pending_fg_role_requests.remove(&id);
                if let Some(request) = pending_fg_role.as_ref() {
                    match &payload {
                        ResponsePayload::Ok(OkPayload::FgRoleChanged {
                            id: response_job_id,
                            attachment_id: response_attachment_id,
                            ..
                        }) if response_job_id.to_string() == request.job_id
                            && response_attachment_id == &request.attachment_id => {}
                        ResponsePayload::Err { code, message } => {
                            self.note_fg_role_failure(request, code, message);
                        }
                        _ => self.note_fg_role_failure(
                            request,
                            "PROTOCOL",
                            "daemon returned an unexpected role response",
                        ),
                    }
                }
                let pending = self.take_pending_submission(id);
                self.refresh_clear_action();

                match payload {
                    ResponsePayload::Ok(ok) => match ok {
                        OkPayload::Ack {} => {
                            if let Some(pending) = pending.as_ref()
                                && pending.is_user_visible()
                            {
                                self.show_submission_result(
                                    pending,
                                    pending.ack_message(),
                                    CardStatus::Success,
                                    None,
                                );
                            }
                        }
                        OkPayload::ExecutionCreated { execution } => {
                            self.upsert_execution(&execution);
                            self.sync_sidebar_items();
                            if let Some(pending) = pending.as_ref()
                                && pending.is_user_visible()
                            {
                                let id = execution.id.to_string();
                                let body = match self.execution_output.get(&id) {
                                    Some(output) if !output.is_empty() => {
                                        format!("{output}\n{}", format_execution_info(&execution))
                                    }
                                    _ => format_execution_info(&execution),
                                };
                                let card_index = self.show_submission_result(
                                    pending,
                                    body,
                                    status_view::job_card_status(&execution_job_status(
                                        &execution.state,
                                    )),
                                    Some(id.clone()),
                                );
                                self.job_cards.insert(id, card_index);
                            }
                        }
                        OkPayload::ExecutionInfo(execution) => {
                            if let Some(pending) = pending.as_ref()
                                && let Some(retry_id) = pending.retry_id()
                            {
                                if execution.id != retry_id {
                                    self.show_submission_result(
                                        pending,
                                        "Error [protocol]: retry lookup returned another execution"
                                            .into(),
                                        CardStatus::Error,
                                        None,
                                    );
                                    return;
                                }
                                let mut spec = execution.spec.clone();
                                spec.retry_of = Some(retry_id);
                                spec.launch_context.spawn_adapter = None;
                                let retry_pending = pending.as_user();
                                let Some(writer) = self.writer.as_ref() else {
                                    self.show_submission_result(
                                        &retry_pending,
                                        "Error [offline]: cued is not connected".into(),
                                        CardStatus::Error,
                                        None,
                                    );
                                    return;
                                };
                                match writer.try_send(RequestPayload::SubmitExecution {
                                    spec: Box::new(spec),
                                }) {
                                    Ok(request_id) => {
                                        self.track_pending_submission(request_id, retry_pending);
                                        self.refresh_clear_action();
                                    }
                                    Err(error) => {
                                        self.show_submission_result(
                                            &retry_pending,
                                            format!(
                                                "Error [transport]: failed to submit retry: {error}"
                                            ),
                                            CardStatus::Error,
                                            None,
                                        );
                                    }
                                }
                                return;
                            }
                            self.upsert_execution(&execution);
                            self.sync_sidebar_items();
                            if let Some(pending) = pending.as_ref()
                                && pending.is_user_visible()
                            {
                                self.show_submission_result(
                                    pending,
                                    format_execution_info(&execution),
                                    status_view::job_card_status(&execution_job_status(
                                        &execution.state,
                                    )),
                                    Some(execution.id.to_string()),
                                );
                            }
                        }
                        OkPayload::ExecutionList(executions) => {
                            let count = executions.len();
                            self.replace_executions(executions);
                            self.sync_sidebar_items();
                            if let Some(pending) = pending.as_ref()
                                && pending.is_user_visible()
                            {
                                self.show_submission_result(
                                    pending,
                                    format!("loaded {count} execution(s) into sidebar"),
                                    CardStatus::Success,
                                    None,
                                );
                            }
                        }
                        OkPayload::ExecutionOutput { id, steps } => {
                            if let Some(pending) = pending.as_ref()
                                && pending.is_user_visible()
                            {
                                self.show_submission_result(
                                    pending,
                                    format_execution_output(&steps),
                                    CardStatus::Success,
                                    Some(id.to_string()),
                                );
                            }
                        }
                        OkPayload::ScheduleCreated { schedule } => {
                            self.upsert_cron(
                                schedule.id.to_string(),
                                format!(
                                    "{} {}",
                                    schedule.schedule.display(),
                                    execution_spec_label(&schedule.execution)
                                ),
                                schedule.status,
                            );
                            self.sync_sidebar_items();
                            if let Some(pending) = pending.as_ref()
                                && pending.is_user_visible()
                            {
                                self.show_submission_result(
                                    pending,
                                    schedule.id.to_string(),
                                    CardStatus::Success,
                                    Some(schedule.id.to_string()),
                                );
                            }
                        }
                        OkPayload::ScheduleList(schedules) => {
                            let count = schedules.len();
                            self.replace_schedules(schedules);
                            self.sync_sidebar_items();
                            if let Some(pending) = pending.as_ref()
                                && pending.is_user_visible()
                            {
                                self.show_submission_result(
                                    pending,
                                    format!("loaded {count} schedule(s) into sidebar"),
                                    CardStatus::Success,
                                    None,
                                );
                            }
                        }
                        OkPayload::FgAttached(attachment) => {
                            let attachment = *attachment;
                            let id = attachment.id.to_string();
                            let card_index = if let Some(pending) = pending.as_ref()
                                && pending.is_user_visible()
                            {
                                Some(self.show_submission_result(
                                    pending,
                                    id.clone(),
                                    CardStatus::Streaming,
                                    Some(id.clone()),
                                ))
                            } else {
                                None
                            };
                            self.start_fg_session(attachment, card_index);
                            let (cols, rows) = self.fg_terminal_size();
                            self.send_fg_resize(cols, rows);
                        }
                        OkPayload::FgRoleChanged {
                            id,
                            attachment_id,
                            role,
                            control_available,
                        } => {
                            self.apply_fg_role_changed(
                                &id.to_string(),
                                attachment_id,
                                role,
                                control_available,
                            );
                        }
                        OkPayload::ScopeCreated { hash, summary } => {
                            if let Some(pending) = pending.as_ref()
                                && pending.is_user_visible()
                            {
                                self.show_submission_result(
                                    pending,
                                    summary,
                                    CardStatus::Success,
                                    Some(hash),
                                );
                            }
                        }
                        OkPayload::Pong { version, .. } => {
                            tracing::debug!(?version, "pong received");
                        }
                        _ => {
                            if let Some(pending) = pending.as_ref()
                                && pending.is_user_visible()
                            {
                                let text = format!("{ok:?}");
                                self.show_submission_result(
                                    pending,
                                    text,
                                    CardStatus::Success,
                                    None,
                                );
                            }
                        }
                    },
                    ResponsePayload::Err { code, message } => {
                        if let Some(pending) = pending.as_ref() {
                            if pending.is_user_visible() {
                                self.show_submission_result(
                                    pending,
                                    format!("Error [{code}]: {message}"),
                                    CardStatus::Error,
                                    None,
                                );
                            } else {
                                self.handle_pending_error(pending, &code, &message);
                            }
                        }
                    }
                }
            }

            AppMsg::ServerEvent(event) => match event {
                EventPayload::ExecutionCreated { execution } => {
                    self.upsert_execution(&execution);
                    self.sync_sidebar_items();
                }
                EventPayload::ExecutionStateChanged { id, new_state, .. } => {
                    self.update_job_status(&id.to_string(), execution_job_status(&new_state), None);
                    self.sync_sidebar_items();
                }
                EventPayload::StepStateChanged { .. } => {}
                EventPayload::ExecutionFinished { execution } => {
                    self.upsert_execution(&execution);
                    if let Some(card_index) = self.job_cards.get(&execution.id.to_string()).copied()
                    {
                        let execution_id = execution.id.to_string();
                        let body = match self.execution_output.get(&execution_id) {
                            Some(output) if !output.is_empty() => {
                                format!("{output}\n{}", format_execution_info(&execution))
                            }
                            _ => format_execution_info(&execution),
                        };
                        self.main_view.set_card_output(card_index, body);
                        self.main_view.set_card_status(
                            card_index,
                            status_view::job_card_status(&execution_job_status(&execution.state)),
                        );
                    }
                    self.sync_sidebar_items();
                }
                EventPayload::OutputChunk {
                    id,
                    stream: _,
                    data,
                } => {
                    self.append_execution_output(&id, &String::from_utf8_lossy(&data));
                }
                EventPayload::FgOutput {
                    id,
                    attachment_id,
                    data,
                } => {
                    self.append_fg_output(&id.to_string(), attachment_id, &data);
                }
                EventPayload::FgControlChanged {
                    id,
                    attachment_id,
                    control_available,
                } => {
                    self.update_fg_control_available(
                        &id.to_string(),
                        attachment_id,
                        control_available,
                    );
                }
                EventPayload::FgExited {
                    id,
                    attachment_id,
                    reason,
                } => {
                    self.finish_fg_session(&id.to_string(), attachment_id, &reason);
                }
                EventPayload::ShuttingDown { reason } => {
                    self.main_view.update(MainViewMsg::AppendOutput {
                        data: format!("⚠ Daemon shutting down: {reason}"),
                    });
                    self.connected = false;
                    self.status_bar.update(StatusBarMsg::Connected(false));
                }
            },

            AppMsg::KeyEvent(key) => {
                if self.fg_active() && key.kind == KeyEventKind::Press {
                    if key.modifiers.contains(KeyModifiers::CONTROL)
                        && matches!(key.code, KeyCode::Char('z'))
                    {
                        self.detach_fg_session();
                        return;
                    }
                    if key.modifiers.contains(KeyModifiers::CONTROL)
                        && matches!(key.code, KeyCode::Char('y'))
                    {
                        self.copy_focus();
                        return;
                    }
                    if key.modifiers.contains(KeyModifiers::CONTROL)
                        && matches!(key.code, KeyCode::Char(']'))
                    {
                        self.toggle_fg_control();
                        return;
                    }
                    if self.fg_is_controller()
                        && let Some(bytes) =
                            foreground::key_bytes(key, self.fg_application_cursor())
                    {
                        self.send_fg_input(bytes);
                    }
                    return;
                }

                if self.job_picker_open() && key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Esc => {
                            self.close_job_picker();
                        }
                        KeyCode::Up => {
                            self.move_job_picker(-1);
                        }
                        KeyCode::Down => {
                            self.move_job_picker(1);
                        }
                        KeyCode::Enter => {
                            self.kill_selected_job_from_picker();
                        }
                        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            self.close_job_picker();
                        }
                        _ => {}
                    }
                    return;
                }

                if key.kind == KeyEventKind::Press {
                    if is_mode_switch_key(key) {
                        self.update(AppMsg::ModeSwitch);
                        return;
                    }
                    if key.modifiers.contains(KeyModifiers::CONTROL)
                        && matches!(key.code, KeyCode::Char('l'))
                    {
                        self.update(AppMsg::ClearDisplay);
                        return;
                    }
                    if key.modifiers.contains(KeyModifiers::CONTROL)
                        && matches!(key.code, KeyCode::Char('d'))
                    {
                        self.update(AppMsg::Quit);
                        return;
                    }
                    if key.code == KeyCode::Char('c')
                        && key.modifiers.contains(KeyModifiers::CONTROL)
                    {
                        self.open_job_picker();
                        return;
                    }
                    if key.code == KeyCode::Char('y')
                        && key.modifiers.contains(KeyModifiers::CONTROL)
                    {
                        self.update(AppMsg::CopyFocus);
                        return;
                    }
                    if key.code == KeyCode::Char('r')
                        && key.modifiers.contains(KeyModifiers::CONTROL)
                        && self.focus == FocusArea::MainView
                        && self.target_settings_open()
                    {
                        self.reload_target_settings();
                        return;
                    }
                    if key.code == KeyCode::Char('t')
                        && key.modifiers.contains(KeyModifiers::CONTROL)
                    {
                        self.update(AppMsg::OpenTargetSettings);
                        return;
                    }
                    if key.code == KeyCode::Tab
                        && !key.modifiers.contains(KeyModifiers::SHIFT)
                        && self.focus == FocusArea::Input
                    {
                        self.complete_input();
                        return;
                    }
                }

                if self.target_settings_open() && key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Esc => {
                            self.close_target_settings();
                            return;
                        }
                        KeyCode::Up => {
                            self.move_target_selection(-1);
                            return;
                        }
                        KeyCode::Down => {
                            self.move_target_selection(1);
                            return;
                        }
                        KeyCode::Home => {
                            self.move_target_selection_to_edge(false);
                            return;
                        }
                        KeyCode::End => {
                            self.move_target_selection_to_edge(true);
                            return;
                        }
                        KeyCode::Enter => {
                            self.save_selected_target_profile();
                            return;
                        }
                        // Live reconnect: [R] when a pending reconnect profile exists.
                        KeyCode::Char('r') | KeyCode::Char('R')
                            if !key.modifiers.contains(KeyModifiers::CONTROL)
                                && self
                                    .target_settings
                                    .as_ref()
                                    .is_some_and(TargetSettingsState::has_pending_reconnect) =>
                        {
                            self.trigger_reconnect_now();
                            return;
                        }
                        _ => {}
                    }
                }

                if self.focus != FocusArea::Input
                    && self.sidebar_visible()
                    && self.sidebar.selected.is_some()
                    && matches!(key.code, KeyCode::Backspace | KeyCode::Delete)
                {
                    self.update(AppMsg::KillSelection);
                    return;
                }

                let child_msg = match self.focus {
                    FocusArea::Input => self.input.handle_key(key),
                    FocusArea::MainView => self.main_view.handle_key(key),
                    FocusArea::Sidebar => self.sidebar.handle_key(key),
                };
                if let Some(msg) = child_msg {
                    self.update(msg);
                }
            }

            AppMsg::MouseEvent(mouse) => {
                if self.fg_active() {
                    return;
                }
                let regions = self.layout_regions();
                let point = Rect::new(mouse.column, mouse.row, 1, 1);

                if self.job_picker_open() {
                    if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
                        let popup = job_picker_popup_rect(Rect::new(
                            0,
                            0,
                            self.terminal_width,
                            self.terminal_height,
                        ));
                        if !contains(popup, point) {
                            self.close_job_picker();
                            return;
                        }
                        let row = point.y.saturating_sub(job_picker_content_rect(popup).y) as usize;
                        if row < self.job_picker_items().len() {
                            if let Some(picker) = self.job_picker.as_mut() {
                                picker.select(row);
                            }
                            self.kill_selected_job_from_picker();
                        }
                    }
                    return;
                }

                if self.target_settings_open() {
                    if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
                        let popup = self.target_settings_popup_rect();
                        if !contains(popup, point) {
                            self.close_target_settings();
                            return;
                        }
                        if let Some(index) = self.target_settings_profile_hit(point) {
                            self.set_focus(FocusArea::MainView);
                            let already_selected = self
                                .target_settings
                                .as_ref()
                                .is_some_and(|state| state.selected_index() == index);
                            if already_selected {
                                self.save_selected_target_profile();
                            } else {
                                self.select_target_profile(index);
                            }
                        }
                    }
                    return;
                }

                if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
                    if contains(regions.header, point) {
                        if let Some(msg) = self.status_bar.action_at(regions.header, point.x) {
                            self.update(msg);
                        }
                        return;
                    }
                    if contains(regions.input, point) {
                        self.set_focus(FocusArea::Input);
                        self.input
                            .set_cursor_from_point(regions.input, point.x, point.y);
                        return;
                    }
                    if let Some(sidebar) = regions.sidebar
                        && contains(sidebar, point)
                    {
                        self.set_focus(FocusArea::Sidebar);
                        if let Some(list) = regions.sidebar_list
                            && contains(list, point)
                        {
                            let row = point.y.saturating_sub(list.y) as usize;
                            if let Some(index) = self.sidebar.select_visible_row(row) {
                                self.activate_sidebar_row(index);
                            }
                        }
                        return;
                    }
                    if let Some(hit) = self.display_tab_hit(regions.display, point) {
                        self.set_focus(FocusArea::MainView);
                        match hit {
                            DisplayTabHit::Activate(index) => self.activate_display_tab(index),
                            DisplayTabHit::Close(index) => self.close_display_tab(index),
                        }
                        return;
                    }
                    if contains(regions.results_inner, point) {
                        self.set_focus(FocusArea::MainView);
                        if let Some(index) =
                            self.main_view.card_at_point(regions.results_inner, point)
                        {
                            self.inspect_card(index);
                        }
                        return;
                    }
                    if contains(regions.main, point) {
                        self.set_focus(FocusArea::MainView);
                        return;
                    }
                }

                match mouse.kind {
                    MouseEventKind::ScrollUp => {
                        if let Some(sidebar) = regions.sidebar
                            && contains(sidebar, point)
                        {
                            self.set_focus(FocusArea::Sidebar);
                            self.sidebar.move_selection(-1);
                        } else if contains(regions.results, point) {
                            self.set_focus(FocusArea::MainView);
                            if let Some(msg) = self.main_view.handle_mouse(mouse) {
                                self.update(msg);
                            }
                        }
                    }
                    MouseEventKind::ScrollDown => {
                        if let Some(sidebar) = regions.sidebar
                            && contains(sidebar, point)
                        {
                            self.set_focus(FocusArea::Sidebar);
                            self.sidebar.move_selection(1);
                        } else if contains(regions.results, point) {
                            self.set_focus(FocusArea::MainView);
                            if let Some(msg) = self.main_view.handle_mouse(mouse) {
                                self.update(msg);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    /// Propagate overview counts to the sidebar.
    pub(crate) fn set_overview(&mut self, counts: OverviewCounts) {
        self.status_bar
            .update(StatusBarMsg::Overview(counts.clone()));
        self.sidebar.update(SidebarMsg::Overview(counts));
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

fn job_record(job: &JobRow) -> JobRecord<'_> {
    JobRecord {
        id: &job.id,
        status: &job.status,
        start_scope: job.start_scope.as_deref(),
        end_scope: job.end_scope.as_deref(),
        warnings: &job.warnings,
        pending_reason: job.pending_reason.as_deref(),
    }
}

fn job_sidebar_record(job: &JobRow) -> JobSidebarRecord<'_> {
    JobSidebarRecord {
        id: &job.id,
        label: &job.label,
        status: &job.status,
    }
}

fn cron_sidebar_record(cron: &CronRow) -> CronSidebarRecord<'_> {
    CronSidebarRecord {
        id: &cron.id,
        label: &cron.label,
        status: cron.status,
    }
}

fn job_picker_record(job: &JobRow) -> JobPickerRecord<'_> {
    JobPickerRecord {
        id: &job.id,
        label: &job.label,
        status: &job.status,
    }
}

fn foreground_event_matches(
    active_job_id: &str,
    active_attachment_id: u64,
    event_job_id: &str,
    event_attachment_id: u64,
) -> bool {
    if active_attachment_id != event_attachment_id {
        return false;
    }
    event_job_id == active_job_id
        || (active_attachment_id == 0 && event_attachment_id == 0 && event_job_id.is_empty())
}

fn cron_picker_record(cron: &CronRow) -> CronPickerRecord<'_> {
    CronPickerRecord {
        id: &cron.id,
        label: &cron.label,
        status: cron.status,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use cue_core::ipc::{Message, Stream};
    use tokio::io::AsyncReadExt;
    use tokio::time::timeout;

    fn step(execution: u64) -> cue_core::StepId {
        cue_core::StepId {
            execution: cue_core::ExecutionId(execution),
            index: 1,
        }
    }

    fn queue_pending(state: &mut AppState, id: u32, pending: PendingSubmission) {
        state.track_pending_submission(id, pending);
    }

    fn fg_attachment(
        execution: u64,
        role: ForegroundRole,
        control_available: bool,
        snapshot: impl Into<Vec<u8>>,
        snapshot_truncated: bool,
    ) -> OkPayload {
        OkPayload::FgAttached(Box::new(ForegroundAttachmentInfo {
            id: step(execution),
            attachment_id: 1,
            role,
            control_available,
            snapshot: snapshot.into(),
            snapshot_truncated,
        }))
    }

    fn attach_test_writer(state: &mut AppState) -> tokio::io::DuplexStream {
        attach_test_writer_with_capabilities(state, std::iter::empty::<&str>())
    }

    fn attach_shared_foreground_test_writer(state: &mut AppState) -> tokio::io::DuplexStream {
        attach_test_writer_with_capabilities(state, [cue_core::ipc::IPC_CAPABILITY_EXECUTION_V3])
    }

    fn attach_test_writer_with_capabilities<'a>(
        state: &mut AppState,
        capabilities: impl IntoIterator<Item = &'a str>,
    ) -> tokio::io::DuplexStream {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let client =
            crate::client::CuedClient::from_stream_with_capabilities(client_stream, capabilities);
        let (_reader, writer) = client.into_reader_and_writer_handle();
        state.writer = Some(writer);
        state.connected = true;
        server_stream
    }

    async fn read_test_message(stream: &mut tokio::io::DuplexStream) -> Message {
        let mut len = [0_u8; 4];
        stream.read_exact(&mut len).await.unwrap();
        let mut body = vec![0_u8; u32::from_be_bytes(len) as usize];
        stream.read_exact(&mut body).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    fn test_target_profile(
        name: &str,
        transport: &str,
        detail: &str,
        source: TargetProfileSource,
    ) -> crate::target_config::TargetProfileSummary {
        crate::target_config::TargetProfileSummary {
            name: name.into(),
            transport: test_target_profile_kind(transport),
            detail: detail.into(),
            source,
        }
    }

    fn test_target_profile_kind(transport: &str) -> TargetProfileKind {
        match transport {
            "unix" => TargetProfileKind::Unix,
            "ssh" => TargetProfileKind::Ssh,
            "invalid" => TargetProfileKind::Invalid,
            "missing" => TargetProfileKind::Missing,
            other => TargetProfileKind::Unsupported(other.into()),
        }
    }

    fn test_target_snapshot(
        path: impl Into<PathBuf>,
        default_profile: &str,
        profiles: Vec<crate::target_config::TargetProfileSummary>,
    ) -> TargetSettingsSnapshot {
        TargetSettingsSnapshot {
            source_path: path.into(),
            auto_detect_ssh: true,
            default_profile: default_profile.into(),
            profiles,
        }
    }

    #[test]
    fn running_stream_job_sidebar_open_prefers_fg() {
        let mut state = AppState::new();
        state.jobs.push(JobRow {
            id: "J1".into(),
            label: "sleep 5".into(),
            status: JobStatus::Running,
            start_scope: None,
            end_scope: None,
            open_hint: JobOpenHint::Fg,
            warnings: Vec::new(),
            pending_reason: None,
        });

        state.activate_sidebar_row(0);

        let card = state.main_view.cards.last().unwrap();
        assert_eq!(card.input, ":fg J1");
    }

    #[test]
    fn finished_job_sidebar_open_uses_tail() {
        let mut state = AppState::new();
        state.jobs.push(JobRow {
            id: "J1".into(),
            label: "cargo build".into(),
            status: JobStatus::Done,
            start_scope: None,
            end_scope: None,
            open_hint: JobOpenHint::Stream,
            warnings: Vec::new(),
            pending_reason: None,
        });

        state.activate_sidebar_row(0);

        let card = state.main_view.cards.last().unwrap();
        assert_eq!(card.input, ":tail J1");
    }

    #[test]
    fn running_fg_job_sidebar_open_prefers_fg() {
        let mut state = AppState::new();
        state.jobs.push(JobRow {
            id: "J1".into(),
            label: "vim notes.txt".into(),
            status: JobStatus::Running,
            start_scope: None,
            end_scope: None,
            open_hint: JobOpenHint::Fg,
            warnings: Vec::new(),
            pending_reason: None,
        });

        state.activate_sidebar_row(0);

        let card = state.main_view.cards.last().unwrap();
        assert_eq!(card.input, ":fg J1");
    }

    #[test]
    fn scope_created_response_shows_summary_not_only_hash() {
        let mut state = AppState::new();
        let card_index = state.main_view.push_card(":cd /tmp".into(), Mode::Job);
        queue_pending(
            &mut state,
            1,
            PendingSubmission::user(Some(card_index), ":cd /tmp".into(), Mode::Job, Vec::new()),
        );

        state.update(AppMsg::Response {
            id: 1,
            payload: ResponsePayload::Ok(OkPayload::ScopeCreated {
                hash: "S@abc12345".into(),
                summary: "S@abc12345\ncwd: /old -> /tmp".into(),
            }),
        });

        let card = state.main_view.cards.last().unwrap();
        assert_eq!(card.label.as_deref(), Some("S@abc12345"));
        assert_eq!(card.output, "S@abc12345\ncwd: /old -> /tmp");
        assert_eq!(card.status, CardStatus::Success);
    }

    #[test]
    fn typed_step_output_is_buffered_and_appended_to_execution_card() {
        let mut state = AppState::new();
        let step = cue_core::StepId {
            execution: cue_core::ExecutionId(7),
            index: 1,
        };

        state.update(AppMsg::ServerEvent(EventPayload::OutputChunk {
            id: step,
            stream: Stream::Stdout,
            data: b"early\n".to_vec(),
        }));
        assert_eq!(state.execution_output.get("E7").unwrap(), "early\n");

        let card_index = state.main_view.push_card("echo later".into(), Mode::Job);
        state.job_cards.insert("E7".into(), card_index);
        state.update(AppMsg::ServerEvent(EventPayload::OutputChunk {
            id: step,
            stream: Stream::Stdout,
            data: b"later\n".to_vec(),
        }));

        assert_eq!(state.main_view.cards[card_index].output, "later\n");
        assert_eq!(state.execution_output.get("E7").unwrap(), "early\nlater\n");
    }

    #[test]
    fn ctrl_c_opens_running_job_picker() {
        let mut state = AppState::new();
        state.jobs = vec![
            JobRow {
                id: "J1".into(),
                label: "sleep 1".into(),
                status: JobStatus::Done,
                start_scope: None,
                end_scope: None,
                open_hint: JobOpenHint::Stream,
                warnings: Vec::new(),
                pending_reason: None,
            },
            JobRow {
                id: "J2".into(),
                label: "sleep 2".into(),
                status: JobStatus::Running,
                start_scope: None,
                end_scope: None,
                open_hint: JobOpenHint::Stream,
                warnings: Vec::new(),
                pending_reason: None,
            },
        ];

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Char('c'),
            KeyModifiers::CONTROL,
        )));

        assert!(state.job_picker_open());
        assert_eq!(state.job_picker_selected(), Some(0));
        assert_eq!(state.job_picker_items().len(), 1);
    }

    #[test]
    fn ctrl_c_in_cron_mode_opens_remove_picker() {
        let mut state = AppState::new();
        state.mode = Mode::Cron;
        state.sync_mode_views();
        state.crons = vec![CronRow {
            id: "C1".into(),
            label: "every 5m cargo test".into(),
            status: CronStatus::Scheduled,
        }];

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Char('c'),
            KeyModifiers::CONTROL,
        )));

        assert!(state.job_picker_open());
        assert_eq!(state.job_picker_selected(), Some(0));
        assert_eq!(state.job_picker_title(), "Crons");
        assert_eq!(state.job_picker_submit_label(), "remove");
        assert_eq!(state.job_picker_items()[0].id, "C1");
    }

    #[test]
    fn ctrl_d_quits_tui() {
        let mut state = AppState::new();

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Char('d'),
            KeyModifiers::CONTROL,
        )));

        assert!(state.should_quit);
    }

    #[test]
    fn fatal_error_quits_tui_state_machine() {
        let mut state = AppState::new();

        state.update(AppMsg::FatalError {
            message: "terminal event poll failed: tty closed".into(),
        });

        assert!(state.should_quit);
    }

    #[test]
    fn targets_footer_shows_target_controls() {
        let mut state = AppState::new();
        state.session_profile_name = Some("local".into());
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            "/tmp/client.toml",
            "remote",
            vec![
                test_target_profile(
                    "local",
                    "unix",
                    "socket: /tmp/cue.sock",
                    TargetProfileSource::Local,
                ),
                test_target_profile(
                    "remote",
                    "ssh",
                    "devbox | cued gateway --stdio",
                    TargetProfileSource::Configured,
                ),
            ],
        )));
        state.focus = FocusArea::MainView;

        assert!(state.footer_text().contains("Enter save default"));
        assert!(state.footer_text().contains("Ctrl+R reload"));
        assert!(state.target_settings_content().is_some_and(|content| {
            content.contains("current session target: local")
                && content.contains("ssh auto-detection: enabled")
        }));
    }

    #[test]
    fn targets_preview_keys_move_selection() {
        let mut state = AppState::new();
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            "/tmp/client.toml",
            "local",
            vec![
                test_target_profile(
                    "local",
                    "unix",
                    "socket: /tmp/cue.sock",
                    TargetProfileSource::Local,
                ),
                test_target_profile(
                    "remote",
                    "ssh",
                    "devbox | cued gateway --stdio",
                    TargetProfileSource::Configured,
                ),
            ],
        )));
        state.focus = FocusArea::MainView;

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Down,
            KeyModifiers::NONE,
        )));

        assert_eq!(
            state
                .target_settings
                .as_ref()
                .and_then(TargetSettingsState::selected_profile_name),
            Some("remote")
        );
    }

    #[test]
    fn home_and_end_jump_target_selection() {
        let mut state = AppState::new();
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            "/tmp/client.toml",
            "local",
            vec![
                test_target_profile(
                    "local",
                    "unix",
                    "socket: /tmp/cue.sock",
                    TargetProfileSource::Local,
                ),
                test_target_profile(
                    "remote",
                    "ssh",
                    "devbox | cued gateway --stdio",
                    TargetProfileSource::Configured,
                ),
                test_target_profile(
                    "staging",
                    "ssh",
                    "stagebox | cued gateway --stdio",
                    TargetProfileSource::AutoDetectedSsh,
                ),
            ],
        )));
        state.focus = FocusArea::MainView;

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::End,
            KeyModifiers::NONE,
        )));
        assert_eq!(
            state
                .target_settings
                .as_ref()
                .and_then(TargetSettingsState::selected_profile_name),
            Some("staging")
        );

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Home,
            KeyModifiers::NONE,
        )));
        assert_eq!(
            state
                .target_settings
                .as_ref()
                .and_then(TargetSettingsState::selected_profile_name),
            Some("local")
        );
    }

    #[test]
    fn enter_on_targets_preview_saves_default_profile() {
        let unique = format!(
            "cue-targets-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("client.toml");
        std::fs::write(
            &config_path,
            r#"
[transport]
default_profile = "local"

[transport.profiles.local]
transport = "unix"

[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
"#,
        )
        .unwrap();

        let mut state = AppState::new();
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            config_path.clone(),
            "local",
            vec![
                test_target_profile(
                    "local",
                    "unix",
                    "socket: /tmp/cue.sock",
                    TargetProfileSource::Local,
                ),
                test_target_profile(
                    "remote",
                    "ssh",
                    "devbox | cued gateway --stdio",
                    TargetProfileSource::Configured,
                ),
            ],
        )));
        state.focus = FocusArea::MainView;
        state.move_target_selection(1);

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Enter,
            KeyModifiers::NONE,
        )));

        let saved = std::fs::read_to_string(&config_path).unwrap();
        assert!(saved.contains("default_profile = \"remote\""));
        assert_eq!(
            state
                .target_settings
                .as_ref()
                .map(TargetSettingsState::default_profile_name),
            Some("remote")
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn enter_on_existing_default_profile_shows_noop_notice() {
        let mut state = AppState::new();
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            "/tmp/client.toml",
            "remote",
            vec![
                test_target_profile(
                    "local",
                    "unix",
                    "socket: /tmp/cue.sock",
                    TargetProfileSource::Local,
                ),
                test_target_profile(
                    "remote",
                    "ssh",
                    "devbox | cued gateway --stdio",
                    TargetProfileSource::Configured,
                ),
            ],
        )));
        state.focus = FocusArea::MainView;
        state.move_target_selection(1);

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Enter,
            KeyModifiers::NONE,
        )));

        assert!(
            state
                .target_settings
                .as_ref()
                .and_then(TargetSettingsState::notice)
                .is_some_and(|notice| notice.contains("already the default target"))
        );
    }

    #[tokio::test]
    async fn saving_unix_profile_offers_live_reconnect() {
        let unique = format!(
            "cue-targets-unix-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("client.toml");
        std::fs::write(
            &config_path,
            r#"
[transport]
default_profile = "local"

[transport.profiles.local]
transport = "unix"
socket = "/tmp/cue.sock"

[transport.profiles.alt]
transport = "unix"
socket = "/tmp/alt.sock"
"#,
        )
        .unwrap();

        let mut state = AppState::new();
        let connector =
            crate::client::ClientConnector::new(|| async { anyhow::bail!("unused connector") });
        let (_events, controller) =
            crate::client::spawn_connection_manager_controllable(None, connector);
        state.set_connection_controller(controller);
        state.session_profile_name = Some("local".into());
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            config_path.clone(),
            "local",
            vec![
                test_target_profile(
                    "local",
                    "unix",
                    "socket: /tmp/cue.sock",
                    TargetProfileSource::Local,
                ),
                test_target_profile(
                    "alt",
                    "unix",
                    "socket: /tmp/alt.sock",
                    TargetProfileSource::Configured,
                ),
            ],
        )));
        state.focus = FocusArea::MainView;
        state.move_target_selection(1);

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Enter,
            KeyModifiers::NONE,
        )));

        assert!(
            state
                .target_settings
                .as_ref()
                .and_then(TargetSettingsState::notice)
                .is_some_and(|notice| notice.contains("Press R to reconnect now"))
        );
        assert_eq!(
            state
                .target_settings
                .as_ref()
                .and_then(TargetSettingsState::pending_reconnect_profile_name),
            Some("alt")
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn saving_ssh_profile_offers_live_reconnect() {
        let unique = format!(
            "cue-targets-ssh-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("client.toml");
        std::fs::write(
            &config_path,
            r#"
[transport]
default_profile = "local"

[transport.profiles.local]
transport = "unix"

[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
"#,
        )
        .unwrap();

        let mut state = AppState::new();
        let connector =
            crate::client::ClientConnector::new(|| async { anyhow::bail!("unused connector") });
        let (_events, controller) =
            crate::client::spawn_connection_manager_controllable(None, connector);
        state.set_connection_controller(controller);
        state.session_profile_name = Some("local".into());
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            config_path.clone(),
            "local",
            vec![
                test_target_profile(
                    "local",
                    "unix",
                    "socket: /tmp/cue.sock",
                    TargetProfileSource::Local,
                ),
                test_target_profile(
                    "remote",
                    "ssh",
                    "devbox | cued gateway --stdio",
                    TargetProfileSource::Configured,
                ),
            ],
        )));
        state.focus = FocusArea::MainView;
        state.move_target_selection(1);

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Enter,
            KeyModifiers::NONE,
        )));

        assert!(
            state
                .target_settings
                .as_ref()
                .and_then(TargetSettingsState::notice)
                .is_some_and(|notice| notice.contains("Press R to reconnect now"))
        );
        assert_eq!(
            state
                .target_settings
                .as_ref()
                .and_then(TargetSettingsState::pending_reconnect_profile_name),
            Some("remote")
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn reconnect_failure_notice_keeps_pending_profile_for_retry() {
        let mut state = AppState::new();
        state.pending_reconnect_profile_name = Some("alt".into());
        state.connected = true;
        state.status_bar.update(StatusBarMsg::Connected(true));
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            "/tmp/client.toml",
            "alt",
            vec![test_target_profile(
                "alt",
                "unix",
                "socket: /tmp/alt.sock",
                TargetProfileSource::Configured,
            )],
        )));

        state.update(AppMsg::ReconnectFailed {
            message: "dial failed".into(),
        });

        assert!(!state.connected);
        assert_eq!(state.pending_reconnect_profile_name.as_deref(), Some("alt"));
        let notice = state
            .target_settings
            .as_ref()
            .and_then(TargetSettingsState::notice)
            .expect("reconnect failure notice");
        assert!(notice.contains("alt"), "{notice}");
        assert!(notice.contains("dial failed"), "{notice}");
        assert!(notice.contains("retrying"), "{notice}");
    }

    #[test]
    fn target_settings_mouse_click_selects_profile() {
        let mut state = AppState::new();
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            "/tmp/client.toml",
            "local",
            vec![
                test_target_profile(
                    "local",
                    "unix",
                    "socket: /tmp/cue.sock",
                    TargetProfileSource::Local,
                ),
                test_target_profile(
                    "remote",
                    "ssh",
                    "devbox | cued gateway --stdio",
                    TargetProfileSource::Configured,
                ),
            ],
        )));
        state.focus = FocusArea::MainView;

        let content_area = state.target_settings_content_rect().unwrap();
        let view = state.render_target_settings_view().unwrap();
        let remote_line = view.profile_line_rows[1] as u16;

        state.update(AppMsg::MouseEvent(MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: content_area.x.saturating_add(1),
            row: content_area.y.saturating_add(remote_line),
            modifiers: KeyModifiers::NONE,
        }));

        assert_eq!(
            state
                .target_settings
                .as_ref()
                .and_then(TargetSettingsState::selected_profile_name),
            Some("remote")
        );
    }

    #[test]
    fn target_settings_click_outside_modal_closes() {
        let mut state = AppState::new();
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            "/tmp/client.toml",
            "local",
            vec![test_target_profile(
                "local",
                "unix",
                "socket: /tmp/cue.sock",
                TargetProfileSource::Local,
            )],
        )));

        state.update(AppMsg::MouseEvent(MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 0,
            row: 0,
            modifiers: KeyModifiers::NONE,
        }));

        assert!(!state.target_settings_open());
    }

    #[test]
    fn target_settings_does_not_save_missing_profile() {
        let mut state = AppState::new();
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            "/tmp/client.toml",
            "remote",
            vec![test_target_profile(
                "remote",
                "missing",
                "profile is referenced by default_profile but not defined",
                TargetProfileSource::Missing,
            )],
        )));

        assert!(!state.target_settings_can_save());

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Enter,
            KeyModifiers::NONE,
        )));

        assert!(
            state
                .target_settings
                .as_ref()
                .and_then(TargetSettingsState::notice)
                .is_some_and(|notice| notice.contains("not a usable target profile"))
        );
    }

    #[test]
    fn ctrl_t_toggles_targets_page_closed() {
        let mut state = AppState::new();
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            "/tmp/client.toml",
            "local",
            vec![test_target_profile(
                "local",
                "unix",
                "socket: /tmp/cue.sock",
                TargetProfileSource::Local,
            )],
        )));
        state.focus = FocusArea::MainView;

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Char('t'),
            KeyModifiers::CONTROL,
        )));

        assert!(!state.target_settings_open());
        assert!(state.target_settings.is_none());
    }

    #[test]
    fn display_tabs_switch_and_close() {
        let mut state = AppState::new();
        state.open_preview_display(DisplayPreview::new("one", "first", "one\n"));
        state.open_preview_display(DisplayPreview::new("two", "second", "two\n"));

        assert_eq!(state.active_display_tab(), Some(1));
        assert_eq!(state.display_tab_labels().len(), 2);

        state.activate_display_tab(0);
        assert_eq!(state.display_pane_content(), "one\n");

        state.close_display_tab(0);
        assert_eq!(state.display_tab_labels(), vec![" second  × ".to_string()]);
        assert_eq!(state.display_pane_content(), "two\n");
    }

    #[test]
    fn copy_target_prefers_active_display_tab() {
        let mut state = AppState::new();
        state.open_preview_display(DisplayPreview::new("execution:E1", "E1", "hello\n"));
        state.main_view.push_card("sleep 1".into(), Mode::Job);

        assert_eq!(
            state.copy_target(),
            Some(CopyTarget {
                label: "E1".into(),
                content: "hello\n".into(),
            })
        );
    }

    #[test]
    fn cron_sidebar_open_shows_preview_tab() {
        let mut state = AppState::new();
        state.mode = Mode::Cron;
        state.sync_mode_views();
        state.crons.push(CronRow {
            id: "C1".into(),
            label: "every 5m cargo test".into(),
            status: CronStatus::Scheduled,
        });

        state.activate_sidebar_row(0);

        assert_eq!(state.display_tab_labels(), vec![" cron C1  × ".to_string()]);
        assert!(state.display_pane_content().contains("every 5m cargo test"));
    }

    #[test]
    fn sidebar_delete_removes_selected_cron() {
        let mut state = AppState::new();
        state.mode = Mode::Cron;
        state.sync_mode_views();
        state.crons.push(CronRow {
            id: "C1".into(),
            label: "every 5m cargo test".into(),
            status: CronStatus::Scheduled,
        });
        state.sync_sidebar_items();
        state.set_focus(FocusArea::Sidebar);
        state.sidebar.selected = Some(0);

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Backspace,
            KeyModifiers::NONE,
        )));

        let card = state.main_view.cards.last().expect("kill card");
        assert_eq!(card.input, ":kill C1");
        assert_eq!(card.mode, Mode::Cron);
    }

    #[test]
    fn opening_cron_from_sidebar_keeps_sidebar_focus() {
        let mut state = AppState::new();
        state.mode = Mode::Cron;
        state.sync_mode_views();
        state.crons.push(CronRow {
            id: "C1".into(),
            label: "every 5m cargo test".into(),
            status: CronStatus::Scheduled,
        });
        state.set_focus(FocusArea::Sidebar);

        state.activate_sidebar_row(0);

        assert_eq!(state.focus, FocusArea::Sidebar);
        assert_eq!(state.display_tab_labels(), vec![" cron C1  × ".to_string()]);
    }

    #[test]
    fn delete_from_main_view_removes_selected_cron() {
        let mut state = AppState::new();
        state.mode = Mode::Cron;
        state.sync_mode_views();
        state.show_sidebar = Some(true);
        state.crons.push(CronRow {
            id: "C1".into(),
            label: "every 5m cargo test".into(),
            status: CronStatus::Scheduled,
        });
        state.sync_sidebar_items();
        state.sidebar.selected = Some(0);
        state.focus = FocusArea::MainView;

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Delete,
            KeyModifiers::NONE,
        )));

        let card = state.main_view.cards.last().expect("kill card");
        assert_eq!(card.input, ":kill C1");
        assert_eq!(card.mode, Mode::Cron);
    }

    #[test]
    fn card_inspection_opens_preview_tab() {
        let mut state = AppState::new();
        let card_index = state.main_view.push_card("cargo test".into(), Mode::Job);
        state.main_view.set_card_output(card_index, "done".into());

        state.inspect_card(card_index);

        assert_eq!(state.display_tab_labels(), vec![" record  × ".to_string()]);
        assert!(state.display_pane_content().contains("input: cargo test"));
    }

    #[test]
    fn tab_completes_builtin_command() {
        let mut state = AppState::new();
        state.input.insert_text(":ki");

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Tab,
            KeyModifiers::NONE,
        )));

        assert_eq!(state.input.content, ":kill ");
    }

    #[test]
    fn completion_error_records_visible_error_card() {
        let mut state = AppState::new();

        state.show_completion_error(anyhow::anyhow!("current directory was removed"));

        let card = state
            .main_view
            .cards
            .last()
            .expect("completion error should create a visible card");
        assert_eq!(card.input, "completion");
        assert_eq!(card.label.as_deref(), Some("completion"));
        assert_eq!(card.status, CardStatus::Error);
        assert!(card.output.contains("Error [completion]"));
        assert!(card.output.contains("current directory was removed"));
    }

    #[test]
    fn tab_completes_bare_job_path() {
        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(env!("CARGO_MANIFEST_DIR")).unwrap();
        let mut state = AppState::new();
        state.input.insert_text("src/app.r");

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Tab,
            KeyModifiers::NONE,
        )));

        assert_eq!(state.input.content, "src/app.rs ");
        std::env::set_current_dir(original_dir).unwrap();
    }

    #[test]
    fn tab_completes_bare_cron_command_path() {
        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(env!("CARGO_MANIFEST_DIR")).unwrap();
        let mut state = AppState::new();
        state.mode = Mode::Cron;
        state.sync_mode_views();
        state.input.insert_text("daily src/app.r");

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Tab,
            KeyModifiers::NONE,
        )));

        assert_eq!(state.input.content, "daily src/app.rs ");
        std::env::set_current_dir(original_dir).unwrap();
    }

    #[test]
    fn shift_tab_switches_mode_from_sidebar_focus() {
        let mut state = AppState::new();
        state.set_focus(FocusArea::Sidebar);

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::BackTab,
            KeyModifiers::SHIFT,
        )));

        assert_eq!(state.mode, Mode::Cron);
        assert_eq!(state.focus, FocusArea::Sidebar);
    }

    #[test]
    fn shift_tab_switches_mode_from_main_view_focus() {
        let mut state = AppState::new();
        state.set_focus(FocusArea::MainView);

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::BackTab,
            KeyModifiers::SHIFT,
        )));

        assert_eq!(state.mode, Mode::Cron);
        assert_eq!(state.focus, FocusArea::MainView);
    }

    #[test]
    fn step_attach_without_precreated_card_opens_display_and_session() {
        let mut state = AppState::new();
        queue_pending(
            &mut state,
            1,
            PendingSubmission::user(None, ":fg E1/S1".into(), Mode::Job, Vec::new()),
        );

        state.update(AppMsg::Response {
            id: 1,
            payload: ResponsePayload::Ok(fg_attachment(
                1,
                ForegroundRole::Controller,
                false,
                Vec::new(),
                false,
            )),
        });

        assert!(state.fg_active());
        assert_eq!(state.main_view.cards.len(), 1);
        let card = state.main_view.cards.last().unwrap();
        assert_eq!(card.input, ":fg E1/S1");
        assert_eq!(card.label.as_deref(), Some("E1/S1"));
        assert_eq!(card.status, CardStatus::Streaming);
    }

    #[test]
    fn named_session_selector_updates_header_and_foreground_identity() {
        let mut state = AppState::new();
        state.set_named_session_selector(Some("shared".into()));

        assert_eq!(state.status_bar.named_session.as_deref(), Some("shared"));

        state.start_fg_session(
            ForegroundAttachmentInfo {
                id: step(1),
                attachment_id: 1,
                role: ForegroundRole::Observer,
                control_available: false,
                snapshot: Vec::new(),
                snapshot_truncated: false,
            },
            None,
        );
        assert!(state.fg_title().contains("session:shared"));

        state.set_named_session_selector(None);
        assert_eq!(state.status_bar.named_session, None);
        assert!(!state.fg_title().contains("session:"));
    }

    #[test]
    fn fg_watch_starts_from_snapshot_and_filters_output_by_step_id() {
        let mut state = AppState::new();
        state.start_fg_session(
            ForegroundAttachmentInfo {
                id: step(1),
                attachment_id: 11,
                role: ForegroundRole::Observer,
                control_available: true,
                snapshot: b"snapshot\r\n".to_vec(),
                snapshot_truncated: true,
            },
            None,
        );

        assert!(state.fg_screen().unwrap().contents().contains("snapshot"));
        assert!(state.fg_title().contains("WATCH · control available"));
        assert!(state.fg_title().contains("partial history"));
        assert!(state.fg_footer_text().contains("history truncated"));

        state.update(AppMsg::ServerEvent(EventPayload::FgOutput {
            id: step(2),
            attachment_id: 11,
            data: b"foreign\r\n".to_vec(),
        }));
        assert!(!state.fg_screen().unwrap().contents().contains("foreign"));

        state.update(AppMsg::ServerEvent(EventPayload::FgOutput {
            id: step(1),
            attachment_id: 11,
            data: b"live\r\n".to_vec(),
        }));
        let contents = state.fg_screen().unwrap().contents();
        assert!(contents.contains("live"));
    }

    #[test]
    fn delayed_foreground_control_and_exit_events_cannot_mutate_a_new_attachment() {
        let mut state = AppState::new();
        state.start_fg_session(
            ForegroundAttachmentInfo {
                id: step(2),
                attachment_id: 22,
                role: ForegroundRole::Observer,
                control_available: true,
                snapshot: Vec::new(),
                snapshot_truncated: false,
            },
            None,
        );

        state.update(AppMsg::ServerEvent(EventPayload::FgControlChanged {
            id: step(2),
            attachment_id: 21,
            control_available: false,
        }));
        state.update(AppMsg::ServerEvent(EventPayload::FgExited {
            id: step(2),
            attachment_id: 21,
            reason: "done".into(),
        }));

        assert_eq!(state.fg_id(), Some("E2/S1"));
        assert!(state.fg_footer_text().contains("control available"));

        state.update(AppMsg::ServerEvent(EventPayload::FgControlChanged {
            id: step(2),
            attachment_id: 22,
            control_available: false,
        }));
        assert!(state.fg_footer_text().contains("control busy"));
    }

    #[test]
    fn delayed_role_response_for_same_step_cannot_mutate_new_attachment() {
        let mut state = AppState::new();
        state.start_fg_session(
            ForegroundAttachmentInfo {
                id: step(5),
                attachment_id: 52,
                role: ForegroundRole::Observer,
                control_available: true,
                snapshot: Vec::new(),
                snapshot_truncated: false,
            },
            None,
        );

        state.update(AppMsg::Response {
            id: 99,
            payload: ResponsePayload::Ok(OkPayload::FgRoleChanged {
                id: step(5),
                attachment_id: 51,
                role: ForegroundRole::Controller,
                control_available: false,
            }),
        });

        assert!(state.fg_title().contains("WATCH · control available"));
        assert!(!state.fg_title().contains("CONTROL"));
    }

    #[tokio::test]
    async fn watch_command_without_daemon_capability_is_visible_and_not_written() {
        let mut state = AppState::new();
        let mut server_stream = attach_test_writer(&mut state);

        state.update(AppMsg::Submit(":watch E1/S1".into()));

        let card = state.main_view.cards.last().expect("visible watch failure");
        assert!(
            card.output
                .contains(cue_core::ipc::IPC_CAPABILITY_EXECUTION_V3)
        );
        assert!(card.output.contains("upgrade/restart cued"));
        assert!(
            timeout(
                Duration::from_millis(20),
                read_test_message(&mut server_stream)
            )
            .await
            .is_err(),
            "unsupported typed :watch must not reach an old daemon"
        );
    }

    #[tokio::test]
    async fn ctrl_bracket_without_daemon_capability_stays_visible_and_is_not_written() {
        let mut state = AppState::new();
        state.start_fg_session(
            ForegroundAttachmentInfo {
                id: step(6),
                attachment_id: 6,
                role: ForegroundRole::Observer,
                control_available: true,
                snapshot: Vec::new(),
                snapshot_truncated: false,
            },
            None,
        );
        let mut server_stream = attach_test_writer(&mut state);

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Char(']'),
            KeyModifiers::CONTROL,
        )));

        assert!(state.fg_title().contains("WATCH"));
        assert!(
            state
                .fg_footer_text()
                .contains(cue_core::ipc::IPC_CAPABILITY_EXECUTION_V3)
        );
        assert!(state.fg_footer_text().contains("upgrade/restart cued"));
        assert!(state.pending_fg_role_requests.is_empty());
        assert!(
            timeout(
                Duration::from_millis(20),
                read_test_message(&mut server_stream)
            )
            .await
            .is_err(),
            "unsupported Ctrl+] claim must not reach an old daemon"
        );
    }

    #[tokio::test]
    async fn fg_observer_claim_is_confirmed_before_input_and_resize_are_forwarded() {
        let mut state = AppState::new();
        state.start_fg_session(
            ForegroundAttachmentInfo {
                id: step(7),
                attachment_id: 7,
                role: ForegroundRole::Observer,
                control_available: true,
                snapshot: Vec::new(),
                snapshot_truncated: false,
            },
            None,
        );
        let mut server_stream = attach_shared_foreground_test_writer(&mut state);

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Char('x'),
            KeyModifiers::NONE,
        )));
        state.update(AppMsg::Paste("pasted".into()));
        state.update(AppMsg::Resize(100, 30));
        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Char(']'),
            KeyModifiers::CONTROL,
        )));

        let claim_id = match read_test_message(&mut server_stream).await {
            Message::Request {
                id,
                payload: RequestPayload::StepClaimControl {},
                ..
            } => id,
            other => panic!("observer forwarded a request before claim: {other:?}"),
        };
        assert!(state.fg_title().contains("WATCH"));
        assert!(state.fg_footer_text().contains("claiming control"));

        state.update(AppMsg::Response {
            id: claim_id,
            payload: ResponsePayload::Ok(OkPayload::FgRoleChanged {
                id: step(7),
                attachment_id: 7,
                role: ForegroundRole::Controller,
                control_available: false,
            }),
        });
        assert!(state.fg_title().contains("CONTROL"));
        match read_test_message(&mut server_stream).await {
            Message::Request {
                payload: RequestPayload::StepResize { cols, rows },
                ..
            } => {
                assert_eq!(cols, 98);
                assert_eq!(rows, 27);
            }
            other => panic!("controller claim did not synchronize PTY size: {other:?}"),
        }

        state.update(AppMsg::Paste("owned".into()));
        match read_test_message(&mut server_stream).await {
            Message::Request {
                payload: RequestPayload::StepInput { data },
                ..
            } => assert_eq!(data, b"owned"),
            other => panic!("controller paste was not forwarded: {other:?}"),
        }
    }

    #[tokio::test]
    async fn fg_role_failure_stays_observer_and_is_visible() {
        let mut state = AppState::new();
        state.start_fg_session(
            ForegroundAttachmentInfo {
                id: step(3),
                attachment_id: 3,
                role: ForegroundRole::Observer,
                control_available: true,
                snapshot: Vec::new(),
                snapshot_truncated: false,
            },
            None,
        );
        let mut server_stream = attach_shared_foreground_test_writer(&mut state);
        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Char(']'),
            KeyModifiers::CONTROL,
        )));
        let request_id = match read_test_message(&mut server_stream).await {
            Message::Request {
                id,
                payload: RequestPayload::StepClaimControl {},
                ..
            } => id,
            other => panic!("unexpected request: {other:?}"),
        };

        state.update(AppMsg::Response {
            id: request_id,
            payload: ResponsePayload::Err {
                code: "CONFLICT".into(),
                message: "another observer claimed control".into(),
            },
        });

        assert!(state.fg_title().contains("WATCH"));
        assert!(!state.fg_title().contains("CONTROL"));
        assert!(
            state
                .fg_footer_text()
                .contains("claim control failed [CONFLICT]")
        );
        assert!(state.pending_fg_role_requests.is_empty());
    }

    #[tokio::test]
    async fn fg_controller_ctrl_bracket_releases_without_detaching() {
        let mut state = AppState::new();
        state.start_fg_session(
            ForegroundAttachmentInfo {
                id: step(4),
                attachment_id: 4,
                role: ForegroundRole::Controller,
                control_available: false,
                snapshot: Vec::new(),
                snapshot_truncated: false,
            },
            None,
        );
        let mut server_stream = attach_shared_foreground_test_writer(&mut state);
        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Char(']'),
            KeyModifiers::CONTROL,
        )));
        let request_id = match read_test_message(&mut server_stream).await {
            Message::Request {
                id,
                payload: RequestPayload::StepReleaseControl {},
                ..
            } => id,
            other => panic!("unexpected request: {other:?}"),
        };
        assert!(state.fg_active());
        assert!(state.fg_title().contains("CONTROL"));

        state.update(AppMsg::Response {
            id: request_id,
            payload: ResponsePayload::Ok(OkPayload::FgRoleChanged {
                id: step(4),
                attachment_id: 4,
                role: ForegroundRole::Observer,
                control_available: true,
            }),
        });

        assert!(state.fg_active());
        assert!(state.fg_title().contains("WATCH · control available"));
    }

    #[test]
    fn switching_modes_filters_card_history() {
        let mut state = AppState::new();
        state.main_view.push_card("cargo test".into(), Mode::Job);
        state
            .main_view
            .push_card("every 5m cargo test".into(), Mode::Cron);

        assert_eq!(state.main_view.mode, Mode::Job);
        assert_eq!(
            state
                .main_view
                .cards
                .iter()
                .filter(|card| card.mode == state.main_view.mode)
                .count(),
            1
        );

        state.update(AppMsg::ModeSwitch);
        assert_eq!(state.main_view.mode, Mode::Cron);
        assert_eq!(
            state
                .main_view
                .cards
                .iter()
                .filter(|card| card.mode == state.main_view.mode)
                .count(),
            1
        );
    }

    #[test]
    fn offline_submit_does_not_stay_waiting() {
        let mut state = AppState::new();
        state.update(AppMsg::Submit("ls".into()));

        let card = state.main_view.cards.last().unwrap();
        assert_eq!(card.status, CardStatus::Error);
        assert!(card.output.contains("offline"));
    }

    #[test]
    fn restart_local_command_uses_restart_handle() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let mut state = AppState::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let shared = Arc::clone(&calls);
        state.set_restart_handle(Some(RestartHandle::new(move || {
            shared.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })));

        state.update(AppMsg::Submit(":restart".into()));

        let card = state.main_view.cards.last().expect("restart card");
        assert_eq!(card.input, ":restart");
        assert_eq!(card.status, CardStatus::Pending);
        assert!(card.output.contains("waiting for reconnect"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn missing_operator_spacing_warns_on_submit() {
        let mut state = AppState::new();
        state.update(AppMsg::Submit("sleep 4->ls".into()));

        let card = state.main_view.cards.last().unwrap();
        assert!(card.output.contains("missing spaces around `->`"));
        assert!(card.output.contains("sleep 4 -> ls"));
    }

    #[test]
    fn clear_display_clears_all_logs_when_idle() {
        let mut state = AppState::new();
        state.main_view.push_card("job".into(), Mode::Job);
        state.main_view.push_card("cron".into(), Mode::Cron);

        state.update(AppMsg::ClearDisplay);

        assert!(state.main_view.cards.is_empty());
    }

    #[test]
    fn clear_display_is_blocked_while_submission_is_pending() {
        let mut state = AppState::new();
        let card_index = state.main_view.push_card("sleep 4".into(), Mode::Job);
        queue_pending(
            &mut state,
            1,
            PendingSubmission::user(Some(card_index), "sleep 4".into(), Mode::Job, Vec::new()),
        );

        state.update(AppMsg::ClearDisplay);

        assert_eq!(
            state
                .main_view
                .cards
                .iter()
                .filter(|card| card.mode == Mode::Job)
                .count(),
            1
        );
    }

    #[test]
    fn fg_output_updates_terminal_modes_and_preserves_formatted_contents() {
        let mut state = AppState::new();
        queue_pending(
            &mut state,
            1,
            PendingSubmission::user(None, ":fg E1/S1".into(), Mode::Job, Vec::new()),
        );
        state.update(AppMsg::Response {
            id: 1,
            payload: ResponsePayload::Ok(fg_attachment(
                1,
                ForegroundRole::Controller,
                false,
                Vec::new(),
                false,
            )),
        });
        state.update(AppMsg::ServerEvent(EventPayload::FgOutput {
            id: step(1),
            attachment_id: 1,
            data: b"\x1b[?1049h\x1b[?1h\x1b[?2004h\x1b[31mhello\x1b[0m".to_vec(),
        }));

        let screen = state.fg_screen().unwrap();
        assert!(screen.alternate_screen());
        assert!(screen.application_cursor());
        assert!(screen.bracketed_paste());
        assert!(screen.contents().contains("hello"));

        state.update(AppMsg::ServerEvent(EventPayload::FgExited {
            id: step(1),
            attachment_id: 1,
            reason: "detached".into(),
        }));

        let card = state.main_view.cards.last().unwrap();
        assert_eq!(card.status, CardStatus::Success);
        assert!(card.output.contains("hello"));
        assert!(card.output.contains("\u{1b}[31m"));
    }
}
