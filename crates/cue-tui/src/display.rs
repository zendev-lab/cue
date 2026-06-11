use std::collections::BTreeSet;

use cue_core::ipc::Stream;
use cue_core::{EventChannel, JobId};
use ratatui::layout::Rect;

const EMPTY_DISPLAY_MESSAGE: &str = "Use `:out J1` for a stdout snapshot, `:tail J1` to follow live stdout, or `:err J1` for stderr.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DisplayStream {
    Stdout,
    Stderr,
}

impl DisplayStream {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }

    fn matches_ipc(self, stream: Stream) -> bool {
        matches!(
            (self, stream),
            (Self::Stdout, Stream::Stdout) | (Self::Stderr, Stream::Stderr)
        )
    }
}

pub(crate) fn display_stream_from_ipc(stream: Stream) -> DisplayStream {
    match stream {
        Stream::Stdout => DisplayStream::Stdout,
        Stream::Stderr => DisplayStream::Stderr,
    }
}

pub(crate) fn output_channel_for_job_id(id: &str) -> Option<EventChannel> {
    match id.parse::<JobId>() {
        Ok(job_id) => Some(EventChannel::Output(job_id)),
        Err(error) => {
            tracing::warn!(job_id = %id, "invalid output subscription target: {error}");
            None
        }
    }
}

#[derive(Debug, Clone)]
struct DisplayTab {
    target: DisplayTarget,
    content: String,
    follow: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DisplayTarget {
    Output { id: String, stream: DisplayStream },
    Preview { key: String, title: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DisplayCopyTarget {
    pub(crate) label: String,
    pub(crate) content: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DisplayTabHit {
    Activate(usize),
    Close(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DisplaySubscriptionPlan {
    pub(crate) subscribe: Vec<String>,
    pub(crate) unsubscribe: Vec<String>,
}

pub(crate) fn plan_display_subscriptions(
    desired: BTreeSet<String>,
    current: BTreeSet<String>,
    pending_subscribes: &BTreeSet<String>,
    pending_unsubscribes: &BTreeSet<String>,
) -> DisplaySubscriptionPlan {
    let subscribe = desired
        .difference(&current)
        .filter(|id| !pending_subscribes.contains(*id))
        .cloned()
        .collect();
    let unsubscribe = current
        .difference(&desired)
        .filter(|id| !pending_unsubscribes.contains(*id))
        .cloned()
        .collect();
    DisplaySubscriptionPlan {
        subscribe,
        unsubscribe,
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct DisplayPane {
    tabs: Vec<DisplayTab>,
    active: Option<usize>,
}

impl DisplayPane {
    pub(crate) fn content(&self) -> &str {
        self.active_tab()
            .map(|tab| tab.content.as_str())
            .unwrap_or(EMPTY_DISPLAY_MESSAGE)
    }

    pub(crate) fn has_target(&self) -> bool {
        self.active.is_some()
    }

    pub(crate) fn labels(&self) -> Vec<String> {
        self.tabs
            .iter()
            .map(|tab| match &tab.target {
                DisplayTarget::Output { id, stream } => {
                    let prefix = if tab.follow { " follow" } else { "" };
                    format!("{prefix} {} {}  × ", stream.label(), id)
                }
                DisplayTarget::Preview { title, .. } => format!(" {title}  × "),
            })
            .collect()
    }

    pub(crate) fn active(&self) -> Option<usize> {
        self.active
    }

    pub(crate) fn copy_target(&self) -> Option<DisplayCopyTarget> {
        let tab = self.active_tab()?;
        let label = match &tab.target {
            DisplayTarget::Output { id, stream } => format!("{} {id}", stream.label()),
            DisplayTarget::Preview { title, .. } => title.clone(),
        };
        Some(DisplayCopyTarget {
            label,
            content: tab.content.clone(),
        })
    }

    pub(crate) fn desired_subscriptions(&self) -> BTreeSet<String> {
        self.tabs
            .iter()
            .filter_map(|tab| match (&tab.target, tab.follow) {
                (DisplayTarget::Output { id, .. }, true) => Some(id.clone()),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn disable_follow(&mut self, id: &str) -> bool {
        let mut changed = false;
        for tab in &mut self.tabs {
            if tab.follow
                && matches!(
                    &tab.target,
                    DisplayTarget::Output {
                        id: existing_id,
                        ..
                    } if existing_id == id
                )
            {
                tab.follow = false;
                changed = true;
            }
        }
        changed
    }

    pub(crate) fn open_preview(&mut self, key: String, title: String, content: String) {
        let target = DisplayTarget::Preview { key, title };
        if let Some(index) = self.tabs.iter().position(|tab| tab.target == target) {
            self.tabs[index].content = content;
            self.active = Some(index);
            return;
        }

        self.tabs.push(DisplayTab {
            target,
            content,
            follow: false,
        });
        self.active = Some(self.tabs.len() - 1);
    }

    pub(crate) fn show_output(
        &mut self,
        id: String,
        stream: DisplayStream,
        data: String,
        truncated: bool,
        follow: bool,
    ) {
        let content = if truncated {
            format!("{data}\n--- (truncated) ---")
        } else {
            data
        };
        if let Some(index) = self.tabs.iter().position(|tab| {
            matches!(
                &tab.target,
                DisplayTarget::Output {
                    id: existing_id,
                    stream: existing_stream,
                } if *existing_id == id && *existing_stream == stream
            )
        }) {
            self.tabs[index].content = content;
            self.tabs[index].follow = follow;
            self.active = Some(index);
        } else {
            self.tabs.push(DisplayTab {
                target: DisplayTarget::Output { id, stream },
                content,
                follow,
            });
            self.active = Some(self.tabs.len() - 1);
        }
    }

    pub(crate) fn append_output(&mut self, id: &str, stream: Stream, data: &str) {
        for tab in &mut self.tabs {
            let DisplayTarget::Output {
                id: existing_id,
                stream: display_stream,
            } = &tab.target
            else {
                continue;
            };
            if tab.follow && existing_id == id && display_stream.matches_ipc(stream) {
                tab.content.push_str(data);
            }
        }
    }

    pub(crate) fn clear(&mut self) {
        self.tabs.clear();
        self.active = None;
    }

    pub(crate) fn activate(&mut self, index: usize) {
        if index < self.tabs.len() {
            self.active = Some(index);
        }
    }

    pub(crate) fn close(&mut self, index: usize) -> bool {
        if self.tabs.get(index).is_none() {
            return false;
        }
        self.tabs.remove(index);
        self.active = match self.tabs.is_empty() {
            true => None,
            false if index >= self.tabs.len() => Some(self.tabs.len() - 1),
            false => Some(index),
        };
        true
    }

    pub(crate) fn hit(&self, display_area: Rect, point: Rect) -> Option<DisplayTabHit> {
        let tab_bar = self.tab_bar_rect(display_area)?;
        if !contains(tab_bar, point) {
            return None;
        }

        let mut x = tab_bar.x;
        for (index, label) in self.labels().into_iter().enumerate() {
            let width = label.chars().count() as u16;
            let start = x;
            let close_x = start + width.saturating_sub(3);
            let end = start + width;
            if point.x >= start && point.x < end {
                return if point.x >= close_x {
                    Some(DisplayTabHit::Close(index))
                } else {
                    Some(DisplayTabHit::Activate(index))
                };
            }
            x = end;
        }
        None
    }

    fn active_tab(&self) -> Option<&DisplayTab> {
        self.active.and_then(|index| self.tabs.get(index))
    }

    fn tab_bar_rect(&self, display_area: Rect) -> Option<Rect> {
        if self.tabs.is_empty() || display_area.width <= 2 || display_area.height <= 2 {
            return None;
        }
        Some(Rect::new(
            display_area.x + 1,
            display_area.y + 1,
            display_area.width.saturating_sub(2),
            1,
        ))
    }
}

fn contains(area: Rect, point: Rect) -> bool {
    point.x >= area.x
        && point.x < area.x + area.width
        && point.y >= area.y
        && point.y < area.y + area.height
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_stream_maps_ipc_streams() {
        assert_eq!(
            display_stream_from_ipc(Stream::Stdout),
            DisplayStream::Stdout
        );
        assert_eq!(
            display_stream_from_ipc(Stream::Stderr),
            DisplayStream::Stderr
        );
    }

    #[test]
    fn output_subscription_channel_requires_job_id() {
        assert_eq!(
            output_channel_for_job_id("J7"),
            Some(EventChannel::Output(JobId(7)))
        );
        assert!(output_channel_for_job_id("C7").is_none());
    }

    #[test]
    fn subscription_plan_skips_pending_requests() {
        let plan = plan_display_subscriptions(
            BTreeSet::from(["J1".to_string(), "J2".to_string()]),
            BTreeSet::from(["J2".to_string(), "J3".to_string()]),
            &BTreeSet::from(["J1".to_string()]),
            &BTreeSet::from(["J3".to_string()]),
        );

        assert_eq!(
            plan,
            DisplaySubscriptionPlan {
                subscribe: Vec::new(),
                unsubscribe: Vec::new(),
            }
        );
    }

    #[test]
    fn subscription_plan_emits_missing_subscribe_and_stale_unsubscribe() {
        let plan = plan_display_subscriptions(
            BTreeSet::from(["J1".to_string()]),
            BTreeSet::from(["J2".to_string()]),
            &BTreeSet::new(),
            &BTreeSet::new(),
        );

        assert_eq!(
            plan,
            DisplaySubscriptionPlan {
                subscribe: vec!["J1".to_string()],
                unsubscribe: vec!["J2".to_string()],
            }
        );
    }

    #[test]
    fn output_tabs_label_following_state() {
        let mut pane = DisplayPane::default();
        pane.show_output(
            "J1".into(),
            DisplayStream::Stdout,
            "one".into(),
            false,
            true,
        );

        assert_eq!(pane.labels(), vec![" follow stdout J1  × ".to_string()]);
        assert_eq!(
            pane.desired_subscriptions(),
            BTreeSet::from(["J1".to_string()])
        );
    }

    #[test]
    fn disable_follow_updates_subscription_intent() {
        let mut pane = DisplayPane::default();
        pane.show_output(
            "J1".into(),
            DisplayStream::Stdout,
            "one".into(),
            false,
            true,
        );

        assert!(pane.disable_follow("J1"));
        assert!(pane.desired_subscriptions().is_empty());
        assert_eq!(pane.labels(), vec![" stdout J1  × ".to_string()]);
    }

    #[test]
    fn preview_tab_is_reused_by_key_and_title() {
        let mut pane = DisplayPane::default();
        pane.open_preview("card:1".into(), "record".into(), "old".into());
        pane.open_preview("card:1".into(), "record".into(), "new".into());

        assert_eq!(pane.labels(), vec![" record  × ".to_string()]);
        assert_eq!(pane.content(), "new");
    }

    #[test]
    fn appends_only_matching_follow_output() {
        let mut pane = DisplayPane::default();
        pane.show_output(
            "J1".into(),
            DisplayStream::Stdout,
            "one\n".into(),
            false,
            true,
        );
        pane.show_output(
            "J2".into(),
            DisplayStream::Stdout,
            "two\n".into(),
            false,
            false,
        );

        pane.append_output("J1", Stream::Stdout, "followed\n");
        pane.append_output("J1", Stream::Stderr, "ignored\n");

        pane.activate(0);
        assert_eq!(pane.content(), "one\nfollowed\n");
        pane.activate(1);
        assert_eq!(pane.content(), "two\n");
    }

    #[test]
    fn closing_active_tab_selects_next_available_tab() {
        let mut pane = DisplayPane::default();
        pane.show_output(
            "J1".into(),
            DisplayStream::Stdout,
            "one".into(),
            false,
            false,
        );
        pane.show_output(
            "J2".into(),
            DisplayStream::Stdout,
            "two".into(),
            false,
            false,
        );
        pane.activate(0);

        assert!(pane.close(0));

        assert_eq!(pane.active(), Some(0));
        assert_eq!(pane.content(), "two");
    }

    #[test]
    fn tab_hit_distinguishes_activation_and_close_region() {
        let mut pane = DisplayPane::default();
        pane.show_output(
            "J1".into(),
            DisplayStream::Stdout,
            "one".into(),
            false,
            false,
        );
        let area = Rect::new(0, 0, 30, 5);

        assert_eq!(
            pane.hit(area, Rect::new(2, 1, 1, 1)),
            Some(DisplayTabHit::Activate(0))
        );
        assert_eq!(
            pane.hit(area, Rect::new(13, 1, 1, 1)),
            Some(DisplayTabHit::Close(0))
        );
    }
}
