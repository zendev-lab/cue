use cue_core::ipc::JobOpenHint;
use cue_core::job::JobStatus;

use crate::component::main_view::Card;
use crate::display::DisplayPreview;
use crate::record_format;

pub(crate) struct CardJob<'a> {
    pub(crate) id: &'a str,
    pub(crate) status: &'a JobStatus,
    pub(crate) open_hint: JobOpenHint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CardAction {
    Foreground { job_id: String },
    Tail { job_id: String },
    Preview(DisplayPreview),
}

pub(crate) fn inspect_card_action(
    index: usize,
    card: &Card,
    job: Option<CardJob<'_>>,
) -> CardAction {
    if let Some(job) = job {
        if matches!(job.status, JobStatus::Running) && job.open_hint == JobOpenHint::Fg {
            return CardAction::Foreground {
                job_id: job.id.to_string(),
            };
        }

        if matches!(job.status, JobStatus::Running) || job.status.is_terminal() {
            return CardAction::Tail {
                job_id: job.id.to_string(),
            };
        }
    }

    CardAction::Preview(DisplayPreview::new(
        format!("card:{index}"),
        card.label
            .clone()
            .map(|label| format!("record {label}"))
            .unwrap_or_else(|| "record".to_string()),
        record_format::format_card_preview(card),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cue_core::Mode;
    use cue_core::job::CancelReason;

    use crate::component::main_view::{Card, CardStatus};

    fn card(input: &str) -> Card {
        let mut card = Card::new(input.to_string(), Mode::Job);
        card.status = CardStatus::Success;
        card.output = "done".into();
        card
    }

    #[test]
    fn inspect_card_action_foregrounds_only_foreground_capable_running_jobs() {
        let card = card("vim notes.md");

        assert_eq!(
            inspect_card_action(
                3,
                &card,
                Some(CardJob {
                    id: "J7",
                    status: &JobStatus::Running,
                    open_hint: JobOpenHint::Fg,
                }),
            ),
            CardAction::Foreground {
                job_id: "J7".into(),
            },
        );
        assert_eq!(
            inspect_card_action(
                3,
                &card,
                Some(CardJob {
                    id: "J8",
                    status: &JobStatus::Running,
                    open_hint: JobOpenHint::Stream,
                }),
            ),
            CardAction::Tail {
                job_id: "J8".into(),
            },
        );
    }

    #[test]
    fn inspect_card_action_tails_terminal_jobs() {
        let card = card("cargo test");

        for status in [
            JobStatus::Done,
            JobStatus::Failed,
            JobStatus::Killed,
            JobStatus::Cancelled(CancelReason::User),
        ] {
            assert_eq!(
                inspect_card_action(
                    3,
                    &card,
                    Some(CardJob {
                        id: "J9",
                        status: &status,
                        open_hint: JobOpenHint::Stream,
                    }),
                ),
                CardAction::Tail {
                    job_id: "J9".into(),
                },
            );
        }
    }

    #[test]
    fn inspect_card_action_previews_non_job_or_non_terminal_cards() {
        let mut card = card("cargo check");
        card.label = Some("build".into());

        assert_eq!(
            inspect_card_action(
                4,
                &card,
                Some(CardJob {
                    id: "J4",
                    status: &JobStatus::Pending,
                    open_hint: JobOpenHint::Fg,
                }),
            ),
            CardAction::Preview(DisplayPreview::new(
                "card:4",
                "record build",
                "mode: JOB\ninput: cargo check\nstatus: success\nlabel: build\n\ndone",
            )),
        );

        assert!(matches!(
            inspect_card_action(2, &card, None),
            CardAction::Preview(_)
        ));
    }
}
