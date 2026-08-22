pub(crate) use cue_language::{CompletionScope, completion_candidates, completion_replacement};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompletionErrorRecord {
    pub(crate) input: String,
    pub(crate) label: String,
    pub(crate) output: String,
}

pub(crate) fn completion_error_record(error: &anyhow::Error) -> CompletionErrorRecord {
    CompletionErrorRecord {
        input: "completion".into(),
        label: "completion".into(),
        output: format!("Error [completion]: {error:#}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_error_record_formats_visible_error_card() {
        let error = anyhow::anyhow!("current directory was removed");

        assert_eq!(
            completion_error_record(&error),
            CompletionErrorRecord {
                input: "completion".into(),
                label: "completion".into(),
                output: "Error [completion]: current directory was removed".into(),
            },
        );
    }
}
