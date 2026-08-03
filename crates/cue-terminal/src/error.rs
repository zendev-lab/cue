/// Errors returned by the foreground terminal model.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The requested terminal dimensions contain a zero column or row.
    #[error("terminal dimensions must be non-zero (cols={cols}, rows={rows})")]
    InvalidSize { cols: u16, rows: u16 },

    /// A mouse coordinate could not be represented in Ghostty's pixel space.
    #[error("mouse coordinate exceeds the supported pixel range")]
    MouseCoordinateOverflow,

    /// An encoded input would exceed the addressable buffer size.
    #[error("terminal input is too large to encode")]
    InputTooLarge,

    /// Ghostty could not be initialized with cue-shell's silent logger policy.
    #[error("failed to initialize Ghostty: {0}")]
    Initialization(String),

    /// A single terminal update generated an unreasonable amount of PTY input.
    #[error("terminal reply batch exceeded the {limit}-byte safety limit")]
    ReplyOverflow { limit: usize },

    /// Ghostty formatted terminal content that was not valid UTF-8.
    #[error("Ghostty returned invalid UTF-8 while formatting terminal content")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),

    /// The underlying Ghostty binding rejected an operation.
    #[error(transparent)]
    Ghostty(#[from] libghostty_vt::Error),
}

/// Convenient result alias for this crate.
pub type Result<T> = std::result::Result<T, Error>;
