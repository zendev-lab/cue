fn examples() {
    // ruleid: daemon-private-files
    let _ = Permissions::from_mode(0o777);
    // ruleid: daemon-private-files
    let _ = std::fs::Permissions::from_mode(0o644);
    // ruleid: daemon-private-files
    options.mode(0o755);
    // ok: daemon-private-files
    let _ = Permissions::from_mode(0o600);
    // ok: daemon-private-files
    options.mode(0o700);
}
