#![no_main]

use cue_core::ScopeHash;
use cue_language::{Mode, SurfaceCommand, compile_command};
use cue_protocol::{decode_message, encode_message};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(message) = decode_message(data) {
        let encoded = encode_message(&message).expect("accepted message must encode");
        assert_eq!(decode_message(&encoded).unwrap(), message);
    }
    if let Ok(source) = std::str::from_utf8(data) {
        if let Ok(SurfaceCommand::Submit(spec)) =
            compile_command(source, Mode::Job, ScopeHash([0; 32]))
        {
            assert!(spec.plan().validate().is_ok());
        }
    }
});
