use std::io::{self, Write};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};

pub(crate) fn copy_to_clipboard(text: &str) -> io::Result<()> {
    let mut stdout = io::stdout();
    write_osc52_sequence(&mut stdout, text)
}

fn write_osc52_sequence(writer: &mut impl Write, text: &str) -> io::Result<()> {
    writer.write_all(osc52_sequence(text).as_bytes())?;
    writer.flush()
}

fn osc52_sequence(text: &str) -> String {
    format!("\x1b]52;c;{}\x07", BASE64_STANDARD.encode(text.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn osc52_sequence_encodes_text_as_base64_clipboard_payload() {
        assert_eq!(osc52_sequence("hello"), "\x1b]52;c;aGVsbG8=\x07");
    }

    #[test]
    fn write_osc52_sequence_writes_and_flushes_payload() {
        let mut output = Vec::new();
        write_osc52_sequence(&mut output, "copy me").expect("write OSC52 sequence");

        assert_eq!(
            String::from_utf8(output).unwrap(),
            "\x1b]52;c;Y29weSBtZQ==\x07"
        );
    }
}
