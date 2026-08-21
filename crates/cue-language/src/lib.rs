//! Client-side language services for Cue's shell-like DSL.
//!
//! This crate owns text parsing and frontend assistance. The daemon accepts
//! typed contracts and does not define language syntax.

mod assistance;
mod ast;
pub mod command_spec;
mod completion;
mod duration;
mod parse;
mod resolver;
mod token;
mod tokenizer;

use cue_core::mode::Mode;

pub use assistance::{complete_input, highlight_input};
pub use completion::{CompletionScope, completion_candidates, completion_replacement};
pub use parse::ParseError;
pub use parse::ParseErrorKind;
pub use resolver::{ResolvedCommand, ResolvedScriptItem};
pub use token::Token;
pub use tokenizer::Tokenizer;

pub fn parse_command(input: &str, mode: Mode) -> Result<ResolvedCommand, ParseError> {
    let ast = parse::Parser::parse(input)?;
    resolver::Resolver::resolve(ast, mode)
}

pub fn parse_file_script_command(input: &str) -> Result<ResolvedCommand, ParseError> {
    let ast = parse::Parser::parse_file_script(input)?;
    resolver::Resolver::resolve(ast, Mode::Job)
}
