//! Client-side language services for Cue's shell-like DSL.
//!
//! This crate owns text parsing and frontend assistance. The daemon accepts
//! typed contracts and does not define language syntax.

mod assistance;
mod ast;
pub mod command_spec;
mod compiler;
mod completion;
mod duration;
mod mode;
mod parse;
mod resolver;
mod token;
mod tokenizer;
mod vnext_compiler;

pub use assistance::{
    CompletionItem, CompletionKind, HighlightKind, HighlightSpan, complete_input, highlight_input,
};
pub use compiler::{
    CompileError, CompiledCommand, FrontendAction, compile_command, compile_file, render_help,
};
pub use completion::{CompletionScope, completion_candidates, completion_replacement};
pub use mode::Mode;
pub use parse::ParseError;
pub use parse::ParseErrorKind;
pub use token::Token;
pub use tokenizer::Tokenizer;
pub use vnext_compiler::{
    OutputSelection, OutputTarget, VnextCommand, VnextCompileError, VnextFrontendAction,
    compile_vnext_command, compile_vnext_file,
};

pub(crate) fn parse_command(
    input: &str,
    mode: Mode,
) -> Result<resolver::ResolvedCommand, ParseError> {
    let ast = parse::Parser::parse(input)?;
    resolver::Resolver::resolve(ast, mode)
}

pub(crate) fn parse_file_script_command(
    input: &str,
) -> Result<resolver::ResolvedCommand, ParseError> {
    let ast = parse::Parser::parse_file_script(input)?;
    resolver::Resolver::resolve(ast, Mode::Job)
}
