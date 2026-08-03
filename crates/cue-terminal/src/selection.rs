use libghostty_vt::{
    Terminal,
    fmt::{Format, Formatter, FormatterOptions},
    screen::TrackedGridRef,
    selection::{FormatOptions, Selection},
    terminal::{Point, PointCoordinate},
};

use crate::Result;

/// Viewport-relative endpoints of the active terminal selection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SelectionRange {
    pub start_row: u16,
    pub start_col: u16,
    pub end_row: u16,
    pub end_col: u16,
    pub rectangular: bool,
}

#[derive(Debug, Default)]
pub(crate) struct SelectionState {
    range: Option<SelectionRange>,
    start: Option<TrackedGridRef>,
    end: Option<TrackedGridRef>,
}

impl SelectionState {
    pub fn begin(&mut self, terminal: &Terminal<'_, '_>, row: u16, col: u16) -> Result<()> {
        self.clear(terminal)?;
        self.start = Some(terminal.track_grid_ref(viewport_point(row, col))?);
        Ok(())
    }

    pub fn update(
        &mut self,
        terminal: &Terminal<'_, '_>,
        row: u16,
        col: u16,
        rectangular: bool,
    ) -> Result<()> {
        let Some(start) = self.start.as_ref() else {
            return Ok(());
        };
        let Some(start_point) = start.point(libghostty_vt::terminal::PointSpace::Viewport)? else {
            self.clear(terminal)?;
            return Ok(());
        };

        self.end = Some(terminal.track_grid_ref(viewport_point(row, col))?);
        self.range = Some(SelectionRange {
            start_row: u16::try_from(start_point.y).unwrap_or(u16::MAX),
            start_col: start_point.x,
            end_row: row,
            end_col: col,
            rectangular,
        });
        self.refresh(terminal)
    }

    pub fn clear(&mut self, terminal: &Terminal<'_, '_>) -> Result<()> {
        terminal.set_selection(None)?;
        self.clear_logical();
        Ok(())
    }

    pub fn detach_before_mutation(&self, terminal: &Terminal<'_, '_>) -> Result<()> {
        if self.start.is_some() {
            terminal.set_selection(None)?;
        }
        Ok(())
    }

    pub fn refresh(&mut self, terminal: &Terminal<'_, '_>) -> Result<()> {
        let (Some(start), Some(end), Some(range)) =
            (self.start.as_ref(), self.end.as_ref(), self.range)
        else {
            terminal.set_selection(None)?;
            return Ok(());
        };

        let (Some(start), Some(end)) = (start.snapshot(terminal)?, end.snapshot(terminal)?) else {
            terminal.set_selection(None)?;
            self.clear_logical();
            return Ok(());
        };

        terminal.set_selection(Some(&Selection::new(start, end, range.rectangular)))?;
        Ok(())
    }

    pub fn has_selection(&self) -> bool {
        self.range.is_some()
            && self.start.as_ref().is_some_and(TrackedGridRef::has_value)
            && self.end.as_ref().is_some_and(TrackedGridRef::has_value)
    }

    pub fn range(&self) -> Option<SelectionRange> {
        self.has_selection().then_some(self.range).flatten()
    }

    pub fn copy_selection(&mut self, terminal: &Terminal<'_, '_>) -> Result<Option<String>> {
        self.refresh(terminal)?;
        let bytes = terminal.format_selection_alloc(
            None,
            FormatOptions::new()
                .with_emit_format(Format::Plain)
                .with_trim(true)
                .with_unwrap(true),
        )?;
        bytes
            .map(|bytes| String::from_utf8(bytes.as_ref().to_vec()))
            .transpose()
            .map_err(Into::into)
    }

    fn clear_logical(&mut self) {
        self.range = None;
        self.start = None;
        self.end = None;
    }
}

pub(crate) fn copy_screen(terminal: &Terminal<'_, '_>) -> Result<String> {
    let bytes = format_screen(terminal, Format::Plain)?;
    Ok(String::from_utf8(bytes)?)
}

pub(crate) fn format_screen(terminal: &Terminal<'_, '_>, format: Format) -> Result<Vec<u8>> {
    let mut formatter = Formatter::new(
        terminal,
        FormatterOptions::new()
            .with_format(format)
            .with_trim(true)
            .with_unwrap(true)
            .with_cursor(false),
    )?;
    let bytes = formatter.format_alloc(None)?;
    Ok(bytes.as_ref().to_vec())
}

fn viewport_point(row: u16, col: u16) -> Point {
    Point::Viewport(PointCoordinate {
        x: col,
        y: u32::from(row),
    })
}
