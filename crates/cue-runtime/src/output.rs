use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;

use cue_core::OutputStream;
use cue_core::StepId;

use crate::{OutputAppend, OutputSlice, OutputStore, RuntimeError, RuntimeErrorKind};

pub const DEFAULT_OUTPUT_CAPACITY: usize = 1024 * 1024;

pub struct MemoryOutputStore {
    capacity: usize,
    streams: Mutex<BTreeMap<(StepId, OutputStream), RetainedStream>>,
}

impl MemoryOutputStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            streams: Mutex::new(BTreeMap::new()),
        }
    }
}

impl Default for MemoryOutputStore {
    fn default() -> Self {
        Self::new(DEFAULT_OUTPUT_CAPACITY)
    }
}

impl OutputStore for MemoryOutputStore {
    fn append(
        &self,
        step: StepId,
        stream: OutputStream,
        data: &[u8],
    ) -> Result<OutputAppend, RuntimeError> {
        let mut streams = self
            .streams
            .lock()
            .map_err(|_| RuntimeError::infrastructure("output store lock poisoned"))?;
        let retained = streams
            .entry((step, stream))
            .or_insert_with(|| RetainedStream::new(self.capacity));
        retained.append(data)
    }

    fn read(
        &self,
        step: StepId,
        stream: OutputStream,
        offset: u64,
        maximum: usize,
    ) -> Result<OutputSlice, RuntimeError> {
        let streams = self
            .streams
            .lock()
            .map_err(|_| RuntimeError::infrastructure("output store lock poisoned"))?;
        let Some(retained) = streams.get(&(step, stream)) else {
            return Ok(OutputSlice {
                offset,
                data: Vec::new(),
                next_offset: offset,
                truncated: false,
            });
        };
        retained.read(offset, maximum)
    }

    fn tail(
        &self,
        step: StepId,
        stream: OutputStream,
        maximum: usize,
    ) -> Result<OutputSlice, RuntimeError> {
        let streams = self
            .streams
            .lock()
            .map_err(|_| RuntimeError::infrastructure("output store lock poisoned"))?;
        let Some(retained) = streams.get(&(step, stream)) else {
            return Ok(OutputSlice {
                offset: 0,
                data: Vec::new(),
                next_offset: 0,
                truncated: false,
            });
        };
        let retained_len = u64::try_from(retained.data.len())
            .map_err(|_| RuntimeError::infrastructure("retained output length overflow"))?;
        let requested = u64::try_from(maximum).map_err(|_| {
            RuntimeError::new(RuntimeErrorKind::InvalidInput, "read limit overflow")
        })?;
        let offset = retained
            .end_offset
            .saturating_sub(requested.min(retained_len));
        retained.read(offset, maximum)
    }
}

struct RetainedStream {
    capacity: usize,
    start_offset: u64,
    end_offset: u64,
    data: VecDeque<u8>,
}

impl RetainedStream {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            start_offset: 0,
            end_offset: 0,
            data: VecDeque::new(),
        }
    }

    fn append(&mut self, incoming: &[u8]) -> Result<OutputAppend, RuntimeError> {
        let start_offset = self.end_offset;
        let incoming_len = u64::try_from(incoming.len())
            .map_err(|_| RuntimeError::infrastructure("output append length overflow"))?;
        self.end_offset = self
            .end_offset
            .checked_add(incoming_len)
            .ok_or_else(|| RuntimeError::infrastructure("output offset overflow"))?;
        self.data.extend(incoming.iter().copied());
        if self.data.len() > self.capacity {
            let discarded = self.data.len() - self.capacity;
            self.data.drain(..discarded);
            self.start_offset = self
                .start_offset
                .checked_add(u64::try_from(discarded).map_err(|_| {
                    RuntimeError::infrastructure("discarded output length overflow")
                })?)
                .ok_or_else(|| RuntimeError::infrastructure("output offset overflow"))?;
        }
        Ok(OutputAppend {
            start_offset,
            end_offset: self.end_offset,
        })
    }

    fn read(&self, requested: u64, maximum: usize) -> Result<OutputSlice, RuntimeError> {
        let offset = requested.max(self.start_offset).min(self.end_offset);
        let local = usize::try_from(offset - self.start_offset)
            .map_err(|_| RuntimeError::infrastructure("output offset overflow"))?;
        let available = self.data.len().saturating_sub(local);
        let length = available.min(maximum);
        let data = self.data.iter().skip(local).take(length).copied().collect();
        let next_offset = offset
            .checked_add(
                u64::try_from(length)
                    .map_err(|_| RuntimeError::infrastructure("output read length overflow"))?,
            )
            .ok_or_else(|| RuntimeError::infrastructure("output offset overflow"))?;
        Ok(OutputSlice {
            offset,
            data,
            next_offset,
            truncated: requested < self.start_offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use cue_core::ExecutionId;

    use super::*;

    fn step() -> StepId {
        StepId {
            execution: ExecutionId(1),
            index: 1,
        }
    }

    #[test]
    fn offsets_remain_absolute_after_retention_wraps() {
        let store = MemoryOutputStore::new(5);
        assert_eq!(
            store.append(step(), OutputStream::Stdout, b"abcd").unwrap(),
            OutputAppend {
                start_offset: 0,
                end_offset: 4
            }
        );
        store.append(step(), OutputStream::Stdout, b"efgh").unwrap();
        assert_eq!(
            store.read(step(), OutputStream::Stdout, 0, 10).unwrap(),
            OutputSlice {
                offset: 3,
                data: b"defgh".to_vec(),
                next_offset: 8,
                truncated: true,
            }
        );
    }

    #[test]
    fn streams_and_steps_are_isolated() {
        let store = MemoryOutputStore::new(16);
        store.append(step(), OutputStream::Stdout, b"out").unwrap();
        store.append(step(), OutputStream::Stderr, b"err").unwrap();
        assert_eq!(
            store.tail(step(), OutputStream::Stdout, 16).unwrap().data,
            b"out"
        );
        assert_eq!(
            store.tail(step(), OutputStream::Stderr, 16).unwrap().data,
            b"err"
        );
    }
}
