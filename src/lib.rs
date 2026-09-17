use memchr::memchr2;
use std::cmp;
use std::io::Error;
use tokio_util::bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Decoder;

#[derive(Clone)]
pub struct DoubleDelimiterCodec {
    a: u8,
    b: u8,
    is_discarding: bool,
    next_index: usize,
    max_length: usize,
}

pub enum DoubleDelimiterCodecError {
    MaxChunkLengthExceeded,
    Io(Error),
}

impl From<Error> for DoubleDelimiterCodecError {
    fn from(e: Error) -> Self {
        DoubleDelimiterCodecError::Io(e)
    }
}

impl DoubleDelimiterCodec {
    pub fn new(a: u8, b: u8) -> Self {
        DoubleDelimiterCodec {
            a,
            b,
            is_discarding: false,
            next_index: 0,
            max_length: usize::MAX,
        }
    }

    pub fn new_with_max_length(a: u8, b: u8, max_length: usize) -> Self {
        DoubleDelimiterCodec {
            max_length,
            ..DoubleDelimiterCodec::new(a, b)
        }
    }
}

const DELIM_SIZE: usize = 2;

impl Decoder for DoubleDelimiterCodec {
    type Item = Bytes;
    type Error = DoubleDelimiterCodecError;

    // implementation details shamelessly stolen from AnyDelimiterCodec
    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            let read_to = cmp::min(self.max_length.saturating_add(DELIM_SIZE), buf.len());
            let slice = &buf[self.next_index..read_to];

            let new_chunk_offset = memchr2(self.a, self.b, slice);

            match (self.is_discarding, new_chunk_offset) {
                (true, Some(offset)) => {
                    // some delimiter found, but we were discarding
                    // + DELIM_SIZE => chop off with delimiter
                    buf.advance(offset + self.next_index + DELIM_SIZE);
                    self.is_discarding = false;
                    self.next_index = 0; // rewind to start as incriminated section was chopped
                    // no return, continue reading buffer in loop
                }
                (true, None) => {
                    // discarding and we didn't find delimiter
                    // no delimiter found till end of slice
                    buf.advance(read_to); // chop off
                    // we continue discarding (self.is_discarding still true)
                    self.next_index = 0;

                    if buf.is_empty() {
                        return Ok(None); // waiter! more bytes please 😋️
                    }
                }
                (false, Some(offset)) => {
                    // not discarding and we found some delimiter
                    let new_chunk_index = offset + self.next_index;
                    self.next_index = 0;
                    // + DELIM_SIZE => message will contain delimiter
                    let chunk = buf.split_to(new_chunk_index + DELIM_SIZE);

                    return Ok(Some(chunk.freeze()));
                }
                // no delimiter found and reached max length
                (false, None) if buf.len() > self.max_length => {
                    // return error (max length reached) and start discarding on next call
                    self.is_discarding = true;

                    return Err(DoubleDelimiterCodecError::MaxChunkLengthExceeded);
                }
                (false, None) => {
                    // no delimiter found but didn't reach length limit
                    self.next_index = read_to; // skip it!

                    return Ok(None); // waiter... I am still hungry 🥺️
                }
            }
        }
    }
}
