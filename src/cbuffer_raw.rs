#![allow(dead_code)]

use crossbeam::atomic::AtomicCell;
use byteorder::{ByteOrder, LittleEndian};
use std::cell::UnsafeCell;
use std::sync::Arc;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

pub struct Sender {
    inner: Arc<UnsafeCell<CBuffer>>,
}

unsafe impl Send for Sender {}

pub struct Receiver {
    inner: Arc<UnsafeCell<CBuffer>>,
}

unsafe impl Send for Receiver {}

pub fn channel(s: BufferSize) -> (Sender, Receiver) {
    let a = Arc::new(UnsafeCell::new(CBuffer::with_capacity(s).expect("fail to create cbuffer.")));
    (Sender::new(a.clone()), Receiver::new(a))
}

impl Sender {
    fn new(inner: Arc<UnsafeCell<CBuffer>>) -> Sender {
        Sender { inner }
    }

    pub fn try_push(&mut self, elem: &[u8]) -> bool {
        unsafe { (*self.inner.get()).push(elem) }
    }

    pub fn push(&mut self, elem: &[u8]) {
        if !unsafe { (*self.inner.get()).push(elem) } {
            std::thread::sleep(Duration::from_micros(5));
        }
    }
}

impl Receiver {
    fn new(inner: Arc<UnsafeCell<CBuffer>>) -> Receiver {
        Receiver { inner }
    }

    pub fn try_pop<F>(&self, consumer: F) -> bool
        where F: FnMut(&[u8]) -> ()
    {
        unsafe { (*self.inner.get()).pop(consumer) }
    }

    pub fn pop<F>(&self, consumer: F)
        where F: FnMut(&[u8]) -> ()
    {
        if !unsafe { (*self.inner.get()).pop(consumer) } {
            std::thread::sleep(Duration::from_micros(5));
        }
    }
}


#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Error {
    OS,
    Overflow,
    Underflow,
}

impl std::error::Error for Error {
    fn description(&self) -> &str { "cbuffer error" }
    fn cause(&self) -> Option<&dyn std::error::Error> { None }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match *self {
            Error::OS => write!(f, "OS error"),
            Error::Overflow => write!(f, "overflow"),
            Error::Underflow => write!(f, "underflow"),
        }
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(_err: std::num::TryFromIntError) -> Error {
        Error::OS
    }
}

pub const BUF_128M: u32 = 27;
pub const BUF_256M: u32 = 28;
pub const BUF_512M: u32 = 29;

#[allow(dead_code)]
pub enum BufferSize {
    Buf64M,
    Buf128M,
    Buf256M,
    Buf512M,
}

pub struct CBuffer {
    capacity: usize,
    v: Box<[u8]>,
    head: AtomicCell<u32>,
    tail: AtomicCell<u32>,
}

unsafe impl Send for CBuffer {}

unsafe impl Sync for CBuffer {}

impl CBuffer {
    pub fn with_capacity(s: BufferSize) -> Result<Self, Error> {
        let capacity = match s {
            BufferSize::Buf64M => {
                64 * 1024 * 1024usize
            }
            BufferSize::Buf128M => {
                128 * 1024 * 1024usize
            }
            BufferSize::Buf256M => {
                256 * 1024 * 1024usize
            }
            BufferSize::Buf512M => {
                512 * 1024 * 1024usize
            }
        };
        let b = vec![0u8; 2 * capacity].into_boxed_slice();

        Ok(CBuffer {
            capacity,
            v: b,
            head: AtomicCell::new(0u32),
            tail: AtomicCell::new(0u32),
        })
    }

    pub fn push(&mut self, data: &[u8]) -> bool {
        let size = data.len();
        let tail = self.tail.load() as usize;
        let head = self.head.load() as usize;
        let used = if head <= tail {
            (tail - head) as usize
        } else {
            self.capacity - (head as usize - tail as usize)
        };
        let unused = self.capacity - used;

        if unused <= size + 4 {
            return false;
        }
        self.write(tail, 4, &transform_u32_to_array_of_u8(size as u32));
        self.write(tail + 4, size, data);
        if self.capacity < (tail + size + 4) as usize {
            self.tail.store(((tail + size + 4) as usize % self.capacity) as u32);
        } else {
            // self.head_tail.store((head, tail + size));
            self.tail.store((tail + size + 4) as u32);
        }
        true
    }

    pub fn pop<F>(&self, consumer: F) -> bool
        where F: FnMut(&[u8]) -> ()
    {
        let tail = self.tail.load() as usize;
        let head = self.head.load() as usize;

        if head == tail {
            return false;
        }

        let len = self.read_length(head, 4);
        self.read(head + 4, len as usize, consumer);
        self.tail.store(len + 4 + head as u32);
        true
    }

    pub fn is_empty(&self) -> bool {
        self.tail.load() == self.head.load()
    }

    pub fn size(&self) -> usize {
        self.capacity
    }

    pub fn used(&self) -> usize {
        let (head, tail) = {
            (self.head.load(),
             self.tail.load())
        };
        if head <= tail {
            (tail - head) as usize
        } else {
            self.capacity - (head as usize - tail as usize)
        }
    }

    pub fn unused(&self) -> usize {
        self.capacity - self.used()
    }

    fn read<F>(&self, head: usize, len: usize, mut consumer: F)
        where F: FnMut(&[u8]) -> ()
    {
        let r = head;
        consumer(&self.v.deref()[r..r + len]);
    }

    fn read_length(&self, head: usize, len: usize) -> u32 {
        let r = head;
        transform_array_of_u8_to_u32(&self.v.deref()[r..r + len])
    }

    fn write(&mut self, tail: usize, len: usize, data: &[u8]) {
        let w = tail;
        &mut self.v.deref_mut()[w..w + len].copy_from_slice(data);
    }
}


#[inline]
fn transform_u32_to_array_of_u8(x: u32) -> [u8; 4] {
    let mut bytes: [u8; 4] = [0; 4];
    LittleEndian::write_u32(&mut bytes, x);
    bytes
}

#[inline]
fn transform_array_of_u8_to_u32(x: &[u8]) -> u32 {
    LittleEndian::read_u32(x)
}


#[cfg(test)]
mod tests {
    #[test]
    fn test_new() {
        use super::{CBuffer, BufferSize};
        let b = CBuffer::with_capacity(BufferSize::Buf128M).unwrap();
        assert_eq!(134217728usize, b.size());
        assert_eq!(0usize, b.used());
    }
}