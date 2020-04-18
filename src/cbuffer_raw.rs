#![allow(dead_code)]

use crossbeam::atomic::AtomicCell;
use byteorder::{ByteOrder, LittleEndian};
use libc::{
    c_void,
    mmap, munmap,
    MAP_ANONYMOUS, MAP_FAILED, MAP_FIXED, MAP_PRIVATE, MAP_SHARED,
    PROT_NONE, PROT_READ, PROT_WRITE,
};
use std::{ptr, slice};
use std::cell::UnsafeCell;
use std::sync::Arc;
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
    fn cause(&self) -> Option<& dyn std::error::Error> { None }
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

pub fn page_size() -> usize {
    unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
}

pub struct CBuffer {
    capacity: usize,
    pointer: ptr::NonNull<u8>,
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

        unsafe {
            let checked_mmap = |ptr, size, prot, flags| {
                let p = mmap(ptr, size, prot, flags, -1, 0);
                if p == MAP_FAILED { return Err(Error::OS); }
                Ok(p)
            };

            let base_pointer = checked_mmap(ptr::null_mut(),
                                            2 * capacity,
                                            PROT_NONE,
                                            MAP_ANONYMOUS | MAP_PRIVATE)?;
            let primary = checked_mmap(base_pointer,
                                       capacity,
                                       PROT_READ | PROT_WRITE,
                                       MAP_FIXED | MAP_SHARED | MAP_ANONYMOUS)?;
            checked_mmap(base_pointer.offset(capacity as isize),
                         capacity,
                         PROT_READ | PROT_WRITE,
                         MAP_FIXED | MAP_SHARED | MAP_ANONYMOUS)?;

            Ok(CBuffer {
                capacity,
                pointer: ptr::NonNull::new(primary as *mut u8).ok_or(Error::OS).unwrap(),
                head: AtomicCell::new(0u32),
                tail: AtomicCell::new(0u32),
            })
        }
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
        self.writable_slice(tail as isize, 4).copy_from_slice(&transform_u32_to_array_of_u8(size as u32));
        self.writable_slice((tail + 4) as isize, size).copy_from_slice(data);
        if self.capacity < (tail + size + 4) as usize {
            self.tail.store(((tail + size + 4) as usize % self.capacity) as u32);
        } else {
            // self.head_tail.store((head, tail + size));
            self.tail.store((tail + size + 4) as u32);
        }
        true
    }

    pub fn pop<F>(&self, mut consumer: F) -> bool
        where F: FnMut(&[u8]) -> ()
    {
        let tail = self.tail.load() as usize;
        let head = self.head.load() as usize;
        if head == tail {
            return false;
        }
        let len = transform_array_of_u8_to_u32(self.readable_slice(head as isize, 4).to_vec().as_slice());
        let rt = self.readable_slice((head + 4) as isize, len as usize);
        consumer(rt);
        self.head.store(len + 4 + head as u32);
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

    fn readable_slice(&self, head: isize, len: usize) -> &[u8] {
        unsafe {
            slice::from_raw_parts(self.pointer.as_ptr().offset(head), len)
        }
    }

    fn writable_slice(&self, tail: isize, len: usize) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(self.pointer.as_ptr().offset(tail), len)
        }
    }
}

impl Drop for CBuffer {
    fn drop(&mut self) {
        unsafe {
            // It's not clear what makes the most sense for handling
            // errors in `drop`, but the consensus seems to be either
            // ignore the error, or panic.
            if munmap(self.pointer.as_ptr().offset(0) as *mut c_void, 2*self.capacity) < 0 {
                panic!("munmap({:p}, {}) failed", self.pointer, 2*self.capacity)
            }
        }
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