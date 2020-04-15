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
use std::sync::{Arc, Mutex};

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

    pub fn push(&mut self, elem: &[u8]) -> bool {
        unsafe { (*self.inner.get()).push(elem) }
    }
}

impl Receiver {
    fn new(inner: Arc<UnsafeCell<CBuffer>>) -> Receiver {
        Receiver { inner }
    }

    pub fn pop<F>(&mut self, consumer: F)
        where F: FnMut(&[u8]) -> ()
    {
        unsafe { (*self.inner.get()).pop(consumer)}
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

pub fn page_size() -> usize {
    unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
}

pub struct CBuffer {
    capacity: usize,
    pointer: ptr::NonNull<u8>,
    head_tail: Arc<Mutex<(u32, u32)>>,
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
                head_tail: Arc::new(Mutex::new((0u32, 0u32))),
            })
        }
    }


    pub fn push(&mut self, data: &[u8]) -> bool {
        let data_len = data.len();
        let size = (data_len + 4) as u32;
        if self.unused() <= size as usize {
            return false;
        }
        let ht = &self.head_tail;
        let mut head_tail = ht.lock().unwrap();
        let tail = head_tail.1;
        self.writable_slice(tail as isize, 4).copy_from_slice(&transform_u32_to_array_of_u8(data_len as u32));
        self.writable_slice((tail + 4) as isize, data_len).copy_from_slice(data);

        if self.capacity < (tail + size) as usize {
            // self.head_tail.store((head, ((tail + size) as usize % self.capacity) as u32));
            head_tail.1 = ((tail + size) as usize % self.capacity) as u32;
        } else {
            // self.head_tail.store((head, tail + size));
            head_tail.1 = tail + size;
        }
        true
    }
    //
    // fn offer(&mut self, data: &[u8]) -> bool {
    //     let size = data.len();
    //     if self.unused() <= size {
    //         return false;
    //     }
    //     // let (head, tail) = self.head_tail.lock();
    //     let mut head_tail = self.head_tail.lock().unwrap();
    //     let tail = head_tail.1;
    //     self.writable_slice(tail as isize, size).copy_from_slice(data);
    //     // let _n = cbuffer_raw::rs_offer(&mut unsafe { *self.inner }, data, tail);
    //     if self.capacity  < tail as usize + size {
    //         // self.head_tail.store((head, ((tail as usize + size) % self.capacity) as u32));
    //         // self.head_tail.store((head, ((tail + size) as usize % self.capacity) as u32));
    //         head_tail.1 = ((tail as usize + size) % self.capacity) as u32;
    //     } else {
    //         // self.head_tail.store((head, (tail as usize + size) as u32));
    //         // self.head_tail.store((head, tail + size));
    //         head_tail.1 = tail + size as u32;
    //     }
    //     true
    // }

    pub fn pop<F>(&self,mut consumer: F)
        where F: FnMut(&[u8]) -> ()
    {
        let mut head_tail = self.head_tail.lock().unwrap();
        if head_tail.0 == head_tail.1 {
            return;
        }
        let head = head_tail.0;
        let len = transform_array_of_u8_to_u32(self.readable_slice(head as isize, 4).to_vec().as_slice());
        let rt = self.readable_slice((head + 4) as isize, len as usize);
        consumer(rt);
        head_tail.0 = len + 4 + head as u32;
        // self.head_tail.store((len + 4 + head as u32, tail));
    }

    // fn poll(&self, len: usize) -> Option<Vec<u8>> {
    //     let mut head_tail = self.head_tail.lock().unwrap();
    //     if head_tail.0 == head_tail.1 {
    //         return None;
    //     }
    //     let head = head_tail.0;
    //     let r = self.readable_slice(head as isize, len).to_vec();
    //     head_tail.0 = len as u32 + head;
    //     // self.head_tail.store(((len + head as usize) as u32, tail));
    //     Some(r)
    // }

    // pub fn is_empty(&self) -> bool {
    //     let (head, tail) = self.head_tail.load();
    //     head == tail
    // }

    pub fn size(&self) -> usize {
        self.capacity
    }

    pub fn used(&self) -> usize {
        let head_tail = self.head_tail.lock().unwrap();
        let (head, tail) = (head_tail.0, head_tail.1);
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
            if munmap(self.pointer.as_ptr().offset(0) as *mut c_void, 2 * self.capacity) < 0 {
                panic!("munmap({:p}, {}) failed", self.pointer, 2 * self.capacity)
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
/*
    #[test]
    fn test_offer() {
        use super::{CBuffer, BufferSize};
        let mut b = CBuffer::with_capacity(BufferSize::Buf128M).unwrap();
        // let (head,tail) = b.head_tail.load();
        b.offer(b"12");
        b.offer(b"34");
        let head_tail = b.head_tail.lock().unwrap();
        let (head ,tail) = (head_tail.0, head_tail.1);
        println!("used: {}, tail: {:?}, head: {:?}", b.used(), tail, head);
        assert_eq!(Some(b"12".to_vec()), b.poll(2));
        assert_eq!(Some(b"34".to_vec()), b.poll(2));
    }

    #[test]
    fn test_push() {
        use super::{CBuffer, BufferSize};
        let mut b = CBuffer::with_capacity(BufferSize::Buf128M).unwrap();
        b.push(b"12AB");
        b.push(b"acefg");
        b.pop(|bytes|{
            assert_eq!(b"12AB", bytes);
        });
        b.pop(|bytes|{
            assert_eq!(b"acefg", bytes);
        });
    }*/
}