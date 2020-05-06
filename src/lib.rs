extern crate libc;

mod cbuffer_raw;

pub use cbuffer_raw::{channel, BufferSize, Sender, Receiver};

#[cfg(test)]
mod tests {
    use chrono::Local;

    #[test]
    fn test_channel() {
        use std::time::Duration;
        use super::{channel, BufferSize};
        use std::thread;

        let (mut sender, mut receiver) = channel(BufferSize::Buf128M);

        let n = 5_000_000;
        let v = b"123abc";

        thread::spawn(move || {
            let begin = Local::now();
            for _i in 0..n {
                loop {
                    if sender.try_push(v) { break; }
                }
            }
            let end = Local::now();
            let a = end - begin;
            println!("sending speed: {}", (n as f32/a.num_microseconds().unwrap()as f32)*1000000f32);
        });

        thread::sleep(Duration::from_millis(5));

        let mut count = 0;
        let begin = Local::now();
        loop {
            if !receiver.try_pop(|bytes| {
                assert_eq!(v, bytes);
                count += 1;
            }){
                break;
            }
        }
        let end = Local::now();
        let b = end - begin;
        println!("receiving speed: {}", (n as f32/b.num_microseconds().unwrap()as f32)*1000000f32);
        assert_eq!(count, n);
    }
}