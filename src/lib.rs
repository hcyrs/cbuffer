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

        let n = 5000000;

        thread::spawn(move || {
            let begin = Local::now();
            for _i in 0..n {
                let v = b"123";
                loop {
                    if sender.push(v) { break; }
                }
            }
            let end = Local::now();
            let a = end - begin;
            println!("{}", a);
        });

        thread::sleep(Duration::from_millis(5));

        let mut count = 0;
        let begin = Local::now();
        loop {
            let mut do_break = false;
            receiver.pop(|bytes| {
                count += 1;
                if count == n {
                    do_break = true;
                }
            });

            if do_break {
                println!("-----n: {}", count);
                break;
            }
        }
        let end = Local::now();
        let b = end - begin;
        println!("{}", b);
        println!("receive: {}", count);
    }
}