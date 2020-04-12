extern crate  libc;

mod cbuffer_raw;

pub use cbuffer_raw::{channel, BufferSize};

#[cfg(test)]
mod tests {
    #[test]
    fn test_channel() {
        use std::time::Duration;
        use super::{channel, BufferSize};
        use std::thread;

        let (mut sender, mut receiver) = channel(BufferSize::Buf128M);

        thread::spawn(move ||{
            for _i in 0..2000 {
                let v = b"123";
                sender.push(v);
            }
        });

        thread::sleep(Duration::from_millis(50));

        let mut count = 0;
        loop {
            match receiver.pop() {
                Some(_) => {
                    count +=1;
                }
                None => {
                    break;
                }
            }
        }
        println!("receive: {}", count);
    }
}