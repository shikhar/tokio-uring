use std::{io::Write, os::unix::prelude::OpenOptionsExt, alloc::Layout, ops::{Deref, DerefMut}};

use tempfile::NamedTempFile;
use tokio_uring::{fs::OpenOptions, buf::{IoBuf, IoBufMut}};

const HELLO: &[u8] = b"hello world...";

fn tempfile() -> NamedTempFile {
    // we need to create the tempfile outside of /tmp, because /tmp may be a tmpfs and not support
    // direct io.
    NamedTempFile::new_in(".").unwrap()
}

pub struct AlignedBuffer {
    layout: Layout,
    data: *mut u8,
    len: usize,
}

impl AlignedBuffer {
    pub fn alloc(layout: Layout) -> Self {
        Self {
            layout,
            data: unsafe { std::alloc::alloc(layout) },
            len: 0,
        }
    }

    pub unsafe fn set_len(&mut self, new_len: usize) {
        debug_assert!(new_len <= self.layout.size());
        self.len = new_len;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn layout(&self) -> Layout {
        self.layout
    }

    pub fn remaining(&self) -> usize {
        self.layout.size() - self.len()
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.data as *const _
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data
    }

    pub fn extend_from_slice(&mut self, other: &[u8]) {
        assert!(other.len() <= self.remaining());

        let buf = unsafe { std::slice::from_raw_parts_mut(self.data.add(self.len()), other.len()) };
        buf.copy_from_slice(other);
        self.len += other.len();
    }
}

impl Deref for AlignedBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.data, self.len()) }
    }
}

impl DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.data, self.len()) }
    }
}

unsafe impl IoBuf for AlignedBuffer {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    fn bytes_total(&self) -> usize {
        self.layout.size()
    }
}

unsafe impl IoBufMut for AlignedBuffer {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        if self.len() < pos {
            self.set_len(pos);
        }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe { std::alloc::dealloc(self.data, self.layout) }
    }
}

#[test]
fn basic_read() {
    tokio_uring::start(async {
        let mut tempfile = tempfile();
        tempfile.write_all(HELLO).unwrap();

        let file = OpenOptions::new().read(true).custom_flags(libc::O_DSYNC).open(tempfile.path()).await.unwrap();

        let buffer = AlignedBuffer::alloc(Layout::from_size_align(4096, 4096).unwrap());

        let (res, buf) = file.read_at(buffer, 0).await;

        let n = res.unwrap();

        assert_eq!(n, HELLO.len());
        assert_eq!(&buf[..n], HELLO);
    });
}

#[test]
fn basic_write() {
    tokio_uring::start(async {
        let tempfile = tempfile();
        let file = OpenOptions::new().write(true).create(true).truncate(true).custom_flags(libc::O_DSYNC).open(tempfile.path()).await.unwrap();
        let mut buffer = AlignedBuffer::alloc(Layout::from_size_align(4096, 4096).unwrap());
        buffer.extend_from_slice(&[0; 4096]);
        dbg!(buffer.len());
        buffer[..HELLO.len()].copy_from_slice(HELLO);
        let (res, buf) = file.write_at(buffer, 0).submit().await;
        assert_eq!(res.unwrap(), 4096);

        let data = std::fs::read(tempfile.path()).unwrap();

        assert_eq!(&data[..], &buf[..]);
    });
}