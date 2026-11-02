//! rview-fs: one read-only FUSE file system that is the union of the directory each
//! machine of a group exports. A file of another machine is held open there while it is
//! open here, and read whole into host memory over velo (RDMA with the `ucx` feature)
//! when any byte of it is first read.
//!
//! Machines learn of each other's files over NATS (sync.rs), from whatever writes into
//! the exported directories, which announces the files that come and go, and from each
//! other, listing a machine when it shows up. README.md has the protocol.

pub mod cache;
pub mod content;
pub mod fs;
pub mod health;
pub mod held;
pub mod index;
pub mod passthrough;
pub mod path;
pub mod remote;
pub mod state;
pub mod sync;
pub mod transfer;
