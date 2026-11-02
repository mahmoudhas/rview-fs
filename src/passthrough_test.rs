use super::*;

#[test]
fn the_backing_ioctls_are_the_kernels() {
    // From linux/fuse.h: _IOW(229, 1, struct fuse_backing_map) and _IOW(229, 2, uint32_t).
    assert_eq!(FUSE_DEV_IOC_BACKING_OPEN, 0x4010_e501);
    assert_eq!(FUSE_DEV_IOC_BACKING_CLOSE, 0x4004_e502);
}

#[test]
fn the_opens_of_an_inode_share_one_backing_file() {
    let mut backings = Backings::default();
    assert_eq!(backings.reuse(7), None);
    assert_eq!(backings.adopt(7, 1), (1, None));
    assert_eq!(backings.reuse(7), Some(1));
    // Registered by an open that raced another: the first one's is used, and its own left over.
    assert_eq!(backings.adopt(7, 2), (1, Some(2)));
    assert_eq!(backings.adopt(8, 3), (3, None)); // another inode
    // Three opens of inode 7: its backing file goes with the last.
    assert_eq!(backings.release(7), None);
    assert_eq!(backings.release(7), None);
    assert_eq!(backings.release(7), Some(1));
    assert_eq!(backings.reuse(7), None);
    assert_eq!(backings.release(8), Some(3));
}
