use super::*;

#[test]
fn the_backing_ioctls_are_the_kernels() {
    // From linux/fuse.h: _IOW(229, 1, struct fuse_backing_map) and _IOW(229, 2, uint32_t).
    assert_eq!(FUSE_DEV_IOC_BACKING_OPEN, 0x4010_e501);
    assert_eq!(FUSE_DEV_IOC_BACKING_CLOSE, 0x4004_e502);
}
