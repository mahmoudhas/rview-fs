use std::time::Duration;

use super::leave_while_detaching;

#[tokio::test]
async fn the_other_machines_are_left_while_the_unmount_still_runs() {
    let (left, hears) = std::sync::mpsc::channel();
    // An unmount that ends once the others are left, as the kernel's may take a minute.
    let detached = leave_while_detaching(
        move || hears.recv_timeout(Duration::from_secs(5)).is_ok(),
        async move { left.send(()).unwrap() },
    )
    .await
    .unwrap();
    assert!(
        detached,
        "the unmount was waited for before the others were left"
    );
}
