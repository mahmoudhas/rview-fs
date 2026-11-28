use std::time::Duration;

use clap::CommandFactory as _;

use super::{Args, leave_while_detaching, list_once_joined};

#[tokio::test]
async fn the_other_machines_hear_of_this_one_before_it_lists_its_export() {
    let joined = std::cell::Cell::new(false);
    // A listing that says whether the others had heard of this machine when it began.
    let heard_first = list_once_joined(
        || {
            joined.set(true);
            Ok(())
        },
        async { Ok(joined.get()) },
    )
    .await
    .unwrap();
    assert!(
        heard_first,
        "the export was listed before the others heard of this machine"
    );
}

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

#[test]
fn what_a_deployment_tunes_is_read_from_the_environment_too() {
    let command = Args::command();
    let variable = |flag: &str| {
        let arg = command.get_arguments().find(|a| a.get_id() == flag);
        arg.and_then(|a| a.get_env()?.to_str())
    };
    assert_eq!(variable("rescan_s"), Some("RVIEW_RESCAN_S"));
    assert_eq!(
        variable("fuse_max_background"),
        Some("RVIEW_FUSE_MAX_BACKGROUND")
    );
    assert_eq!(variable("fuse_workers"), Some("RVIEW_FUSE_WORKERS"));
    assert_eq!(variable("rdma_devices"), Some("RVIEW_RDMA_DEVICES"));
}
