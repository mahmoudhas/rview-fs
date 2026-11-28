use std::path::{Path, PathBuf};

use futures::FutureExt as _;
use velo::Velo;

use super::*;
use crate::cache::Key;
use crate::held::Held;
use crate::index::{Entry, EntryKind};
use crate::remote::{Remote, RemoteFile};
use crate::state::testing::{state, state_of, velo};
use crate::transfer::tests::never_answering;

#[test]
fn hidden_files_being_written_are_not_taken() {
    let announcement: Announcement = serde_json::from_str(
        r#"{"node": "a", "updates": [
            {"op": "add", "path": "d/x.bin", "size": 4, "mtime": 1.0},
            {"op": "add", "path": "d/.x.bin.k3j.tmp", "size": 2, "mtime": 1.0},
            {"op": "add", "path": ".writing/y.bin", "size": 2, "mtime": 1.0},
            {"op": "add", "path": "/etc/passwd", "size": 2, "mtime": 1.0},
            {"op": "add", "path": "d/../z.bin", "size": 2, "mtime": 1.0}
        ]}"#,
    )
    .unwrap();
    let node = NodeId::parse(&announcement.node).unwrap();
    let mut index = Index::new();
    apply(&mut index, &node, announcement.updates);
    assert_eq!(index.list(""), Some(vec![("d".to_owned(), EntryKind::Dir)]));
    assert_eq!(
        index.list("d"),
        Some(vec![("x.bin".to_owned(), EntryKind::File)])
    );
}

#[test]
fn a_heartbeat_tells_whether_its_node_is_new_restarted_or_known() {
    let mut nodes = Nodes::default();
    let b = NodeId::parse("b").unwrap();
    let (first, second) = (InstanceId::new_v4(), InstanceId::new_v4());
    assert_eq!(nodes.heard(b.clone(), first), Heard::New);
    assert_eq!(nodes.heard(b.clone(), first), Heard::Again);
    assert_eq!(nodes.heard(b.clone(), second), Heard::Restarted(first));
    assert_eq!(nodes.instance(&b), Some(second));
    nodes.forget(&b);
    assert_eq!(nodes.heard(b, second), Heard::New); // back after it went
}

#[test]
fn a_listing_goes_into_the_index_over_many_short_holds_of_it() {
    // 20,000 files are more than one hold of LISTING_HOLD takes.
    let index = parking_lot::RwLock::new(Index::new());
    let b = NodeId::parse("b").unwrap();
    let stat = Stat {
        size: 1,
        mtime: 1.0,
    };
    let files = (0..20_000)
        .map(|i| {
            (
                RelPath::parse(&format!("d{}/f{i}.bin", i % 100)).unwrap(),
                stat,
            )
        })
        .collect();
    let mut replacement = Replacement::new(&b, files, 10.0, index.read().mark());
    let mut holds = 1;
    while replace_part(index.write(), &mut replacement) {
        holds += 1;
        // Between two holds the index is anyone's.
        assert!(index.try_read().is_some());
    }
    assert!(holds > 1, "the whole listing went in at one hold");
    assert_eq!(index.read().file_count(), 20_000);
}

/// Serve the export at `root` with `velo`, as another node's rview-fs does.
fn serve(velo: &Velo, root: &Path) {
    let stages = Arc::new(transfer::Stages::new(1 << 20));
    transfer::serve(velo, root.to_owned(), false, stages, Arc::new(Held::new(8))).unwrap();
}

fn export_of_one_file() -> tempfile::TempDir {
    let export = tempfile::tempdir().unwrap();
    std::fs::write(export.path().join("x.bin"), b"1234").unwrap();
    export
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_file_of_another_node_is_read_over_either_path_and_a_failed_read_says_whose() {
    let export = export_of_one_file();
    let (ours, theirs) = (velo().await, velo().await);
    serve(&theirs, export.path());
    ours.register_peer(theirs.peer_info()).unwrap();
    let state = state(ours);
    let b = NodeId::parse("b").unwrap();
    state.nodes.write().heard(b.clone(), theirs.instance_id());
    let key = |path| Key {
        node: b.clone(),
        path: RelPath::parse(path).unwrap(),
    };
    let runtime = tokio::runtime::Handle::current();
    let file = RemoteFile::open(Arc::clone(&state), key("x.bin"), runtime)
        .await
        .unwrap()
        .unwrap();
    for rdma in [true, false] {
        let (instance, handle) = (file.remote.instance, file.remote.handle);
        let read = transfer::fetch(&state.velo, instance, handle, rdma, &state.read_bytes);
        assert_eq!(&read.await.unwrap()[..], b"1234");
    }
    // Without UCX, velo moves both in chunks over TCP.
    let read = &state.read_bytes;
    assert_eq!((read.get("rdma"), read.get("tcp")), (0, 8));
    assert_eq!(
        &state.remote_content(&file.remote).await.unwrap()[..],
        b"1234"
    );
    let unopened = Remote {
        key: key("y.bin"),
        handle: file.remote.handle + 1,
        ..file.remote.clone()
    };
    let failed = format!("{:#}", state.remote_content(&unopened).await.unwrap_err());
    assert!(failed.starts_with("reading y.bin of b: "), "{failed}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_listing_bigger_than_a_message_is_listed() {
    // A message carries at most 16 MiB, and the listing of thousands of files deep in
    // folders is more: 5,000 paths of nearly 4 KiB.
    let export = tempfile::tempdir().unwrap();
    let deep: PathBuf = (0..15).map(|i| format!("{i:0>250}")).collect();
    std::fs::create_dir_all(export.path().join(&deep)).unwrap();
    for i in 0..5000 {
        std::fs::write(export.path().join(&deep).join(format!("{i}.bin")), b"1234").unwrap();
    }
    let (ours, theirs) = (velo().await, velo().await);
    ours.register_peer(theirs.peer_info()).unwrap();
    serve(&theirs, export.path());
    let state = state(ours);
    let b = NodeId::parse("b").unwrap();
    state.nodes.write().heard(b.clone(), theirs.instance_id());
    list_node(&state, &b, theirs.instance_id()).await.unwrap();
    let listed = state.index.read().list(deep.to_str().unwrap()).unwrap();
    assert_eq!(listed.len(), 5000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_that_came_up_is_listed_again_until_it_answers() {
    let export = export_of_one_file();
    let (ours, theirs) = (velo().await, velo().await);
    ours.register_peer(theirs.peer_info()).unwrap();
    let state = state(ours);
    let b = NodeId::parse("b").unwrap();
    state.nodes.write().heard(b.clone(), theirs.instance_id());
    // Up, and answering no listing yet: it serves its export only later.
    assert!(list_node(&state, &b, theirs.instance_id()).await.is_err());
    let late = tokio::spawn({
        let (theirs, root) = (theirs.clone(), export.path().to_owned());
        async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            serve(&theirs, &root);
        }
    });
    let listing = list_until_listed(Arc::clone(&state), b.clone(), theirs.instance_id());
    tokio::time::timeout(Duration::from_secs(20), listing)
        .await
        .expect("listed once it answers");
    late.await.unwrap();
    let Some(Entry::File(file)) = state.index.read().lookup("x.bin") else {
        panic!("b's file, once b answered")
    };
    assert_eq!(file.nodes, vec![b]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_gone_is_listed_no_more() {
    let (ours, theirs) = (velo().await, velo().await);
    ours.register_peer(theirs.peer_info()).unwrap();
    let state = state(ours);
    let b = NodeId::parse("b").unwrap();
    state.nodes.write().heard(b.clone(), theirs.instance_id());
    // It never answers, and goes.
    let listing = tokio::spawn(list_until_listed(
        Arc::clone(&state),
        b.clone(),
        theirs.instance_id(),
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    state.nodes.write().forget(&b);
    tokio::time::timeout(Duration::from_secs(10), listing)
        .await
        .expect("no more listings once it went")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_listing_of_a_node_that_went_or_restarted_is_dropped() {
    let export = export_of_one_file();
    let (ours, theirs) = (velo().await, velo().await);
    serve(&theirs, export.path());
    ours.register_peer(theirs.peer_info()).unwrap();
    let state = state(ours);
    let (b, answers) = (NodeId::parse("b").unwrap(), theirs.instance_id());
    // Restarted since: up as another instance than the one that answers.
    state.nodes.write().heard(b.clone(), InstanceId::new_v4());
    list_node(&state, &b, answers).await.unwrap();
    assert_eq!(state.index.read().file_count(), 0);
    // Gone since.
    state.nodes.write().forget(&b);
    list_node(&state, &b, answers).await.unwrap();
    assert_eq!(state.index.read().file_count(), 0);
    // Up as the instance that answers.
    state.nodes.write().heard(b.clone(), answers);
    list_node(&state, &b, answers).await.unwrap();
    assert_eq!(state.index.read().file_count(), 1);
}

#[test]
fn nats_is_told_back_when_connected_again_and_not_when_first_connected() {
    let back = Arc::new(Notify::new());
    let event = reconnects(Arc::clone(&back));
    drop(event(async_nats::Event::Connected));
    assert!(back.notified().now_or_never().is_none());
    drop(event(async_nats::Event::Disconnected));
    assert!(back.notified().now_or_never().is_none());
    drop(event(async_nats::Event::Connected));
    assert!(back.notified().now_or_never().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_file_nobody_announced_is_listed_once_nats_is_back() {
    let export = export_of_one_file();
    let state = state_of(velo().await, export.path().to_owned(), 0);
    let (back, cancel) = (Arc::new(Notify::new()), CancellationToken::new());
    // The next rescan by the clock is an hour away.
    let rescans = tokio::spawn(rescan(
        Arc::clone(&state),
        Duration::from_secs(3600),
        Arc::clone(&back),
        cancel.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(state.index.read().lookup("x.bin").is_none());
    back.notify_one();
    let listed = async {
        while state.index.read().lookup("x.bin").is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), listed)
        .await
        .expect("listed once NATS was back");
    cancel.cancel();
    rescans.await.unwrap().unwrap();
}

#[test]
fn what_waits_on_a_node_is_told_when_it_restarts_or_goes_and_not_at_a_heartbeat() {
    let mut nodes = Nodes::default();
    let b = NodeId::parse("b").unwrap();
    let (first, second) = (InstanceId::new_v4(), InstanceId::new_v4());
    assert_eq!(nodes.heard(b.clone(), first), Heard::New);
    let (instance, gone) = nodes.up(&b).unwrap();
    assert_eq!(nodes.heard(b.clone(), first), Heard::Again);
    assert!(instance == first && !gone.is_cancelled());
    assert_eq!(nodes.heard(b.clone(), second), Heard::Restarted(first));
    assert!(gone.is_cancelled());
    let (instance, gone) = nodes.up(&b).unwrap();
    assert!(instance == second && !gone.is_cancelled());
    nodes.forget(&b);
    assert!(gone.is_cancelled() && nodes.up(&b).is_none());
    // Silent for too long.
    nodes.heard(b.clone(), second);
    let (_, gone) = nodes.up(&b).unwrap();
    nodes.peers.get_mut(&b).unwrap().seen = Instant::now().checked_sub(2 * NODE_TTL).unwrap();
    assert_eq!(nodes.expire(), vec![(b.clone(), second)]);
    assert!(gone.is_cancelled());
}

/// `b`, up as an instance that never answers, is up again as `again`: what `watch_nodes`
/// does at the heartbeat that says so.
fn restarts(state: &State, b: &NodeId, again: &Velo) {
    state.velo.register_peer(again.peer_info()).unwrap();
    let heard = state.nodes.write().heard(b.clone(), again.instance_id());
    assert!(matches!(heard, Heard::Restarted(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_a_killed_node_never_answers_is_answered_by_the_node_once_it_is_up_again() {
    let export = export_of_one_file();
    let (ours, killed, again) = (velo().await, never_answering().await, velo().await);
    serve(&again, export.path());
    ours.register_peer(killed.peer_info()).unwrap();
    let state = state(ours);
    let b = NodeId::parse("b").unwrap();
    state.nodes.write().heard(b.clone(), killed.instance_id());
    let key = Key {
        node: b.clone(),
        path: RelPath::parse("x.bin").unwrap(),
    };
    let open = || {
        let (state, key) = (Arc::clone(&state), key.clone());
        tokio::spawn(async move { state.open_remote(&key).await })
    };
    let opening = open();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!opening.is_finished(), "it waits for its answer");
    restarts(&state, &b, &again);
    let opened = tokio::time::timeout(Duration::from_secs(10), opening)
        .await
        .expect("the node as it is up now answers")
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!((opened.instance, opened.size), (again.instance_id(), 4));
    // One that waits while the node goes for good ends then, and not a minute later.
    state.nodes.write().heard(b.clone(), killed.instance_id());
    let opening = open();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!opening.is_finished(), "it waits for its answer");
    state.nodes.write().forget(&b);
    let failed = tokio::time::timeout(Duration::from_secs(10), opening)
        .await
        .expect("ended when the node went")
        .unwrap();
    assert!(format!("{:#}", failed.unwrap_err()).contains("is not up"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_file_held_by_a_node_that_restarted_is_read_from_the_node_as_it_is_up_now() {
    let export = export_of_one_file();
    std::fs::write(export.path().join("y.bin"), b"56").unwrap();
    let (ours, killed, again) = (velo().await, never_answering().await, velo().await);
    serve(&again, export.path());
    ours.register_peer(killed.peer_info()).unwrap();
    let state = state(ours);
    let b = NodeId::parse("b").unwrap();
    state.nodes.write().heard(b.clone(), killed.instance_id());
    // Open on the instance that then never answers: its handles went with it.
    let held = |path, size| Remote {
        key: Key {
            node: b.clone(),
            path: RelPath::parse(path).unwrap(),
        },
        instance: killed.instance_id(),
        handle: 7,
        size,
    };
    // It restarts under the fetch.
    let reading = tokio::spawn({
        let (state, held) = (Arc::clone(&state), held("x.bin", 4));
        async move { state.remote_content(&held).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!reading.is_finished(), "it waits for its answer");
    restarts(&state, &b, &again);
    let read = tokio::time::timeout(Duration::from_secs(10), reading)
        .await
        .expect("the node as it is up now is asked")
        .unwrap()
        .unwrap();
    assert_eq!(&read[..], b"1234");
    // It restarted before the fetch.
    let read = tokio::time::timeout(
        Duration::from_secs(10),
        state.remote_content(&held("y.bin", 2)),
    )
    .await
    .expect("the instance that went is not asked")
    .unwrap();
    assert_eq!(&read[..], b"56");
    // What its successor does not have fails, and says whose.
    let gone = state.remote_content(&held("gone.bin", 1)).await;
    let said = format!("{:#}", gone.unwrap_err());
    assert!(said.contains("gone.bin of b") && said.contains("has it no more"));
}
