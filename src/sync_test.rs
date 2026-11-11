use std::path::{Path, PathBuf};

use parking_lot::RwLock;
use velo::Velo;
use velo::transports::tcp::TcpTransportBuilder;

use super::*;
use crate::cache::ContentCache;
use crate::cache::Key;
use crate::held::Held;
use crate::index::{Entry, EntryKind};
use crate::remote::{Remote, RemoteFile, RemoteFiles, Stalls};

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

/// A node's velo, over TCP on this host.
async fn velo() -> Velo {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let tcp = TcpTransportBuilder::new()
        .from_listener(listener)
        .unwrap()
        .build()
        .unwrap();
    Velo::builder()
        .add_transport(Arc::new(tcp))
        .build()
        .await
        .unwrap()
}

/// What this node knows, which reaches the others with `velo`.
fn state(velo: Velo) -> Arc<State> {
    Arc::new(State {
        me: NodeId::parse("a").unwrap(),
        export: PathBuf::from("/nonexistent"),
        index: RwLock::new(Index::new()),
        nodes: RwLock::default(),
        cache: ContentCache::new(0),
        remote: RemoteFiles::default(),
        velo,
        rdma: false,
        stalls: Stalls::default(),
    })
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
        let read = transfer::fetch(&state.velo, file.remote.instance, file.remote.handle, rdma);
        assert_eq!(&read.await.unwrap()[..], b"1234");
    }
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
