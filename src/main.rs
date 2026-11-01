//! Mount the union of the directory each machine of a group exports, read-only, with
//! files of other machines read into host memory when first touched. Run one on every
//! machine of the group, with the same NATS subject.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use clap::Parser;
use fractal_fuse::{MountOptions, Session};
use parking_lot::RwLock;
use prometheus::Registry;
use rview_fs::cache::ContentCache;
use rview_fs::fs::RviewFs;
use rview_fs::index::{Index, NodeId};
use rview_fs::state::State;
use rview_fs::{sync, transfer};
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use velo::observability::VeloMetrics;
use velo::transports::tcp::TcpTransportBuilder;
use velo::{ShutdownPolicy, Velo};

#[derive(Parser, Debug)]
#[command(about = "Mount the union of the directory each machine of a group exports, read-only")]
struct Args {
    /// The directory this machine exports.
    #[arg(long, env = "RVIEW_EXPORT")]
    export: PathBuf,
    /// Where to mount the union.
    #[arg(long, env = "RVIEW_MOUNT")]
    mount: PathBuf,
    /// The name this machine's files are announced under (default: host name).
    #[arg(long, env = "RVIEW_NODE_ID")]
    node_id: Option<String>,
    /// The NATS server files are announced on, which every machine's rview-fs heartbeats on.
    #[arg(long, env = "RVIEW_NATS")]
    nats: String,
    /// Files are announced on `{subject}.files`, and every rview-fs heartbeats on
    /// `{subject}.nodes`.
    #[arg(long, env = "RVIEW_SUBJECT", default_value = "rview")]
    subject: String,
    /// Host memory for the files of other machines, in GiB.
    #[arg(long, default_value_t = 16.0)]
    cache_gib: f64,
    /// Host memory this machine stages its files in for the others to read, in GiB. Past
    /// it, another machine cannot open a file of this one until stages are taken.
    #[arg(long, default_value_t = 16.0)]
    stage_gib: f64,
    /// Where velo listens for the other machines.
    #[arg(long, default_value = "0.0.0.0:0")]
    bind: SocketAddr,
    /// Threads that serve FUSE requests.
    #[arg(long, default_value_t = 8)]
    fuse_workers: usize,
    /// FUSE requests in flight per kernel queue. The kernel has a queue per possible CPU
    /// and every one gets this many 1 MiB buffers before the mount serves all CPUs.
    #[arg(long, default_value_t = 8)]
    fuse_queue_depth: u16,
    /// Move files between machines over RDMA (UCX).
    #[cfg(feature = "ucx")]
    #[arg(long)]
    rdma: bool,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let outcome = runtime.block_on(run(args));
    runtime.shutdown_timeout(Duration::from_secs(10));
    // The FUSE workers still wait in the kernel. Exiting is what ends that: the kernel
    // cancels the requests an exiting process waits on, which nothing else does.
    outcome
}

/// Detach the mount at `mount`, lazily: whoever still reads it may finish, and nothing
/// new finds it.
fn detach(mount: &Path) -> std::io::Result<std::process::ExitStatus> {
    std::process::Command::new("fusermount3")
        .arg("-u")
        .arg("-z")
        .arg(mount)
        .status()
}

async fn build_velo(args: &Args, metrics: Arc<VeloMetrics>) -> anyhow::Result<(Velo, bool)> {
    let listener =
        std::net::TcpListener::bind(args.bind).with_context(|| format!("binding {}", args.bind))?;
    let tcp = Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)?
            .build()?,
    );
    let builder = Velo::builder().add_transport(tcp).metrics(metrics);
    #[cfg(feature = "ucx")]
    if args.rdma {
        use velo::RdmaConfig;
        use velo::transports::ucx::UcxTransportBuilder;

        let ucx = Arc::new(UcxTransportBuilder::new().build()?);
        let staged = (args.stage_gib * f64::from(1u32 << 30)) as u64;
        let mut rdma = RdmaConfig::default();
        // A file is staged by copying it into registered memory. Pooled arenas are
        // registered once and reused; velo would give every file of 64 MiB or more an
        // arena of its own, registered for it and unmapped only by a later sweep, which
        // makes staging slow and soon spends the budget, past which it stages over TCP.
        rdma.pool.dedicated_arena_min = u64::MAX;
        // What transfer.rs stages at once, and the pool's rounding up of it (< 12.5%).
        rdma.pool.registered_bytes_budget = staged + staged / 8;
        rdma.pool.arena_reclaim_after = Some(Duration::from_secs(60));
        let velo = builder
            .add_ucx_transport(ucx)
            .rdma_config(rdma)
            .build()
            .await?;
        return Ok((velo, true));
    }
    Ok((builder.build().await?, false))
}

async fn run(args: Args) -> anyhow::Result<()> {
    let node = match &args.node_id {
        Some(node) => node.clone(),
        None => hostname()?,
    };
    let me = NodeId::parse(&node).with_context(|| format!("node id {node:?}"))?;
    let subject = args.subject.clone();
    std::fs::create_dir_all(&args.export)?;
    let registry = Registry::new();
    let (velo, pinned) = build_velo(&args, Arc::new(VeloMetrics::register(&registry)?)).await?;
    let state = Arc::new(State {
        me,
        export: args.export.clone(),
        index: RwLock::new(Index::new()),
        nodes: RwLock::default(),
        cache: ContentCache::new((args.cache_gib * f64::from(1u32 << 30)) as u64),
        velo: velo.clone(),
    });
    let nats = async_nats::connect(&args.nats)
        .await
        .with_context(|| format!("connecting to {}", args.nats))?;
    // Before the export is scanned, so that what is announced during the scan waits in
    // the subscription, and before the mount appears.
    let files = nats.subscribe(sync::files_subject(&subject)).await?;
    let nodes = nats.subscribe(sync::nodes_subject(&subject)).await?;
    nats.flush().await?;
    sync::scan_local(&state).await?;

    let cancel = CancellationToken::new();
    let mut tasks = JoinSet::new();
    let stages = Arc::new(transfer::Stages::new(
        (args.stage_gib * f64::from(1u32 << 30)) as u64,
    ));
    tasks.spawn(transfer::serve(
        velo.clone(),
        args.export.clone(),
        pinned,
        stages,
        cancel.clone(),
    ));
    tasks.spawn(sync::rescan(Arc::clone(&state), cancel.clone()));
    tasks.spawn(log_transfer_paths(registry, cancel.clone()));
    tasks.spawn(sync::watch_files(files, Arc::clone(&state), cancel.clone()));
    tasks.spawn(sync::watch_nodes(
        nats,
        subject.clone(),
        nodes,
        Arc::clone(&state),
        cancel.clone(),
    ));

    let options = MountOptions::new()
        .fs_name("rview-fs")
        .read_only(true)
        .allow_other(true)
        .default_permissions(true);
    let (mount, workers, depth) = (args.mount.clone(), args.fuse_workers, args.fuse_queue_depth);
    let session = tokio::task::spawn_blocking(move || {
        // An rview-fs that died left its mount behind, which answers nothing.
        if std::fs::metadata(&mount).is_err_and(|e| e.raw_os_error() == Some(libc::ENOTCONN)) {
            tracing::warn!(mount = %mount.display(), "detaching the mount of an rview-fs that died");
            detach(&mount)?;
        }
        std::fs::create_dir_all(&mount)?;
        Session::new(mount, options).map(|s| s.with_worker_count(workers).with_queue_depth(depth))
    })
    .await?
    .with_context(|| format!("mounting {}", args.mount.display()))?;
    let fs = RviewFs::new(Arc::clone(&state), tokio::runtime::Handle::current());
    let (ended_tx, ended) = tokio::sync::oneshot::channel();
    let fuse = std::thread::Builder::new()
        .name("rview-fuse".into())
        .spawn(move || {
            // The other end is gone when this is dropped, which select! below notices.
            let _ = ended_tx.send(session.run(fs));
        })?;
    tracing::info!(mount = %args.mount.display(), node = state.me.as_str(), %subject, "mounted");

    let mut sigterm = signal(SignalKind::terminate())?;
    let outcome = tokio::select! {
        _ = sigterm.recv() => Ok(()),
        _ = tokio::signal::ctrl_c() => Ok(()),
        ended = ended => match ended {
            Ok(Ok(())) => Err(anyhow::anyhow!("the mount was unmounted")),
            Ok(Err(e)) => Err(anyhow::Error::from(e).context("serving the mount failed")),
            Err(_) => Err(anyhow::anyhow!("the FUSE thread died")),
        },
        Some(done) = tasks.join_next() => match done {
            Ok(Ok(())) => Err(anyhow::anyhow!("a background task ended")),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(e.into()),
        },
    };
    // Not the session's own shutdown: it cancels the requests its workers wait on, which
    // the kernel never completes, and the workers then wait for them forever.
    tracing::info!("unmounting");
    let mount = args.mount.clone();
    let detached = tokio::task::spawn_blocking(move || detach(&mount)).await?;
    if !detached
        .as_ref()
        .is_ok_and(std::process::ExitStatus::success)
    {
        tracing::warn!(status = ?detached, "unmounting {} failed", args.mount.display());
    }
    cancel.cancel();
    while let Some(done) = tasks.join_next().await {
        if let Ok(Err(e)) = done {
            tracing::warn!("a task failed while stopping: {e:#}");
        }
    }
    velo.shutdown(ShutdownPolicy::Timeout(Duration::from_secs(10)))
        .await;
    // Not joined: its workers end with the process (see main).
    drop(fuse);
    outcome
}

/// How files moved between machines, RDMA or chunks over TCP, and why: logged when it
/// changes. A transfer that works says nothing about the path it took.
async fn log_transfer_paths(registry: Registry, cancel: CancellationToken) -> anyhow::Result<()> {
    let mut last = Vec::new();
    let mut tick = tokio::time::interval(Duration::from_secs(60));
    loop {
        let stopping = tokio::select! {
            () = cancel.cancelled() => true,
            _ = tick.tick() => false,
        };
        let paths: Vec<(String, u64)> = registry
            .gather()
            .iter()
            .filter(|family| family.name() == "velo_rendezvous_rdma_path_total")
            .flat_map(|family| family.get_metric())
            .map(|metric| {
                let labels: Vec<&str> =
                    metric.get_label().iter().map(|pair| pair.value()).collect();
                (labels.join("/"), metric.get_counter().value() as u64)
            })
            .filter(|(_, count)| *count > 0)
            .collect();
        if paths != last {
            tracing::info!(?paths, "file transfers by path");
            last = paths;
        }
        if stopping {
            return Ok(());
        }
    }
}

fn hostname() -> anyhow::Result<String> {
    let name =
        std::fs::read_to_string("/proc/sys/kernel/hostname").context("reading the host name")?;
    Ok(name.trim().to_owned())
}
