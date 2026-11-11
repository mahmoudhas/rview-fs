//! Mount the union of the directory each machine of a group exports, read-only, with
//! files of other machines read into host memory when first read. Run one on every
//! machine of the group, with the same NATS subject.
//!
//! It runs on Linux 6.14: FUSE over io_uring, which it turns on, passthrough, and RDMA.
//! Without passthrough or RDMA it goes on, slower, and says so loudly (health.rs).

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
use rview_fs::health::{self, Degraded};
use rview_fs::held::Held;
use rview_fs::index::{Index, NodeId};
use rview_fs::passthrough::Passthrough;
use rview_fs::remote::{RemoteFiles, Stalls};
use rview_fs::state::State;
use rview_fs::{metrics, remote, sync, transfer};
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use velo::observability::VeloMetrics;
use velo::transports::tcp::TcpTransportBuilder;
use velo::{ShutdownPolicy, Velo, VeloBuilder};

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
    /// Host memory for the files of other machines read before, in GiB.
    #[arg(long, default_value_t = 16.0)]
    cache_gib: f64,
    /// Host memory this machine stages its files in while other machines read them, in
    /// GiB. Past it, another machine cannot read a file of this one until stages are taken.
    #[arg(long, default_value_t = 16.0)]
    stage_gib: f64,
    /// Serve every read itself, instead of having the kernel read files directly (FUSE
    /// passthrough).
    #[arg(long)]
    no_passthrough: bool,
    /// Move files between machines over TCP, not RDMA.
    #[arg(long)]
    no_rdma: bool,
    /// Where velo listens for the other machines.
    #[arg(long, default_value = "0.0.0.0:0")]
    bind: SocketAddr,
    /// Where to serve Prometheus metrics, velo's and rview-fs's own (off by default).
    #[arg(long, env = "RVIEW_METRICS")]
    metrics: Option<SocketAddr>,
    /// Threads that serve FUSE requests.
    #[arg(long, default_value_t = 8)]
    fuse_workers: usize,
    /// FUSE requests in flight per kernel queue. The kernel has a queue per possible CPU
    /// and every one gets this many 1 MiB buffers before the mount serves all CPUs.
    #[arg(long, default_value_t = 8)]
    fuse_queue_depth: u16,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            // fractal-fuse warns of every request it has no handler for, and programs make
            // one on every open (Python asks isatty), which the kernel passes on.
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,fractal_fuse::dispatch=error".into()),
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

/// FUSE over io_uring, the only way fractal-fuse serves a mount: Linux 6.14 has it, off
/// unless this parameter is turned on.
const ENABLE_URING: &str = "/sys/module/fuse/parameters/enable_uring";

fn turn_on_fuse_over_io_uring() -> anyhow::Result<()> {
    let was = match std::fs::read_to_string(ENABLE_URING) {
        Ok(was) => was,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => anyhow::bail!(
            "this kernel has no FUSE over io_uring ({ENABLE_URING} is missing): rview-fs needs \
             Linux 6.14 or later built with CONFIG_FUSE_IO_URING"
        ),
        Err(e) => return Err(e).with_context(|| format!("reading {ENABLE_URING}")),
    };
    if was.trim() == "Y" {
        return Ok(());
    }
    std::fs::write(ENABLE_URING, "Y").with_context(|| {
        format!(
            "FUSE over io_uring is off, and turning it on failed: as root, echo Y > {ENABLE_URING}"
        )
    })?;
    tracing::info!(
        "turned on FUSE over io_uring ({ENABLE_URING} was {})",
        was.trim()
    );
    Ok(())
}

/// The most files rview-fs holds open for other machines: half of its open-file limit,
/// raised to the most it may be, and the other half for everything else.
fn files_to_hold() -> anyhow::Result<usize> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit writes the struct it is given, which lives for the call.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(std::io::Error::last_os_error()).context("reading the open-file limit");
    }
    // Past fs.nr_open (2^20 by default) the kernel refuses any limit.
    let most = limit.rlim_max.min(1 << 20);
    if limit.rlim_cur < most {
        let raised = libc::rlimit {
            rlim_cur: most,
            rlim_max: limit.rlim_max,
        };
        // SAFETY: setrlimit reads the struct it is given, which lives for the call.
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) } == 0 {
            limit.rlim_cur = most;
        } else {
            tracing::warn!(
                e = %std::io::Error::last_os_error(),
                "raising the open-file limit failed"
            );
        }
    }
    let held = usize::try_from(limit.rlim_cur / 2).unwrap_or(usize::MAX);
    tracing::info!(
        open_files = limit.rlim_cur,
        held,
        "files held open for other machines at most"
    );
    Ok(held)
}

fn tcp_velo(args: &Args, metrics: Arc<VeloMetrics>) -> anyhow::Result<VeloBuilder> {
    let listener =
        std::net::TcpListener::bind(args.bind).with_context(|| format!("binding {}", args.bind))?;
    let tcp = Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)?
            .build()?,
    );
    Ok(Velo::builder().add_transport(tcp).metrics(metrics))
}

#[cfg(feature = "ucx")]
async fn rdma_velo(args: &Args, metrics: Arc<VeloMetrics>) -> anyhow::Result<Velo> {
    use velo::RdmaConfig;
    use velo::transports::ucx::UcxTransportBuilder;

    let ucx = Arc::new(UcxTransportBuilder::new().build()?);
    let staged = (args.stage_gib * f64::from(1u32 << 30)) as u64;
    let mut rdma = RdmaConfig::default();
    // A file being read is staged by copying it into registered memory. Pooled arenas
    // are registered once and reused; velo would give every file of 64 MiB or more an
    // arena of its own, registered for it and unmapped only by a later sweep, which
    // makes staging slow and soon spends the budget, past which it stages over TCP.
    rdma.pool.dedicated_arena_min = u64::MAX;
    // What transfer.rs stages at once, and the pool's rounding up of it (< 12.5%).
    rdma.pool.registered_bytes_budget = staged + staged / 8;
    rdma.pool.arena_reclaim_after = Some(Duration::from_secs(60));
    tcp_velo(args, metrics)?
        .add_ucx_transport(ucx)
        .rdma_config(rdma)
        .build()
        .await
}

/// Velo, and whether it moves files over RDMA: unless `--no-rdma`, it does when it can.
async fn build_velo(
    args: &Args,
    metrics: Arc<VeloMetrics>,
    degraded: &Degraded,
) -> anyhow::Result<(Velo, bool)> {
    if !args.no_rdma {
        #[cfg(feature = "ucx")]
        match rdma_velo(args, Arc::clone(&metrics)).await {
            Ok(velo) => return Ok((velo, true)),
            Err(e) => degraded.report(
                "RDMA",
                format!("{e:#}: files move between machines over TCP, several times slower"),
            ),
        }
        #[cfg(not(feature = "ucx"))]
        degraded.report(
            "RDMA",
            "this rview-fs is built without it (--features ucx): files move between machines \
             over TCP, several times slower"
                .into(),
        );
    }
    Ok((tcp_velo(args, metrics)?.build().await?, false))
}

/// Whether the kernel passes files through, tried once the mount answers: no request is
/// answered before the kernel takes the reply to INIT, which is when it grants it.
async fn probe_passthrough(mount: PathBuf, passthrough: Arc<Passthrough>) {
    let up = tokio::task::spawn_blocking(move || std::fs::metadata(&mount));
    match tokio::time::timeout(Duration::from_secs(30), up).await {
        Ok(Ok(Ok(_))) => passthrough.probe(),
        failed => tracing::error!(?failed, "the mount did not answer: passthrough is untried"),
    }
}

async fn run(args: Args) -> anyhow::Result<()> {
    let node = match &args.node_id {
        Some(node) => node.clone(),
        None => hostname()?,
    };
    let me = NodeId::parse(&node).with_context(|| format!("node id {node:?}"))?;
    let subject = args.subject.clone();
    std::fs::create_dir_all(&args.export)?;
    // No fallback: fractal-fuse serves a mount over io_uring or not at all.
    if let Err(e) = turn_on_fuse_over_io_uring() {
        tracing::error!("rview-fs cannot mount: {e:#}");
        return Err(e);
    }
    let held = Arc::new(Held::new(files_to_hold()?));
    let degraded = Arc::new(Degraded::default());
    let registry = Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry)?);
    let read_bytes = transfer::ReadBytes::default();
    read_bytes.register(&registry)?;
    let stalls = Stalls::default();
    stalls.register(&registry)?;
    let (velo, pinned) = build_velo(&args, metrics, &degraded).await?;
    let state = Arc::new(State {
        me,
        export: args.export.clone(),
        index: RwLock::new(Index::new()),
        nodes: RwLock::default(),
        cache: ContentCache::new((args.cache_gib * f64::from(1u32 << 30)) as u64),
        remote: RemoteFiles::default(),
        velo: velo.clone(),
        rdma: pinned,
        stalls,
        read_bytes,
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
    // Before the first heartbeat (watch_nodes): a node that hears it lists this one at
    // once, and a listing that finds no handler fails.
    transfer::serve(
        &velo,
        args.export.clone(),
        pinned,
        Arc::clone(&stages),
        Arc::clone(&held),
    )?;
    tasks.spawn(transfer::reap(velo.clone(), stages, held, cancel.clone()));
    tasks.spawn(remote::renew(Arc::clone(&state), cancel.clone()));
    tasks.spawn(sync::rescan(Arc::clone(&state), cancel.clone()));
    if let Some(addr) = args.metrics {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("binding {addr} for metrics"))?;
        tasks.spawn(metrics::serve(listener, registry.clone(), cancel.clone()));
    }
    tasks.spawn(log_transfer_paths(
        registry,
        pinned,
        Arc::clone(&degraded),
        cancel.clone(),
    ));
    let reminding = Arc::clone(&degraded);
    let stop_reminding = cancel.clone();
    tasks.spawn(async move { reminding.remind(stop_reminding).await });
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
        .default_permissions(true)
        .passthrough(!args.no_passthrough);
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
    let passthrough = (!args.no_passthrough)
        .then(|| Arc::new(Passthrough::new(session.fuse_fd(), Arc::clone(&degraded))));
    let fs = RviewFs::new(
        Arc::clone(&state),
        tokio::runtime::Handle::current(),
        passthrough.clone(),
    );
    let (ended_tx, ended) = tokio::sync::oneshot::channel();
    let fuse = std::thread::Builder::new()
        .name("rview-fuse".into())
        .spawn(move || {
            // The other end is gone when this is dropped, which select! below notices.
            let _ = ended_tx.send(session.run(fs));
        })?;
    tracing::info!(mount = %args.mount.display(), node = state.me.as_str(), %subject, "mounted");
    if let Some(passthrough) = passthrough {
        tokio::spawn(probe_passthrough(args.mount.clone(), passthrough));
    }

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
/// changes, as an error when `rdma` is on and files moved over TCP all the same. A
/// transfer that works says nothing about the path it took.
async fn log_transfer_paths(
    registry: Registry,
    rdma: bool,
    degraded: Arc<Degraded>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
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
            let fallbacks = health::tcp_fallbacks(&paths);
            if rdma && !fallbacks.is_empty() {
                degraded.report(
                    "RDMA for every file",
                    format!("files moved over TCP although RDMA is on, by why: {fallbacks:?}"),
                );
            }
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
