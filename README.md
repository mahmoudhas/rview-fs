# rview-fs

A read-only FUSE file system that shows every machine of a group the union of the
directory each of them exports. A file another machine holds is read into host memory
over RDMA, whole, when it is first read, and never copied to a local disk.

It suits files that machines write once and read many times, each machine writing its own
and reading everyone's: no shared storage, no copying every file everywhere, and a
machine's own files read from its own disk.

It runs on Linux 6.14. It mounts with [fractal-fuse](https://github.com/fractalbits-labs/fractal-fuse)
(FUSE over io_uring), has the kernel read files itself (FUSE passthrough), moves files
with [velo](https://github.com/ai-dynamo/velo) over RDMA, and learns of the machines and
their files on [NATS](https://nats.io).

## How it works

- Every machine runs one rview-fs, which exports a directory (`--export`) and mounts the
  union of every machine's export (`--mount`).
- Whatever writes into an export announces each file it adds or deletes on NATS. Each
  rview-fs keeps an index of every machine's files from those announcements, and lists a
  machine's export when the machine shows up, again until it answers, and every 10
  minutes.
- Opening a file of this machine hands the export's file to the kernel, which reads and
  maps it with no request to rview-fs. All the opens of a file share one backing file,
  the one the kernel allows a file at a time.
- Opening a file of another machine has that machine open it too, and hold it open until
  this one closes it: a file it deletes meanwhile is still read whole. Nothing moves when
  the file is opened. The first read fetches all of it into host memory, a mapped file's
  first page fault included, and rview-fs serves its reads from there; the kernel keeps
  what it read. A file opened and never read is never fetched. An open file keeps its
  bytes until closed, and a mapped one until unmapped, so a page the kernel drops is read
  again without the other machine.
- `--cache-gib` keeps other machines' files read before, which then open without asking
  their machine, least recently used first out; `--stage-gib` bounds what a machine stages
  for the others while they read its files.

### Files open on another machine

A machine holds a file open for another from the other's open to its close, as an
`Arc<File>`: a read of it still going keeps it until done. Nothing the other fails to say
leaves it open:

- Every 10 s each machine lists the files it has open to each machine holding some. A file
  not on the list is closed, unless it was opened within the last 120 s, when its open may
  not have answered yet: a close that never arrived, or an open that gave up.
- A machine silent for 60 s has all of its files closed.
- rview-fs raises its open-file limit to the most it may (at most 2^20), and holds at most
  half of it open for the others; an open past that fails, and says why.

### Machines that go down and come back up

A machine that comes up, new or back, heartbeats at once. Every rview-fs that hears it
lists its export whole, and heartbeats back so that it lists theirs: each gets the files
the other had already, not only those announced since.

- A listing that fails, of a machine not reachable yet or too busy to answer, is made
  again 1 s later, and twice as long after each time since, up to 30 s, until the machine
  answers, goes, or restarts. rview-fs answers listings before it first heartbeats.
- A machine that restarts within 30 s keeps the files it had until its listing replaces
  them; one silent for 30 s is gone, and so are its files, until it comes back.
- A listing that answers after its machine went or restarted is dropped: it brings back no
  files of a machine gone.
- A machine that heartbeats an address velo cannot use is left out, and said so at each
  heartbeat, until it heartbeats one velo can.

## What it needs, and what happens without

| | Needs | Without it |
| --- | --- | --- |
| FUSE over io_uring | Linux 6.14 built with `CONFIG_FUSE_IO_URING` | rview-fs does not start: fractal-fuse has no other way. rview-fs turns the kernel's switch on (`/sys/module/fuse/parameters/enable_uring`) when it is off |
| Passthrough | Linux 6.9 built with `CONFIG_FUSE_PASSTHROUGH`, `CAP_SYS_ADMIN`, and the export on a plain file system: not overlayfs | rview-fs serves the reads of this machine's files itself, several times slower. It serves the files of other machines either way |
| RDMA | A build with `--features ucx` (the default), rdma-core's libraries, and an RDMA NIC; on RoCE, the host's network namespace, and with several NICs, UCX on one port: `UCX_NET_DEVICES=mlx5_0:1 UCX_TLS=rc_mlx5,ud_mlx5,self` | Files move between machines over TCP |

What is missing is logged as an error, `DEGRADED: ... is not available: <why>`, when
rview-fs finds it, and again every 10 minutes. So is a transfer over TCP while RDMA is on,
and an RDMA transfer that never completes. `--no-passthrough` and `--no-rdma` do without
them on purpose, and say nothing.

## Exports

Whatever writes into an export follows three rules:

- Nothing is shared before it is whole. A name that starts with `.`, of a file or of a
  folder above it, is temporary: rview-fs never lists it, takes it from an announcement
  or serves it. Write a file under a temporary name (`.data.bin.k3j.tmp`), then link or
  rename it to its shared name once it is complete; it never changes after that.
- Announce every file that becomes visible or goes away, on NATS (below). rview-fs also
  lists every machine when it shows up and every 10 minutes, so an announcement lost on
  the way only makes a file late.
- A machine that holds a file at some path holds the same bytes there as every other
  machine that holds one.

A file another machine opens has its modification time renewed, so a cleanup that deletes
the least recently used files keeps the ones other machines use.

## NATS

Two subjects under `--subject`:

| Subject | Who sends | What |
| --- | --- | --- |
| `{subject}.files` | whatever writes into an export | `{"node": ..., "updates": [{"op": "add" \| "del", "path": ..., "size": ..., "mtime": ...}, ...]}` |
| `{subject}.nodes` | every rview-fs, every 5 s | its node, and how velo reaches it |

`node` is the name of the machine whose export holds the files: its rview-fs's
`--node-id`. Paths are relative to the export. A machine silent on `{subject}.nodes` for
30 s is gone, and so are its files.

## Running

```bash
UCX_NET_DEVICES=mlx5_0:1 UCX_TLS=rc_mlx5,ud_mlx5,self \
  rview-fs --export /data/export --mount /mnt/union --nats nats://nats:4222 \
  --subject myapp [--node-id $(hostname)]
```

| Flag (environment variable) | What it does |
| --- | --- |
| `--export` (`RVIEW_EXPORT`) | The directory this machine exports |
| `--mount` (`RVIEW_MOUNT`) | Where to mount the union; a mount left by an rview-fs that died is detached first |
| `--node-id` (`RVIEW_NODE_ID`) | The name this machine's files are announced under (host name) |
| `--nats` (`RVIEW_NATS`) | The NATS server |
| `--subject` (`RVIEW_SUBJECT`) | The subjects' prefix (`rview`) |
| `--cache-gib` | Host memory for the files of other machines read before (16) |
| `--stage-gib` | Host memory this machine stages its files in while the others read them (16); past it, they cannot read its files until stages are taken |
| `--bind` | Where velo listens for the other machines (`0.0.0.0:0`) |
| `--fuse-workers`, `--fuse-queue-depth` | Threads serving FUSE requests (8), and requests in flight per kernel queue (8) |
| `--no-passthrough` | Serve the reads of this machine's files too |
| `--no-rdma` | Move files between machines over TCP |

It needs root, or `CAP_SYS_ADMIN`, `/dev/fuse` and `fusermount3`. On SIGTERM it detaches
the mount and exits. It logs how its transfers went: `rdma/ok`, or why a file moved over
TCP.

## Benchmarks

`bench/bench.py` plays two machines on one host: two rview-fs, each with its own export
and mount, and a NATS server. It writes files into the exports, announces them, and reads
them through the first machine's mount: that machine's files and the other's. Each stage
reports ops, ops/s, GB/s, and latency percentiles; a stage that reads cold drops the page
cache of the files first. At the end it checks that every file read through the mounts is
the file in the export, and prints what rview-fs logged as a warning or an error.

```bash
cargo build --release
python3 bench/bench.py            # files between the machines over TCP
UCX_NET_DEVICES=mlx5_0:1 UCX_TLS=rc_mlx5,ud_mlx5,self python3 bench/bench.py --rdma
```

It needs `nats-server`, FUSE, and Python 3.10 or later; nothing outside its standard
library. `--rview-fs-args` passes more arguments to both rview-fs: `--no-passthrough`,
say, or `"--cache-gib 1"` to measure fetches that reuse memory the cache freed.

The results below are medians of 3 runs on one machine with Linux 6.14 (an OCI GPU node,
256 CPUs), its exports on the machine's NVMe RAID (ext4); 8 files of 64 MiB in each set,
2000 files of 4 KiB, and 8 readers in the parallel stages. Files moved between the two
rview-fs over RDMA (one RoCE port, `mlx5_0`, looped back to itself): every one of them
`rdma/ok` but a 4 KiB file below RDMA's minimum. Without passthrough and with:

| stage | MB/op | p50 ms, without | p50 ms, with | GB/s, without | GB/s, with |
| --- | ---: | ---: | ---: | ---: | ---: |
| announced file shown by another machine | – | 0.0468 | 0.048 | – | – |
| stat, first (another machine's 2000 small files) | – | 0.0303 | 0.0316 | – | – |
| stat, again (another machine's 2000 small files) | – | 0.00611 | 0.00627 | – | – |
| list a folder of 2000 files | – | 3.51 | 3.57 | – | – |
| open, local disk, not rview-fs | – | 0.00679 | 0.00662 | – | – |
| open, this machine's file | – | 0.0554 | 0.0641 | – | – |
| open, another machine's 64 MiB file, not read yet | – | 0.234 | 0.202 | – | – |
| read, local disk, not rview-fs, cold | 67.1 | 32.4 | 31.4 | 2.16 | 2.14 |
| read, local disk, not rview-fs, cached | 67.1 | 10.8 | 10.7 | 6.52 | 6.37 |
| read, this machine's file, cold | 67.1 | 67.5 | 30.4 | 0.989 | 2.29 |
| read, this machine's file, cached | 67.1 | 12.4 | 10.9 | 5.7 | 6.32 |
| read, another machine's file, fetched | 67.1 | 84.6 | 87.2 | 0.693 | 0.664 |
| read, another machine's file, first MiB (fetches the file) | 1.05 | 49.2 | 50.1 | 0.0167 | 0.0164 |
| read, another machine's file, cached | 67.1 | 13.2 | 13.5 | 5.17 | 5.03 |
| read, another machine's file, from rview-fs memory | 67.1 | 35.4 | 36.2 | 1.9 | 1.84 |
| open, another machine's file, in rview-fs memory | – | 0.021 | 0.0208 | – | – |
| 4 KiB random read, local disk, not rview-fs, cold | 0.0041 | 0.0673 | 0.0673 | 0.06 | 0.0601 |
| 4 KiB random read, this machine's file, cold | 0.0041 | 0.112 | 0.0674 | 0.0374 | 0.0606 |
| 4 KiB random read, another machine's file, from rview-fs memory | 0.0041 | 0.0256 | 0.027 | 0.159 | 0.15 |
| map and fault in, this machine's file, cold | 67.1 | 72.6 | 23.4 | 0.981 | 2.92 |
| map and fault in, another machine's file, fetched | 67.1 | 78.8 | 78.2 | 0.848 | 0.863 |
| map, then touch a random page, another machine's file, fetched on the first | 0.0041 | 0.131 | 0.13 | 0.00463 | 0.00457 |
| read, another machine's files, fetched, 8 at once | 67.1 | 162 | 165 | 2 | 2.1 |
| read, another machine's files, from rview-fs memory, 8 at once | 67.1 | 38.5 | 40.6 | 9.41 | 8.12 |
| read, this machine's files, cold, 8 at once | 67.1 | 133 | 34.3 | 3.81 | 14.3 |

What it shows:

- **Metadata:** a file another machine announces shows up about 50 µs later. A `stat`
  takes about 30 µs the first time and 6 µs while the kernel keeps the answer (1 s).
  Listing a folder of 2000 files takes 3.5 ms.
- **This machine's files, passed through,** read as fast as the disk read directly: 2.3
  GB/s cold for one reader, 14 GB/s for 8, and a random 4 KiB read in 67 µs. Served by
  rview-fs instead, they read at 1 GB/s, 3.8 GB/s for 8 readers, and 112 µs.
- **Another machine's file** opens in 0.2 ms: it is held open there, and nothing moves.
  Its first read fetches all of it, 50 ms for 64 MiB, and a file never read is never
  fetched. rview-fs then serves it from memory: 1.9 GB/s for one reader, 8–9 GB/s for 8,
  and a random 4 KiB read in 26 µs; what the kernel keeps reads at 5 GB/s.
- **Fetching is what is left to make faster:** a 64 MiB file moves at about 0.7–0.9 GB/s
  between two rview-fs on one machine, about 2 GB/s with 8 at once.

## Building and testing

```bash
cargo build --release                         # RDMA too: needs libibverbs-dev and librdmacm-dev
cargo build --release --no-default-features   # TCP only
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
```

A build with RDMA runs with rdma-core's libraries: `libibverbs1`, `librdmacm1` and
`ibverbs-providers` on Debian and Ubuntu.

## Known issues

- On Linux 6.6 with FUSE over io_uring backported, an rview-fs that exited on SIGTERM
  while a FUSE request was in flight once left its kernel connection open. A process
  waiting on that request hung until the connection was aborted. To abort it, find the
  mount's device in `/proc/self/mountinfo` (the third field, `0:N`) and run
  `echo 1 > /sys/fs/fuse/connections/N/abort`. An rview-fs killed with SIGKILL aborts its
  connection cleanly.
- On a machine with several RDMA NICs, UCX left to choose devices on its own can make
  transfers between two rview-fs never complete: pin it to one port (`UCX_NET_DEVICES`).
  rview-fs gives up on a transfer after 60 s and says so as `DEGRADED`.
