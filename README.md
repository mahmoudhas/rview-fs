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
  machine's export when the machine shows up and every 10 minutes.
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
2000 files of 4 KiB, and 8 readers in the parallel stages.

Files between the machines over TCP, without passthrough and with:

| stage | MB/op | p50 ms, without | p50 ms, with | GB/s, without | GB/s, with |
| --- | ---: | ---: | ---: | ---: | ---: |
| announced file shown by another machine | – | 0.0475 | 0.0532 | – | – |
| stat, first (another machine's 2000 small files) | – | 0.0291 | 0.0219 | – | – |
| stat, again (another machine's 2000 small files) | – | 0.00628 | 0.00605 | – | – |
| list a folder of 2000 files | – | 3.46 | 3.09 | – | – |
| open, local disk, not rview-fs | – | 0.00655 | 0.00615 | – | – |
| open, this machine's file | – | 0.0571 | 0.0407 | – | – |
| open, another machine's 64 MiB file, not read yet | – | 0.254 | 80 | – | – |
| read, local disk, not rview-fs, cold | 67.1 | 30.3 | 27 | 2.21 | 2.49 |
| read, local disk, not rview-fs, cached | 67.1 | 10.5 | 9.29 | 6.51 | 7.39 |
| read, this machine's file, cold | 67.1 | 60.8 | 26.7 | 1.11 | 2.55 |
| read, this machine's file, cached | 67.1 | 12.2 | 9.49 | 5.83 | 7.3 |
| read, another machine's file, fetched | 67.1 | 113 | 87 | 0.585 | 0.772 |
| read, another machine's file, first MiB (fetches the file) | 1.05 | 78 | 0.181 | 0.0134 | 5.85 |
| read, another machine's file, cached | 67.1 | 11.6 | 10.3 | 5.67 | 6.54 |
| read, another machine's file, from rview-fs memory | 67.1 | 35.4 | 10.3 | 1.89 | 6.47 |
| open, another machine's file, in rview-fs memory | – | 0.0218 | 0.0193 | – | – |
| 4 KiB random read, local disk, not rview-fs, cold | 0.0041 | 0.0673 | 0.0669 | 0.0602 | 0.0604 |
| 4 KiB random read, this machine's file, cold | 0.0041 | 0.11 | 0.0671 | 0.0382 | 0.061 |
| 4 KiB random read, another machine's file, from rview-fs memory | 0.0041 | 0.0259 | 0.00133 | 0.158 | 3.02 |
| map and fault in, this machine's file, cold | 67.1 | 74.6 | 21.9 | 0.909 | 3.03 |
| map and fault in, another machine's file, fetched | 67.1 | 109 | 79.1 | 0.62 | 0.851 |
| map, then touch a random page, another machine's file, fetched on the first | 0.0041 | 0.128 | 0.00251 | 0.00312 | 0.00335 |
| read, another machine's files, fetched, 8 at once | 67.1 | 413 | 375 | 1.28 | 1.41 |
| read, another machine's files, from rview-fs memory, 8 at once | 67.1 | 41.2 | 13 | 8.9 | 33.5 |
| read, this machine's files, cold, 8 at once | 67.1 | 167 | 33.1 | 2.94 | 15.1 |

Over RDMA (one RoCE port, `mlx5_0`, looped back to itself), with passthrough, the stages
that move the other machine's files:

| stage | ops | MB/op | ops/s | GB/s | p50 ms | p90 ms | max ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| open, another machine's 64 MiB file, not read yet | 8 | – | 10.8 | – | 79.2 | 80 | 187 |
| read, another machine's file, fetched | 8 | 67.1 | 12 | 0.803 | 83.3 | 84 | 84.9 |
| read, another machine's file, first MiB (fetches the file) | 8 | 1.05 | 4,813 | 5.05 | 0.202 | 0.218 | 0.265 |
| read, another machine's file, cached | 8 | 67.1 | 74.5 | 5 | 13.4 | 13.5 | 13.5 |
| read, another machine's file, from rview-fs memory | 8 | 67.1 | 76.6 | 5.14 | 13.2 | 13.2 | 13.2 |
| open, another machine's file, in rview-fs memory | 8 | – | 40,421 | – | 0.0244 | 0.0247 | 0.0304 |
| 4 KiB random read, another machine's file, from rview-fs memory | 4000 | 0.0041 | 601,954 | 2.47 | 0.00165 | 0.00198 | 0.00864 |
| map and fault in, another machine's file, fetched | 8 | 67.1 | 12.8 | 0.859 | 76 | 77.4 | 97.7 |
| map, then touch a random page, another machine's file, fetched on the first | 512 | 0.0041 | 880 | 0.0036 | 0.00341 | 0.00395 | 73.2 |
| read, another machine's files, fetched, 8 at once | 16 | 67.1 | 28.4 | 1.91 | 161 | 454 | 486 |
| read, another machine's files, from rview-fs memory, 8 at once | 16 | 67.1 | 520 | 34.9 | 13.9 | 14.5 | 15.3 |

What it shows:

- **Metadata:** a file another machine announces shows up about 50 µs later. A `stat`
  takes 22–29 µs the first time and 6 µs while the kernel keeps the answer (1 s). Listing
  a folder of 2000 files takes 3–3.5 ms.
- **This machine's files, passed through,** read as fast as the disk read directly: 2.5
  GB/s cold for one reader, 15 GB/s for 8, and a random 4 KiB read in 67 µs. Served by
  rview-fs instead, they read at 1.1 GB/s, 2.9 GB/s for 8 readers, and 110 µs.
- **Another machine's file** is fetched when it is opened: 80 ms for 64 MiB, over TCP or
  RDMA alike. After that, passed through, it reads like memory: 6.5 GB/s for one reader,
  34 GB/s for 8, and a random 4 KiB read in about 1.5 µs, where serving it through
  rview-fs gives 1.9 GB/s, 8.9 GB/s, and 26 µs.
- **The fetch is what is left to make faster:** a 64 MiB file moves at about 0.8 GB/s
  between two rview-fs on one machine, over TCP or RDMA; 1.4–1.9 GB/s with 8 at once.

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
