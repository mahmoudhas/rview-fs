# rview-fs

A read-only FUSE file system that shows every machine of a group the union of the
directory each of them exports. A file another machine holds is fetched into host memory
the first time any byte of it is read, over RDMA when built with `--features ucx` and run
with `--rdma`, and is never copied to a local disk. An open file keeps those bytes until it
is closed, and a mapped one until it is unmapped: a page the kernel drops is read again
without the other machine.

It suits files that machines write once and read many times, each machine writing its own
and reading everyone's: no shared storage, no copying every file everywhere, and a
machine's own files read from its own disk.

It mounts with [fractal-fuse](https://github.com/fractalbits-labs/fractal-fuse) (FUSE over
io_uring), moves files with [velo](https://github.com/ai-dynamo/velo), and learns of the
machines and their files on [NATS](https://nats.io).

## How it works

- Every machine runs one rview-fs, which exports a directory (`--export`) and mounts the
  union of every machine's export (`--mount`).
- Whatever writes into an export announces each file it adds or deletes on NATS. Each
  rview-fs keeps an index of every machine's files from those announcements, and lists a
  machine's export when the machine shows up and every 10 minutes.
- A file of this machine is read from its disk.
- Opening a file of another machine has that machine stage it, which keeps the file even if
  that machine deletes it meanwhile. The first read of any byte fetches the whole file into
  host memory, and every read after is served from there.
- `--cache-gib` bounds the memory for other machines' files, which go least recently used
  first; `--stage-gib` bounds what a machine stages for the others.

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

Serving a file to another machine renews its modification time, so a cleanup that
deletes the least recently used files keeps the ones other machines use.

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
rview-fs --export /data/export --mount /mnt/union --nats nats://nats:4222 \
  --subject myapp [--node-id $(hostname)] [--rdma]
```

| Flag (environment variable) | What it does |
| --- | --- |
| `--export` (`RVIEW_EXPORT`) | The directory this machine exports |
| `--mount` (`RVIEW_MOUNT`) | Where to mount the union; a mount left by an rview-fs that died is detached first |
| `--node-id` (`RVIEW_NODE_ID`) | The name this machine's files are announced under (host name) |
| `--nats` (`RVIEW_NATS`) | The NATS server |
| `--subject` (`RVIEW_SUBJECT`) | The subjects' prefix (`rview`) |
| `--cache-gib` | Host memory for the files of other machines (16) |
| `--stage-gib` | Host memory this machine stages its files in for the others (16); past it, they cannot open its files until stages are read or closed |
| `--bind` | Where velo listens for the other machines (`0.0.0.0:0`) |
| `--fuse-workers`, `--fuse-queue-depth` | Threads serving FUSE requests (8), and requests in flight per kernel queue (8) |
| `--rdma` | Move files between machines over RDMA (with `--features ucx`) |

It needs `/dev/fuse` and `fusermount3`, and a kernel with FUSE over io_uring
(`/sys/module/fuse/parameters/enable_uring` = `Y`). On SIGTERM it detaches the mount and
exits. It logs how its transfers went: `rdma/ok`, or why it pulled chunks over TCP.

## Benchmarks

`bench/bench.py` plays two machines on one host: two rview-fs, each with its own export
and mount, and a NATS server. It writes files into the exports, announces them, and reads
them through the first machine's mount: that machine's files and the other's. Each stage
reports ops, ops/s, GB/s, and latency percentiles; a stage that reads cold drops the page
cache of the files first. At the end it checks that every file read through the mounts is
the file in the export.

```bash
cargo build --release
python3 bench/bench.py                    # files between the machines over TCP

cargo build --release --features ucx
UCX_NET_DEVICES=mlx5_0:1 python3 bench/bench.py --rdma
```

It needs `nats-server`, FUSE, and Python 3.10 or later; nothing outside its standard
library. `--rview-fs-args` passes more arguments to both rview-fs, such as
`"--cache-gib 1"` to measure fetches that reuse memory the cache freed.

The results below are medians of 3 runs, on one host:

- 2× Intel Xeon 6767P (256 CPUs), Linux 6.6 with FUSE over io_uring;
- the exports on 4 NVMe drives (Solidigm SB5PH27X076T) in RAID 0;
- for RDMA, one ConnectX-7 port at 100 Gb/s InfiniBand, looped back to itself;
- 8 files of 64 MiB in each set, 2000 files of 4 KiB, and 8 readers in the parallel stages.

Rates varied between runs by up to about 30%, the most for reads served from memory.

Files between the machines over TCP:

| stage | ops | MB/op | ops/s | GB/s | p50 ms | p90 ms | max ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| announced file shown by another machine | 200 | – | 15,044 | – | 0.0502 | 0.1 | 0.989 |
| stat, first (another machine's 2000 small files) | 2000 | – | 40,219 | – | 0.0244 | 0.0262 | 0.211 |
| stat, again (another machine's 2000 small files) | 2000 | – | 178,307 | – | 0.0054 | 0.00588 | 0.194 |
| list a folder of 2000 files | 20 | – | 373 | – | 2.67 | 2.68 | 2.81 |
| open, local disk, not rview-fs | 8 | – | 122,542 | – | 0.00662 | 0.00828 | 0.0214 |
| open, this machine's file | 8 | – | 14,838 | – | 0.047 | 0.0554 | 0.206 |
| open, another machine's 64 MiB file, not read yet | 8 | – | 3,393 | – | 0.211 | 0.435 | 0.714 |
| read, local disk, not rview-fs, cold | 8 | 67.1 | 137 | 9.21 | 6.54 | 7.47 | 12.1 |
| read, local disk, not rview-fs, cached | 8 | 67.1 | 131 | 8.79 | 7.84 | 7.89 | 7.91 |
| read, this machine's file, cold | 8 | 67.1 | 23.8 | 1.6 | 39.4 | 43.1 | 49.9 |
| read, this machine's file, cached | 8 | 67.1 | 120 | 8.02 | 8.35 | 8.39 | 8.62 |
| read, another machine's file, fetched | 8 | 67.1 | 13.7 | 0.922 | 71.5 | 75.9 | 79.6 |
| read, another machine's file, first MiB (fetches the file) | 8 | 1.05 | 25.1 | 0.0263 | 38.5 | 42.7 | 45.8 |
| read, another machine's file, cached | 8 | 67.1 | 119 | 7.98 | 8.41 | 8.46 | 8.76 |
| read, another machine's file, from rview-fs memory | 8 | 67.1 | 30.3 | 2.03 | 32.8 | 33.1 | 33.2 |
| open, another machine's file, in rview-fs memory | 8 | – | 50,982 | – | 0.0174 | 0.0191 | 0.0361 |
| 4 KiB random read, local disk, not rview-fs, cold | 4000 | 0.0041 | 17,290 | 0.0708 | 0.0559 | 0.0647 | 0.271 |
| 4 KiB random read, this machine's file, cold | 4000 | 0.0041 | 11,334 | 0.0464 | 0.0844 | 0.0948 | 1.1 |
| 4 KiB random read, another machine's file, from rview-fs memory | 4000 | 0.0041 | 51,239 | 0.21 | 0.0194 | 0.0211 | 0.228 |
| map and fault in, this machine's file, cold | 8 | 67.1 | 26.7 | 1.8 | 37.3 | 38 | 38.8 |
| map and fault in, another machine's file, fetched | 8 | 67.1 | 13.9 | 0.93 | 71.4 | 73.6 | 74.6 |
| map, then touch a random page, another machine's file, fetched on the first | 512 | 0.0041 | 1,479 | 0.00606 | 0.0621 | 0.0833 | 40.9 |
| read, another machine's files, fetched, 8 at once | 16 | 67.1 | 30.5 | 2.05 | 241 | 277 | 283 |
| read, another machine's files, from rview-fs memory, 8 at once | 16 | 67.1 | 140 | 9.4 | 45.4 | 67.1 | 69.1 |
| read, this machine's files, cold, 8 at once | 8 | 67.1 | 95.1 | 6.38 | 68.6 | 71.5 | 81.9 |

Over RDMA, the stages that move the other machine's files:

| stage | ops | MB/op | ops/s | GB/s | p50 ms | p90 ms | max ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| open, another machine's 64 MiB file, not read yet | 8 | – | 41 | – | 23.9 | 26.5 | 27.5 |
| read, another machine's file, fetched | 8 | 67.1 | 11.9 | 0.798 | 79.6 | 83.1 | 116 |
| read, another machine's file, first MiB (fetches the file) | 8 | 1.05 | 22.8 | 0.0239 | 39.5 | 40 | 74.3 |
| read, another machine's file, cached | 8 | 67.1 | 120 | 8.04 | 8.29 | 8.41 | 8.53 |
| read, another machine's file, from rview-fs memory | 8 | 67.1 | 29.7 | 2 | 33.2 | 33.9 | 36.4 |
| open, another machine's file, in rview-fs memory | 8 | – | 49,422 | – | 0.018 | 0.0197 | 0.0372 |
| 4 KiB random read, another machine's file, from rview-fs memory | 4000 | 0.0041 | 90,841 | 0.372 | 0.0108 | 0.0121 | 0.363 |
| map and fault in, another machine's file, fetched | 8 | 67.1 | 13.3 | 0.89 | 74.5 | 76.6 | 86.1 |
| map, then touch a random page, another machine's file, fetched on the first | 512 | 0.0041 | 1,497 | 0.00613 | 0.0601 | 0.068 | 41.8 |
| read, another machine's files, fetched, 8 at once | 16 | 67.1 | 36.9 | 2.47 | 182 | 266 | 295 |
| read, another machine's files, from rview-fs memory, 8 at once | 16 | 67.1 | 143 | 9.62 | 40.6 | 69.7 | 70.5 |

What it shows:

- **Metadata:** a file another machine announces shows up about 50 µs later. A `stat`
  takes 24 µs the first time and 5 µs while the kernel keeps the answer (1 s). Listing a
  folder of 2000 files takes 2.7 ms.
- **This machine's files:**
  - One reader gets 1.6 GB/s from a cold disk that gives 9 GB/s read directly; 8 readers
    get 6.4 GB/s. Every read goes through rview-fs as a FUSE request.
  - From the page cache, reads run at 8 GB/s, as on the disk directly.
  - A random 4 KiB read takes 84 µs, against 56 µs on the disk directly.
- **Another machine's files:**
  - Opening one stages it there. Over TCP that takes 0.2 ms. Over RDMA it takes 24 ms for
    64 MiB, which includes reading the file from that machine's disk and copying it into
    registered memory.
  - The first read fetches the whole file: about 40 ms for 64 MiB, over either transport.
    A whole 64 MiB file, opened and read cold, comes at 0.8–0.9 GB/s per reader, and at
    2–2.5 GB/s with 8 readers.
  - Once fetched, reads come from rview-fs memory: 2 GB/s per reader, and 9.5 GB/s with
    8 readers. A random 4 KiB read takes 11–19 µs. From the page cache, reads run at 8 GB/s.
  - Touching a mapped file: the first touch fetches the whole file, and a later touch of a
    new page takes 60 µs.
- **On one host, TCP and RDMA fetch at the same rate.** Over RDMA, a 64 MiB fetch spends
  about 16 ms copying the file out of registered memory, and about 20 ms on the transfer
  itself, which runs at about 3 GB/s here: a quarter of the link. Between hosts, the link
  adds its own limits, which these numbers do not show.

## Building and testing

```bash
cargo build --release                  # TCP only
cargo build --release --features ucx   # RDMA too; needs libibverbs-dev and librdmacm-dev
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
```

## Known issues

On Linux 6.6 with FUSE over io_uring backported, an rview-fs that exited on SIGTERM while
a FUSE request was in flight once left its kernel connection open. A process waiting on
that request hung until the connection was aborted. To abort it, find the mount's device
in `/proc/self/mountinfo` (the third field, `0:N`) and run
`echo 1 > /sys/fs/fuse/connections/N/abort`. An rview-fs killed with SIGKILL aborts its
connection cleanly.
