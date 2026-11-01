#!/usr/bin/env python3
"""Benchmark of rview-fs on one host, which plays two machines: two rview-fs, each with
its own export and mount, and a NATS server. Files are written into the exports and
announced on NATS, then read through the first machine's mount: its own files, and the
other machine's, which rview-fs fetches over TCP (or RDMA, with --rdma).

Every stage reports ops, ops/s, GB/s, and latency percentiles in milliseconds. The page
cache of a file is dropped (posix_fadvise) where a stage reads cold.

Needs a built rview-fs, nats-server, and FUSE (/dev/fuse, fusermount3). Python 3.10+,
standard library only. From the repository root:

    cargo build --release
    python3 bench/bench.py [--rdma] [--file-mib 64] [--files 8] [--threads 8]
"""

import argparse
import ctypes
import hashlib
import json
import mmap
import os
import random
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

SUBJECT = "rview.bench"
MIB = 1 << 20
_MADV_POPULATE_READ = 22  # Linux 5.14
_libc = ctypes.CDLL(None, use_errno=True)
_libc.madvise.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_until(condition, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise TimeoutError(f"waited {timeout}s for {what}")
        time.sleep(0.01)


def mounted(path: Path) -> bool:
    """Whether something is mounted at ``path``, from the mount table: asking the file
    system would wait forever on a FUSE daemon that died with a request open."""
    target = os.fsencode(os.path.abspath(path))
    with open("/proc/self/mountinfo", "rb") as f:
        return any(line.split()[4] == target for line in f)


class Nats:
    """A NATS server of our own, and a connection to publish on."""

    def __init__(self, binary: str):
        port = free_port()
        self.url = f"nats://127.0.0.1:{port}"
        self.proc = subprocess.Popen([binary, "-a", "127.0.0.1", "-p", str(port)],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        def up() -> bool:
            if self.proc.poll() is not None:
                raise RuntimeError("nats-server exited")
            try:
                self.sock = socket.create_connection(("127.0.0.1", port), timeout=1)
                return True
            except OSError:
                return False

        wait_until(up, 10, "nats-server")
        self.sock.settimeout(10)
        self.lines = self.sock.makefile("rb")
        self.lines.readline()  # INFO
        self.sock.sendall(b'CONNECT {"verbose":false,"pedantic":false}\r\n')

    def publish(self, subject: str, payload: bytes) -> None:
        self.sock.sendall(b"PUB %s %d\r\n%s\r\n" % (subject.encode(), len(payload), payload))

    def flush(self) -> None:
        """Wait until the server has everything published so far."""
        self.sock.sendall(b"PING\r\n")
        while (line := self.lines.readline()) != b"PONG\r\n":
            if not line:
                raise ConnectionError("NATS closed the connection")
            if line == b"PING\r\n":
                self.sock.sendall(b"PONG\r\n")

    def close(self) -> None:
        self.sock.close()
        self.proc.terminate()
        self.proc.wait(timeout=10)


class Node:
    """A machine: its export, and the rview-fs that mounts the union."""

    def __init__(self, work: Path, name: str, nats: Nats, binary: str, args: list[str]):
        self.name = f"node-{name}"
        self.export, self.mount = work / name / "export", work / name / "mnt"
        self.export.mkdir(parents=True)
        self.mount.mkdir(parents=True)
        self.log_path = work / f"{name}.log"
        self.log = self.log_path.open("w")
        self.proc = subprocess.Popen(
            [binary, "--export", str(self.export), "--mount", str(self.mount), "--node-id", self.name,
             "--nats", nats.url, "--subject", SUBJECT, "--bind", f"127.0.0.1:{free_port()}", *args],
            stdout=self.log, stderr=subprocess.STDOUT)
        try:
            wait_until(lambda: mounted(self.mount), 20, f"{self.name} to mount")
        except BaseException:
            self.stop()
            raise

    def stop(self) -> None:
        """Stop rview-fs, which unmounts on its way out; kill it, and raise, if it does not
        go in time."""
        self.log.close()
        if self.proc.poll() is not None:
            return
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=30)
        except subprocess.TimeoutExpired as e:
            self.proc.kill()
            self.proc.wait(timeout=30)
            raise RuntimeError(f"{self.name}: rview-fs did not stop in 30s of SIGTERM; killed it") from e

    def transfers(self) -> str:
        """How rview-fs moved the files it served, from its log: rdma/ok, or why not."""
        log = re.sub(r"\x1b\[[0-9;]*m", "", self.log_path.read_text())
        lines = [line for line in log.splitlines() if "file transfers by path" in line]
        return lines[-1].split("paths=")[-1] if lines else "none"


@dataclass
class Stage:
    """Timed ops of one stage. ``wall`` is how long they took together when they ran at
    once; otherwise the sum of theirs."""

    name: str
    seconds: list[float] = field(default_factory=list)
    nbytes: list[int] = field(default_factory=list)
    wall: float | None = None

    def add(self, seconds: float, nbytes: int = 0) -> None:
        self.seconds.append(seconds)
        self.nbytes.append(nbytes)

    def summary(self) -> dict:
        wall = self.wall if self.wall is not None else sum(self.seconds)
        lat = sorted(s * 1e3 for s in self.seconds)

        def pct(p: float) -> float:
            return lat[min(len(lat) - 1, round(p / 100 * (len(lat) - 1)))]

        return {"stage": self.name, "ops": len(lat), "MB/op": sum(self.nbytes) / len(lat) / 1e6,
                "ops/s": len(lat) / wall, "GB/s": sum(self.nbytes) / wall / 1e9,
                "p50 ms": pct(50), "p90 ms": pct(90), "max ms": lat[-1]}


def write_files(node: Node, nats: Nats, files: list[tuple[str, int]]) -> None:
    """Write ``files`` (path, bytes) into ``node``'s export, each under a hidden name and
    renamed into place whole, and announce them."""
    block = os.urandom(MIB)
    updates = []
    for rel, size in files:
        path = node.export / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.parent / f".{path.name}.tmp"
        with open(tmp, "wb") as f:
            for offset in range(0, size, MIB):
                f.write(block[:min(MIB, size - offset)])
        os.rename(tmp, path)
        st = path.stat()
        updates.append({"op": "add", "path": rel, "size": st.st_size, "mtime": st.st_mtime})
    for i in range(0, len(updates), 500):
        nats.publish(f"{SUBJECT}.files",
                     json.dumps({"node": node.name, "updates": updates[i:i + 500]}).encode())
    nats.flush()


def drop_cached(paths: list[Path], *, written: bool = False) -> None:
    """Have the kernel drop the pages of ``paths`` it caches, so the next read goes to the
    file system. Pages ``written`` are flushed first: the kernel keeps those it has not."""
    for path in paths:
        fd = os.open(path, os.O_RDONLY)
        try:
            if written:
                os.fdatasync(fd)
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)


def read_whole(path: Path, first: Stage | None = None) -> int:
    """Read ``path`` in 1 MiB reads; with ``first``, time the first one there."""
    buf = memoryview(bytearray(MIB))
    total = 0
    with open(path, "rb", buffering=0) as f:
        begin = time.perf_counter()
        while got := f.readinto(buf):
            if first is not None and total == 0:
                first.add(time.perf_counter() - begin, got)
            total += got
    return total


def sequential(stage: Stage, paths: list[Path], first: Stage | None = None) -> None:
    for path in paths:
        begin = time.perf_counter()
        nbytes = read_whole(path, first)
        stage.add(time.perf_counter() - begin, nbytes)


def in_parallel(stage: Stage, paths: list[Path], threads: int) -> None:
    """Read ``paths`` whole, ``threads`` at once."""
    def one(path: Path) -> None:
        begin = time.perf_counter()
        nbytes = read_whole(path)
        stage.add(time.perf_counter() - begin, nbytes)

    begin = time.perf_counter()
    with ThreadPoolExecutor(threads) as pool:
        list(pool.map(one, paths))
    stage.wall = time.perf_counter() - begin


def random_reads(stage: Stage, paths: list[Path], count: int, block: int = 4096) -> None:
    """``count`` reads of ``block`` bytes at random offsets of each file, with no read-ahead,
    so each is one request to the file system."""
    rng = random.Random(0)
    for path in paths:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_RANDOM)
            size = os.fstat(fd).st_size
            for _ in range(count):
                offset = rng.randrange(size // block) * block
                begin = time.perf_counter()
                os.pread(fd, block, offset)
                stage.add(time.perf_counter() - begin, block)
        finally:
            os.close(fd)


def opens(stage: Stage, paths: list[Path]) -> None:
    for path in paths:
        begin = time.perf_counter()
        fd = os.open(path, os.O_RDONLY)
        stage.add(time.perf_counter() - begin)
        os.close(fd)


def map_populated(stage: Stage, paths: list[Path]) -> None:
    """Map each file and fault all of it in (MADV_POPULATE_READ), which reports what touching
    the mapping would SIGBUS for."""
    for path in paths:
        begin = time.perf_counter()
        with open(path, "rb") as f:
            mapped = mmap.mmap(f.fileno(), 0, flags=mmap.MAP_PRIVATE, prot=mmap.PROT_READ | mmap.PROT_WRITE)
        anchor = ctypes.c_char.from_buffer(mapped)
        if _libc.madvise(ctypes.addressof(anchor), len(mapped), _MADV_POPULATE_READ):
            err = ctypes.get_errno()
            raise OSError(err, f"populating {path}: {os.strerror(err)}")
        stage.add(time.perf_counter() - begin, len(mapped))
        del anchor
        mapped.close()


def first_touches(stage: Stage, paths: list[Path], count: int) -> None:
    """Map each file, nothing read in, and touch ``count`` random pages of it."""
    rng = random.Random(1)
    for path in paths:
        with open(path, "rb") as f:
            mapped = mmap.mmap(f.fileno(), 0, flags=mmap.MAP_PRIVATE, prot=mmap.PROT_READ)
        pages = len(mapped) // mmap.PAGESIZE
        for page in rng.sample(range(pages), min(count, pages)):
            begin = time.perf_counter()
            mapped[page * mmap.PAGESIZE]
            stage.add(time.perf_counter() - begin, mmap.PAGESIZE)
        mapped.close()


def digest(path: Path) -> str:
    h = hashlib.sha256()
    buf = memoryview(bytearray(MIB))
    with open(path, "rb", buffering=0) as f:
        while got := f.readinto(buf):
            h.update(buf[:got])
    return h.hexdigest()


def table(stages: list[Stage]) -> str:
    head = ["stage", "ops", "MB/op", "ops/s", "GB/s", "p50 ms", "p90 ms", "max ms"]
    rows = ["| " + " | ".join(head) + " |", "| --- | " + " | ".join(["---:"] * (len(head) - 1)) + " |"]
    for s in stages:
        r = s.summary()
        cells = [r["stage"], str(r["ops"])]
        for h in head[2:]:
            cells.append("–" if h in ("MB/op", "GB/s") and r["GB/s"] == 0 else
                         f"{r[h]:,.0f}" if r[h] >= 1000 else f"{r[h]:.3g}")
        rows.append("| " + " | ".join(cells) + " |")
    return "\n".join(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--rview-fs", default="target/release/rview-fs", help="the binary")
    parser.add_argument("--nats-server", default=shutil.which("nats-server"), help="the binary")
    parser.add_argument("--rdma", action="store_true", help="run rview-fs with --rdma (a --features ucx build)")
    parser.add_argument("--rview-fs-args", default="", help="more arguments for both rview-fs, as one string")
    parser.add_argument("--file-mib", type=int, default=64, help="size of the big files")
    parser.add_argument("--files", type=int, default=8, help="big files in each set")
    parser.add_argument("--small", type=int, default=2000, help="small files to stat and list")
    parser.add_argument("--threads", type=int, default=8, help="readers at once, for the parallel stages")
    parser.add_argument("--dir", default=None, help="where the exports go; a temporary folder in it")
    parser.add_argument("--json", default=None, help="also write the results here")
    args = parser.parse_args()
    if args.nats_server is None:
        parser.error("needs nats-server, on PATH or --nats-server")
    size = args.file_mib * MIB
    work = Path(tempfile.mkdtemp(prefix="rview-fs-bench-", dir=args.dir))
    stages: list[Stage] = []

    def stage(name: str) -> Stage:
        stages.append(Stage(name))
        return stages[-1]

    nats = Nats(args.nats_server)
    nodes: list[Node] = []
    try:
        extra = (["--rdma"] if args.rdma else []) + args.rview_fs_args.split()
        nodes += [Node(work, "a", nats, args.rview_fs, extra), Node(work, "b", nats, args.rview_fs, extra)]
        a, b = nodes
        for node, other in ((a, b), (b, a)):  # until each knows the other, it shows none of its files
            write_files(node, nats, [(f"hello-{node.name}", 1)])
            wait_until((other.mount / f"hello-{node.name}").exists, 30, f"{other.name} to show {node.name}")

        def big(prefix: str, count: int) -> list[str]:
            return [f"{prefix}/{i:03}.bin" for i in range(count)]

        own, fetched, mapped, touched = (big(p, args.files) for p in ("own", "fetched", "mapped", "touched"))
        together = big("together", 2 * args.threads)
        small = [f"small/{i:05}.bin" for i in range(args.small)]
        write_files(a, nats, [(p, size) for p in own])
        write_files(b, nats, [(p, size) for p in (*fetched, *mapped, *touched, *together)] +
                    [(p, 4096) for p in small])
        for node, last in ((a, own[-1]), (a, small[-1]), (a, together[-1])):
            wait_until((node.mount / last).exists, 60, f"{node.name} to show {last}")
        drop_cached([a.export / p for p in own] + [b.export / p for p in (*fetched, *mapped, *touched, *together)],
                    written=True)

        def at(node: Node, paths: list[str], *, export: bool = False) -> list[Path]:
            return [(node.export if export else node.mount) / p for p in paths]

        native, local, remote = at(a, own, export=True), at(a, own), at(a, fetched)

        # How soon another machine shows a file announced.
        shown = stage("announced file shown by another machine")
        for i in range(200):
            rel = f"shown/{i:03}.bin"
            write_files(b, nats, [(rel, 4096)])
            begin = time.perf_counter()
            while not (a.mount / rel).exists():
                time.sleep(0.0001)
            shown.add(time.perf_counter() - begin)

        # Metadata.
        for name in ("stat, first", "stat, again"):
            st = stage(f"{name} (another machine's {len(small)} small files)")
            for path in at(a, small):
                begin = time.perf_counter()
                path.stat()
                st.add(time.perf_counter() - begin)
        listing = stage(f"list a folder of {len(small)} files")
        for _ in range(20):
            begin = time.perf_counter()
            os.listdir(a.mount / "small")
            listing.add(time.perf_counter() - begin)

        # Opens: of another machine's file, it is staged there.
        opens(stage("open, local disk, not rview-fs"), native)
        opens(stage("open, this machine's file"), local)
        opens(stage(f"open, another machine's {args.file_mib} MiB file, not read yet"), remote)

        # Whole files, 1 MiB reads, one file at a time.
        sequential(stage("read, local disk, not rview-fs, cold"), native)
        sequential(stage("read, local disk, not rview-fs, cached"), native)
        drop_cached(native)
        sequential(stage("read, this machine's file, cold"), local)
        sequential(stage("read, this machine's file, cached"), local)
        drop_cached([b.export / p for p in fetched])  # staging them for the opens read them there
        whole = stage("read, another machine's file, fetched")
        sequential(whole, remote, stage("read, another machine's file, first MiB (fetches the file)"))
        sequential(stage("read, another machine's file, cached"), remote)
        drop_cached(remote)
        sequential(stage("read, another machine's file, from rview-fs memory"), remote)
        opens(stage("open, another machine's file, in rview-fs memory"), remote)

        # Random 4 KiB reads, no read-ahead: one request each.
        drop_cached(native)
        random_reads(stage("4 KiB random read, local disk, not rview-fs, cold"), native, 500)
        drop_cached(native)
        drop_cached(local)
        random_reads(stage("4 KiB random read, this machine's file, cold"), local, 500)
        drop_cached(remote)
        random_reads(stage("4 KiB random read, another machine's file, from rview-fs memory"), remote, 500)

        # Mapped.
        drop_cached(native)
        drop_cached(local)
        map_populated(stage("map and fault in, this machine's file, cold"), local)
        map_populated(stage("map and fault in, another machine's file, fetched"), at(a, mapped))
        first_touches(stage("map, then touch a random page, another machine's file, fetched on the first"),
                      at(a, touched), 64)

        # Readers at once, each on its own files.
        in_parallel(stage(f"read, another machine's files, fetched, {args.threads} at once"),
                    at(a, together), args.threads)
        drop_cached(at(a, together))
        in_parallel(stage(f"read, another machine's files, from rview-fs memory, {args.threads} at once"),
                    at(a, together), args.threads)
        drop_cached(native)
        drop_cached(local)
        in_parallel(stage(f"read, this machine's files, cold, {args.threads} at once"), local, args.threads)

        # What the mounts gave is what the exports hold.
        for node, rel in ((a, own[0]), (b, fetched[0]), (b, mapped[-1]), (b, together[-1]), (b, small[0])):
            for reader in (a, b):
                if digest(reader.mount / rel) != digest(node.export / rel):
                    raise AssertionError(f"{rel} through {reader.name}'s mount is not {node.name}'s file")
        time.sleep(0.5)  # for the kernel to close what was read
    finally:
        failures = []
        for node in nodes:
            try:
                node.stop()
            except RuntimeError as e:
                failures.append(e)
        for node in nodes:
            if mounted(node.mount):
                subprocess.run(["fusermount3", "-u", "-z", str(node.mount)], check=False, timeout=30)
        nats.close()
        transfers = {node.name: node.transfers() for node in nodes}
        if any(mounted(node.mount) for node in nodes):
            print(f"left {work} in place: something is still mounted in it")
        else:
            shutil.rmtree(work, ignore_errors=True)
        if failures:
            raise failures[0]

    config = (f"{args.files} files of {args.file_mib} MiB per set, {args.small} small files of 4 KiB, "
              f"{args.threads} readers at once; files between the machines over "
              + (f"RDMA (transfers by path: {transfers})" if args.rdma else "TCP"))
    print(config)
    print(table(stages))
    if args.json:
        Path(args.json).write_text(json.dumps({"config": config, "stages": [s.summary() for s in stages]}, indent=1))


if __name__ == "__main__":
    main()
