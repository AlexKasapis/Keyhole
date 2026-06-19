#!/usr/bin/env python3
"""Drive a TUI binary through a pseudo-terminal for headless smoke tests.

There is no controlling TTY in agent/CI shells, so a TUI can't be launched
directly. This allocates a PTY, sets a window size, runs the program, sends a
timed key script, then asserts on exit status, alternate-screen enter/leave, and
expected on-screen text.

Example:
    cargo build && docker compose up -d redis
    printf '[[connection]]\\ntype="redis"\\nname="local"\\nhost="127.0.0.1"\\nport=6380\\ndb=0\\n' > /tmp/bt.toml
    python3 scripts/tui_smoke.py \\
        --cmd "./target/debug/brokertui --config /tmp/bt.toml --connect local" \\
        --send 2.0:d --send 3.5:b --send 4.5:q \\
        --expect "Connected to local" --expect Keys --expect Dashboard --expect Version

Caveat: ratatui redraws only changed cells, so a string visible on screen can be
fragmented in the captured byte stream when a coincidental matching cell splits
the redraw. Assert on freshly-drawn text, not labels that merely changed frames.
"""
import argparse
import fcntl
import os
import pty
import select
import shlex
import struct
import sys
import termios
import time

SPECIAL = {
    "ENTER": b"\r",
    "RET": b"\r",
    "ESC": b"\x1b",
    "TAB": b"\t",
    "BACKSPACE": b"\x7f",
    "BS": b"\x7f",
    "SPACE": b" ",
    "UP": b"\x1b[A",
    "DOWN": b"\x1b[B",
    "LEFT": b"\x1b[D",
    "RIGHT": b"\x1b[C",
}


def to_bytes(keys: str) -> bytes:
    if keys in SPECIAL:
        return SPECIAL[keys]
    if keys.upper().startswith("CTRL-") and len(keys) == 6:
        return bytes([ord(keys[-1].lower()) & 0x1F])
    return keys.encode()


def parse_send(spec: str):
    delay, _, keys = spec.partition(":")
    return float(delay), to_bytes(keys)


def run(cmd, sends, rows, cols, timeout):
    argv = shlex.split(cmd)
    pid, fd = pty.fork()
    if pid == 0:
        env = dict(os.environ)
        env.setdefault("TERM", "xterm-256color")
        os.execvpe(argv[0], argv, env)
        os._exit(127)
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))

    out = b""
    start = time.time()
    pending = sorted(sends)
    exit_code = None
    while True:
        r, _, _ = select.select([fd], [], [], 0.2)
        if r:
            try:
                data = os.read(fd, 65536)
            except OSError:
                data = b""
            if not data:
                break
            out += data
        now = time.time() - start
        while pending and now >= pending[0][0]:
            _, b = pending.pop(0)
            try:
                os.write(fd, b)
            except OSError:
                pass
        wpid, status = os.waitpid(pid, os.WNOHANG)
        if wpid != 0:
            exit_code = os.waitstatus_to_exitcode(status)
            break
        if now > timeout:
            os.kill(pid, 9)
            os.waitpid(pid, 0)
            exit_code = -1
            break

    while True:
        r, _, _ = select.select([fd], [], [], 0.2)
        if not r:
            break
        try:
            d = os.read(fd, 65536)
        except OSError:
            break
        if not d:
            break
        out += d
    if exit_code is None:
        _, status = os.waitpid(pid, 0)
        exit_code = os.waitstatus_to_exitcode(status)
    return exit_code, out.decode("utf-8", "replace")


def main():
    ap = argparse.ArgumentParser(description="PTY smoke-test driver for a TUI binary.")
    ap.add_argument("--cmd", required=True, help="program + args to run")
    ap.add_argument(
        "--send",
        action="append",
        default=[],
        metavar="DELAY:KEYS",
        help="send KEYS at DELAY seconds (KEYS: literal text, or ENTER/ESC/TAB/UP/DOWN/SPACE/CTRL-x)",
    )
    ap.add_argument("--expect", action="append", default=[], help="substring that must appear")
    ap.add_argument("--reject", action="append", default=[], help="substring that must NOT appear")
    ap.add_argument("--rows", type=int, default=40)
    ap.add_argument("--cols", type=int, default=140)
    ap.add_argument("--timeout", type=float, default=15.0)
    args = ap.parse_args()

    exit_code, txt = run(
        args.cmd, [parse_send(s) for s in args.send], args.rows, args.cols, args.timeout
    )

    alt_in = "\x1b[?1049h" in txt
    alt_out = "\x1b[?1049l" in txt
    ok = exit_code == 0 and alt_in and alt_out
    print(f"exit={exit_code}  alt_screen_enter={alt_in}  alt_screen_leave={alt_out}")
    for needle in args.expect:
        hit = needle in txt
        ok = ok and hit
        print(f"  {'OK  ' if hit else 'MISS'} expect {needle!r}")
    for needle in args.reject:
        absent = needle not in txt
        ok = ok and absent
        print(f"  {'OK  ' if absent else 'BAD '} reject {needle!r}")
    print("PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
