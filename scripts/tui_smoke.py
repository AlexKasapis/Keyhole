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
        --cmd "./target/debug/keyhole --config /tmp/bt.toml --connect local" \\
        --send 2.0:d --send 3.5:b --send 4.5:q \\
        --expect "Connected to local" --expect Keys --expect Dashboard --expect Version

Caveat: ratatui redraws only changed cells, so a string visible on screen can be
fragmented in the captured byte stream when a coincidental matching cell splits
the redraw. Assert on freshly-drawn text, not labels that merely changed frames.
"""
import argparse
import codecs
import fcntl
import json
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
    # Ctrl+arrows (xterm modifyOtherKeys form) — the app's pane-focus idiom.
    "CTRL-UP": b"\x1b[1;5A",
    "CTRL-DOWN": b"\x1b[1;5B",
    "CTRL-RIGHT": b"\x1b[1;5C",
    "CTRL-LEFT": b"\x1b[1;5D",
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
    events = []
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
            events.append((time.time() - start, data))
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
        events.append((time.time() - start, d))
    if exit_code is None:
        _, status = os.waitpid(pid, 0)
        exit_code = os.waitstatus_to_exitcode(status)
    return exit_code, out.decode("utf-8", "replace"), events


def coalesce_fps(events, max_fps):
    """Merge consecutive output chunks into at most ``max_fps`` per second.

    Each terminal chunk is a delta the emulator applies in order, so merging the
    chunks that land within one ``1/max_fps`` window into a single event (kept at
    the window's first timestamp) preserves the rendered result while cutting the
    number of distinct animation steps a renderer must encode — the app's ~30 Hz
    ambient redraws (the pulsing health dot) otherwise bloat the output. A no-op
    when ``max_fps`` is falsy or non-positive.
    """
    if not max_fps or max_fps <= 0:
        return list(events)
    min_dt = 1.0 / max_fps
    out = []
    for elapsed, data in events:
        if out and elapsed - out[-1][0] < min_dt:
            out[-1] = (out[-1][0], out[-1][1] + data)
        else:
            out.append((elapsed, data))
    return out


def build_cast(events, rows, cols, term="xterm-256color"):
    """Render captured ``(elapsed, bytes)`` chunks as an asciicast v2 document.

    Returns the file text: a JSON header line, then one ``[elapsed, "o", text]``
    event per chunk. A UTF-8 incremental decoder spans chunk boundaries, so a
    multibyte glyph (box-drawing chars, etc.) split across two PTY reads is not
    mangled into replacement characters. Empty-after-decode chunks (all bytes
    buffered for the next read) are skipped rather than written as empty events.
    """
    header = {"version": 2, "width": cols, "height": rows, "env": {"TERM": term}}
    lines = [json.dumps(header)]
    dec = codecs.getincrementaldecoder("utf-8")("replace")
    last = 0.0
    for elapsed, data in events:
        last = elapsed
        text = dec.decode(data)
        if text:
            lines.append(json.dumps([round(elapsed, 6), "o", text]))
    tail = dec.decode(b"", final=True)
    if tail:
        lines.append(json.dumps([round(last, 6), "o", tail]))
    return "\n".join(lines) + "\n"


def write_cast(path, events, rows, cols):
    with open(path, "w", encoding="utf-8") as f:
        f.write(build_cast(events, rows, cols))


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
    ap.add_argument(
        "--cast",
        metavar="PATH",
        help="also write the captured session as an asciicast v2 file at PATH",
    )
    ap.add_argument(
        "--max-fps",
        type=float,
        default=0.0,
        metavar="HZ",
        help="coalesce cast frames to at most HZ per second (0 = keep every frame)",
    )
    ap.add_argument(
        "--allow-timeout",
        action="store_true",
        help="recording mode: end by hitting --timeout (SIGKILL) instead of a clean "
        "quit, so the capture stops on the last drawn frame; only --expect/--reject "
        "and alt-screen entry are required to PASS",
    )
    args = ap.parse_args()

    exit_code, txt, events = run(
        args.cmd, [parse_send(s) for s in args.send], args.rows, args.cols, args.timeout
    )

    if args.cast:
        cast_events = coalesce_fps(events, args.max_fps)
        write_cast(args.cast, cast_events, args.rows, args.cols)
        print(f"wrote {len(cast_events)} event(s) to {args.cast}")

    alt_in = "\x1b[?1049h" in txt
    alt_out = "\x1b[?1049l" in txt
    ok = alt_in if args.allow_timeout else (exit_code == 0 and alt_in and alt_out)
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
