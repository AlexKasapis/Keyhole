#!/usr/bin/env python3
"""Unit tests for the demo-recording cast helpers (stdlib only — no pytest).

Covers the genuinely-new logic added for `just record-demo`:
  - tui_smoke.build_cast    : (elapsed, bytes) chunks -> asciicast v2 document
  - tui_smoke.coalesce_fps  : frame-rate capping by merging chunks in a window
  - freeze_poster.freeze    : seek+pause an animated SVG into a static poster

Run directly: `python3 scripts/test_cast_tools.py` (also invoked by scripts/test.sh).
"""
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from freeze_poster import freeze  # noqa: E402
from tui_smoke import build_cast, coalesce_fps  # noqa: E402


def parse(doc):
    lines = [ln for ln in doc.splitlines() if ln.strip()]
    return json.loads(lines[0]), [json.loads(ln) for ln in lines[1:]]


class BuildCastTests(unittest.TestCase):
    def test_header_shape(self):
        header, _ = parse(build_cast([], rows=24, cols=80))
        self.assertEqual(header, {"version": 2, "width": 80, "height": 24, "env": {"TERM": "xterm-256color"}})

    def test_events_are_output_records(self):
        _, events = parse(build_cast([(0.0, b"hi"), (0.5, b" there")], 10, 40))
        self.assertEqual(events, [[0.0, "o", "hi"], [0.5, "o", " there"]])

    def test_multibyte_split_across_chunks_is_reassembled(self):
        # "│" is 0xE2 0x94 0x82 — split it across two PTY reads.
        ev = [(0.0, b"a\xe2\x94"), (0.5, b"\x82b")]
        _, events = parse(build_cast(ev, 10, 40))
        text = "".join(e[2] for e in events)
        self.assertEqual(text, "a│b")
        # The partial-only first chunk must not emit replacement chars.
        self.assertNotIn("�", text)

    def test_fully_buffered_chunk_emits_no_empty_event(self):
        # First chunk is only the lead bytes of a multibyte glyph -> decodes to "".
        _, events = parse(build_cast([(0.0, b"\xe2\x94"), (0.5, b"\x82")], 10, 40))
        self.assertTrue(all(e[2] != "" for e in events))
        self.assertEqual("".join(e[2] for e in events), "│")


class CoalesceFpsTests(unittest.TestCase):
    def test_zero_is_passthrough_copy(self):
        ev = [(0.0, b"a"), (0.1, b"b")]
        out = coalesce_fps(ev, 0)
        self.assertEqual(out, ev)
        self.assertIsNot(out, ev)

    def test_merges_within_window_preserving_bytes_and_first_timestamp(self):
        ev = [(0.0, b"a"), (0.01, b"b"), (0.2, b"c")]
        out = coalesce_fps(ev, 10)  # min_dt = 0.1s
        self.assertEqual(out, [(0.0, b"ab"), (0.2, b"c")])
        # Concatenated stream is unchanged — only the framing collapses.
        self.assertEqual(b"".join(d for _, d in out), b"".join(d for _, d in ev))

    def test_reduces_high_rate_stream(self):
        ev = [(i * 0.01, b"x") for i in range(100)]  # 100 chunks across ~1s
        out = coalesce_fps(ev, 12)
        self.assertLess(len(out), len(ev))
        self.assertEqual(b"".join(d for _, d in out), b"x" * 100)


class FreezePosterTests(unittest.TestCase):
    SVG = '<svg width="100" height="50"><rect/><text>hi</text></svg>'

    def test_injects_seek_and_pause_after_opening_tag(self):
        out = freeze(self.SVG, at=11)
        # The style must be the first child (right after the opening <svg ...> tag).
        self.assertTrue(out.startswith('<svg width="100" height="50"><style>'))
        self.assertIn("animation-delay:-11s!important", out)
        self.assertIn("animation-play-state:paused!important", out)

    def test_preserves_original_content(self):
        out = freeze(self.SVG, at=5)
        self.assertIn("<rect/>", out)
        self.assertIn("<text>hi</text>", out)
        self.assertTrue(out.endswith("</svg>"))

    def test_at_value_is_used(self):
        self.assertIn("-7.5s", freeze(self.SVG, at=7.5))


if __name__ == "__main__":
    unittest.main(verbosity=2)
