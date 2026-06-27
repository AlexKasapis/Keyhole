#!/usr/bin/env python3
"""Freeze a svg-term animated SVG into a static poster for reduced-motion users.

svg-term lays its frames out in one horizontal strip and reveals a frame at a
time by animating the strip's offset. That means a *single-frame* cast renders
blank — the content frame is parked off-screen with no animation to scroll it
into view. So rather than rendering a separate still, we take the real animated
SVG and bake a negative `animation-delay` + paused play-state into it: every
animation seeks to `--at` seconds and holds there. The result renders identically
to that moment of the animation and never moves (so it honours reduced motion).

    python3 scripts/freeze_poster.py --in demo.svg --at 11 --out poster.svg
"""
import argparse


def freeze(svg, at):
    """Return ``svg`` with all animations seeked to ``at`` seconds and paused."""
    style = (
        f"<style>*{{animation-delay:-{at}s!important;"
        "animation-play-state:paused!important}</style>"
    )
    end = svg.index(">") + 1  # insert as the first child, just after the opening <svg ...>
    return svg[:end] + style + svg[end:]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--in", dest="inp", required=True, help="animated svg-term SVG")
    ap.add_argument("--out", required=True, help="output frozen poster SVG")
    ap.add_argument("--at", type=float, default=11.0, help="seconds to freeze at (default 11)")
    args = ap.parse_args()
    with open(args.inp, encoding="utf-8") as f:
        svg = f.read()
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(freeze(svg, args.at))


if __name__ == "__main__":
    main()
