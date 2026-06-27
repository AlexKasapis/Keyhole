#!/usr/bin/env bash
#
# Record a real Keyhole TUI session and render it to the animated SVG the
# marketing sites embed. The asciicast at scripts/demos/keyhole-demo.cast is the
# source of truth; the SVGs under website/public and docs-site/public are
# regenerated from it, so a stale demo is fixed by re-running this script and
# committing the result.
#
# Pipeline: redis up -> seed -> publish fake traffic -> drive the binary through
# a PTY (scripts/tui_smoke.py) capturing an asciicast -> svg-term render. The
# capture ends by hitting the timeout (SIGKILL) so it stops on the last drawn
# frame rather than the blank post-quit screen.
#
# Needs: docker (compose), cargo, python3, npx. Run from anywhere.
set -euo pipefail

# --- knobs (override via env) ------------------------------------------------
ROWS="${KEYHOLE_DEMO_ROWS:-32}"
COLS="${KEYHOLE_DEMO_COLS:-100}"
FPS="${KEYHOLE_DEMO_FPS:-12}"
DURATION="${KEYHOLE_DEMO_DURATION:-15.5}"
RATE="${KEYHOLE_DEMO_RATE:-3}"
POSTER_AT="${KEYHOLE_DEMO_POSTER_AT:-11}"
REDIS_PORT="${KEYHOLE_REDIS_PORT:-6379}"
CONFIG="config.dev.toml"
PROFILE="dev-redis"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT" || exit 1

CAST="scripts/demos/keyhole-demo.cast"
SITES=(website docs-site)

TMP="$(mktemp -d)"
PUB_PID=""
cleanup() {
  if [ -n "$PUB_PID" ]; then kill "$PUB_PID" 2>/dev/null || true; fi
  rm -rf "$TMP"
}
trap cleanup EXIT

redis_up() { (exec 3<>"/dev/tcp/127.0.0.1/${REDIS_PORT}") 2>/dev/null; }

# --- 1. redis ----------------------------------------------------------------
if redis_up; then
  echo "==> reusing redis already listening on :${REDIS_PORT}"
else
  echo "==> starting redis"
  docker compose up -d redis
  for ((i = 0; i < 30; i++)); do redis_up && break; sleep 1; done
  redis_up || { echo "redis did not come up on :${REDIS_PORT}" >&2; exit 1; }
fi

# --- 2. binary ---------------------------------------------------------------
echo "==> building keyhole (release)"
cargo build --release
BIN="${REPO_ROOT}/target/release/keyhole"

# --- 3. sample data + live traffic -------------------------------------------
echo "==> seeding sample keyspace"
"$BIN" dev seed --config "$CONFIG"

echo "==> publishing fake traffic (${RATE}/s)"
"$BIN" dev publish --broker redis --rate "$RATE" --config "$CONFIG" >/dev/null 2>&1 &
PUB_PID=$!
sleep 1

# --- 4. record ---------------------------------------------------------------
echo "==> recording ${DURATION}s session (${COLS}x${ROWS} @ ${FPS}fps)"
python3 scripts/tui_smoke.py \
  --cmd "$BIN --config $CONFIG --connect $PROFILE" \
  --rows "$ROWS" --cols "$COLS" --timeout "$DURATION" --max-fps "$FPS" --allow-timeout \
  --send 2.0:z --send 2.6:DOWN --send 3.0:DOWN --send 3.4:DOWN \
  --send 5.0:CTRL-DOWN --send 6.0:TAB --send 7.0:TAB --send 8.0:p \
  --expect Keys --expect dev-redis \
  --cast "$TMP/demo.cast"

mkdir -p "$(dirname "$CAST")"
cp "$TMP/demo.cast" "$CAST"
echo "==> wrote ${CAST} ($(wc -c <"$CAST") bytes)"

# --- 5. render ---------------------------------------------------------------
render() { npx --yes svg-term-cli --in "$1" --out "$2" --window --width "$COLS" --height "$ROWS"; }

echo "==> rendering animated SVG"
render "$CAST" "$TMP/keyhole-demo.svg"

# Poster = the animated SVG frozen at POSTER_AT (a single-frame cast renders blank
# because svg-term parks it off-screen). See scripts/freeze_poster.py.
echo "==> freezing reduced-motion poster (frame at ${POSTER_AT}s)"
python3 scripts/freeze_poster.py --in "$TMP/keyhole-demo.svg" --at "$POSTER_AT" --out "$TMP/keyhole-demo-poster.svg"

# Both sites embed the same demo; keep their public assets byte-identical.
for site in "${SITES[@]}"; do
  cp "$TMP/keyhole-demo.svg" "${site}/public/keyhole-demo.svg"
  cp "$TMP/keyhole-demo-poster.svg" "${site}/public/keyhole-demo-poster.svg"
done

echo "==> done:"
for site in "${SITES[@]}"; do
  echo "    ${site}/public/keyhole-demo.svg        $(wc -c <"${site}/public/keyhole-demo.svg") bytes"
  echo "    ${site}/public/keyhole-demo-poster.svg $(wc -c <"${site}/public/keyhole-demo-poster.svg") bytes"
done
