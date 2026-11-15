#!/usr/bin/env bash
# Run the Hermes integration tests against a Hermes Agent source tree.
# Usage: HERMES_SRC=/path/to/hermes-agent integrations/hermes/test/run.sh [pytest args]
# The venv goes to $HERMES_TEST_VENV (default: a dir under ${TMPDIR:-/tmp}). The source tree stays read-only.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
: "${HERMES_SRC:?set HERMES_SRC to a Hermes Agent source tree, for example a v0.23.1 checkout}"
venv="${HERMES_TEST_VENV:-${TMPDIR:-/tmp}/a2a-hub-hermes-test-venv}"
uv="${UV:-uvx --from uv>=0.11 uv}"

if [ ! -x "$venv/bin/python" ]; then
  req="$(mktemp)"
  trap 'rm -f "$req"' EXIT
  (cd "$HERMES_SRC" && $uv export --frozen --no-hashes --extra messaging --extra mcp --group dev \
    --no-emit-project -o "$req" >/dev/null)
  $uv venv -q --python 3.14 "$venv"
  # The lock is fully resolved. --no-deps keeps its overrides.
  $uv pip install -q --no-deps -p "$venv/bin/python" -r "$req"
fi

cd "$here"
HERMES_SRC="$HERMES_SRC" PYTHONDONTWRITEBYTECODE=1 exec "$venv/bin/python" -m pytest -q -p no:cacheprovider "$@"
