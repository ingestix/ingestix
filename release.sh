#!/usr/bin/env bash
set -euo pipefail

# Ingestix release helper.
# Publishes crates in dependency order:
#   ingestix-derive -> ingestix
#
# Defaults to dry-run for safety. Use --execute to perform real publish.

CRATES=(
  "ingestix-derive"
  "ingestix"
)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_README="${ROOT_DIR}/README.md"

VERSION_EXPECTED="${VERSION_EXPECTED:-0.1.0-alpha}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-15}"
POLL_MAX_ATTEMPTS="${POLL_MAX_ATTEMPTS:-40}"
ALLOW_DIRTY=false
EXECUTE=false
SKIP_PREFLIGHT=false

usage() {
  cat <<'EOF'
Usage: ./release.sh [options]

Options:
  --execute            Perform real `cargo publish` (default is --dry-run).
  --allow-dirty        Pass --allow-dirty to cargo commands.
  --version <version>  Expected version to verify on crates.io (default: 0.1.0-alpha).
  --skip-preflight     Skip local preflight checks.
  -h, --help           Show this help.

Environment overrides:
  VERSION_EXPECTED, POLL_INTERVAL_SECONDS, POLL_MAX_ATTEMPTS
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --execute)
      EXECUTE=true
      shift
      ;;
    --allow-dirty)
      ALLOW_DIRTY=true
      shift
      ;;
    --version)
      VERSION_EXPECTED="${2:-}"
      if [[ -z "${VERSION_EXPECTED}" ]]; then
        echo "ERROR: --version requires a value"
        exit 1
      fi
      shift 2
      ;;
    --skip-preflight)
      SKIP_PREFLIGHT=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: Unknown option: $1"
      usage
      exit 1
      ;;
  esac
done

RUN_MODE="--dry-run"
if [[ "${EXECUTE}" == "true" ]]; then
  RUN_MODE=""
fi

DIRTY_FLAG=""
if [[ "${ALLOW_DIRTY}" == "true" ]]; then
  DIRTY_FLAG="--allow-dirty"
fi

STAGED_CRATE_READMES=()

run_cmd() {
  echo
  echo ">> $*"
  "$@"
}

get_crate_readme_path() {
  local crate="$1"
  local manifest_path

  manifest_path="$(
    cargo metadata --no-deps --format-version 1 \
      | python3 -c '
import json
import sys

crate = sys.argv[1]
data = json.load(sys.stdin)
for pkg in data.get("packages", []):
    if pkg.get("name") == crate:
        print(pkg["manifest_path"])
        break
else:
    sys.exit(1)
' "${crate}"
  )" || {
    echo "ERROR: Could not resolve manifest path for crate package '${crate}' via cargo metadata."
    return 1
  }

  dirname "${manifest_path}"
}

cleanup_staged_readmes() {
  local crate_readme
  for crate_readme in "${STAGED_CRATE_READMES[@]}"; do
    if [[ -f "${crate_readme}" ]]; then
      echo
      echo ">> rm -f ${crate_readme}"
      rm -f "${crate_readme}"
    fi
  done
}

trap cleanup_staged_readmes EXIT

stage_readme_for_crate() {
  local crate="$1"
  local crate_dir
  local crate_readme

  # Only the ingestix crate (path: ingestix-core/) needs a staged root README.
  # ingestix-derive has its own README and must keep it untouched.
  if [[ "${crate}" != "ingestix" ]]; then
    return 0
  fi

  crate_dir="$(get_crate_readme_path "${crate}")" || return 1
  crate_readme="${crate_dir}/README.md"

  if [[ ! -f "${ROOT_README}" ]]; then
    echo "ERROR: Root README not found at ${ROOT_README}"
    return 1
  fi

  run_cmd cp "${ROOT_README}" "${crate_readme}"
  STAGED_CRATE_READMES+=("${crate_readme}")
}

cleanup_readme_for_crate() {
  local crate="$1"
  local crate_dir
  local crate_readme

  # Keep cleanup symmetric with stage_readme_for_crate.
  if [[ "${crate}" != "ingestix" ]]; then
    return 0
  fi

  crate_dir="$(get_crate_readme_path "${crate}")" || return 1
  crate_readme="${crate_dir}/README.md"

  if [[ -f "${crate_readme}" ]]; then
    run_cmd rm -f "${crate_readme}"
  fi
}

wait_until_indexed() {
  local crate="$1"
  local expected="$2"

  echo
  echo "Waiting for crates.io index: ${crate} ${expected}"

  for ((i=1; i<=POLL_MAX_ATTEMPTS; i++)); do
    local line
    line="$(cargo search "${crate}" --limit 1 2>/dev/null || true)"

    if [[ "${line}" == ${crate}" = \""${expected}"\""* ]]; then
      echo "Indexed: ${line}"
      return 0
    fi

    echo "Attempt ${i}/${POLL_MAX_ATTEMPTS}: not indexed yet."
    sleep "${POLL_INTERVAL_SECONDS}"
  done

  echo "ERROR: ${crate} ${expected} did not appear in index in time."
  return 1
}

if [[ "${SKIP_PREFLIGHT}" == "false" ]]; then
  echo "Running preflight checks..."
  run_cmd cargo test --doc -p ingestix-derive -p ingestix
  run_cmd cargo test -p ingestix-derive -p ingestix
fi

if [[ "${EXECUTE}" == "false" ]]; then
  echo
  echo "Dry-run mode. No publish will occur."
else
  echo
  echo "REAL publish mode enabled."
  echo "Crates will be published in order: ${CRATES[*]}"
fi

for crate in "${CRATES[@]}"; do
  stage_readme_for_crate "${crate}"
  run_cmd cargo publish -p "${crate}" ${RUN_MODE} ${DIRTY_FLAG}
  cleanup_readme_for_crate "${crate}"

  # In dry-run mode, do not wait for index propagation.
  if [[ "${EXECUTE}" == "true" ]]; then
    wait_until_indexed "${crate}" "${VERSION_EXPECTED}"
    run_cmd cargo info "${crate}"
  fi
done

echo
echo "Release flow completed successfully."
