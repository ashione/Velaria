#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF' >&2
usage: scripts/prepare_velaria_release.sh <version> [tag]

Prepare a release commit and local tag for Velaria's Python package.

Examples:
  scripts/prepare_velaria_release.sh 0.1.3
  scripts/prepare_velaria_release.sh 0.1.3 release-v0.1.3
EOF
  exit 1
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
fi

VERSION="$1"
TAG="${2:-v${VERSION}}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION_BZL="${ROOT_DIR}/python/version.bzl"
VERSION_PY="${ROOT_DIR}/python/velaria/_version.py"
UV_LOCK="${ROOT_DIR}/python/uv.lock"

case "${TAG}" in
  "v${VERSION}"|"release-v${VERSION}")
    ;;
  *)
    echo "tag ${TAG} does not match version ${VERSION}" >&2
    exit 1
    ;;
esac

cd "${ROOT_DIR}"

if [[ -n "$(git status --porcelain --untracked-files=all)" ]]; then
  echo "working tree is not clean; commit or stash changes before preparing a release" >&2
  exit 1
fi

BRANCH="$(git symbolic-ref --quiet --short HEAD || true)"
if [[ -z "${BRANCH}" ]]; then
  echo "detached HEAD is not supported; switch to a branch before preparing a release" >&2
  exit 1
fi

CURRENT_VERSION="$(./scripts/get_velaria_version.sh)"
if [[ "${CURRENT_VERSION}" == "${VERSION}" ]]; then
  echo "package version is already ${VERSION}" >&2
  exit 1
fi

if git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null; then
  echo "local tag already exists: ${TAG}" >&2
  exit 1
fi

if git ls-remote --exit-code --tags origin "refs/tags/${TAG}" >/dev/null 2>&1; then
  echo "remote tag already exists on origin: ${TAG}" >&2
  exit 1
fi

"${ROOT_DIR}/scripts/bump_velaria_version.sh" "${VERSION}"

git add "${VERSION_BZL}" "${VERSION_PY}"
if [[ -f "${UV_LOCK}" ]]; then
  git add "${UV_LOCK}"
fi

if git diff --cached --quiet; then
  echo "no staged changes after bumping version; aborting" >&2
  exit 1
fi

git commit -m "release: bump Velaria Python version to ${VERSION}"
git tag -a "${TAG}" -m "Release ${TAG}"

cat <<EOF
Prepared release commit and local tag.

  branch: ${BRANCH}
  version: ${VERSION}
  tag: ${TAG}

Next steps:
  git push origin ${BRANCH}
  git push origin ${TAG}
EOF
