#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
common=${1:-"$root/../nakama-common"}
version=${VERSION:-3.41.0-cardgame.2}
namespace=${IMAGE_NAMESPACE:-sadeghdev}
commit=$(git -C "$root" rev-parse --short HEAD)
set -- --load
if [ "${PUSH:-false}" = true ]; then
  : "${IMAGE_NAMESPACE:?Set IMAGE_NAMESPACE to the Docker Hub namespace before publishing}"
  set -- --push --platform "${PLATFORMS:-linux/amd64,linux/arm64}"
fi
if [ -n "${BUILDER:-}" ]; then
  set -- "$@" --builder "$BUILDER"
fi
for target in runtime pluginbuilder; do
  case "$target" in
    runtime) image="$namespace/nakama" ;;
    pluginbuilder) image="$namespace/nakama-pluginbuilder" ;;
  esac
  docker buildx build "$@" --target "$target" \
    --build-context "nakama-common=$common" \
    --build-arg "VERSION=$version" --build-arg "COMMIT=$commit" \
    -f "$root/build/Dockerfile.cardgame" -t "$image:$version" "$root"
done
