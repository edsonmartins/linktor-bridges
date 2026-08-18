#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
go_root="$project_root/go"
tools_dir="$project_root/tools/bin"
gomobile_bin="$tools_dir/gomobile"
android_sdk="${ANDROID_HOME:-${ANDROID_SDK_ROOT:-}}"

if [[ -z "$android_sdk" ]]; then
  echo "ANDROID_HOME or ANDROID_SDK_ROOT must point to the Android SDK" >&2
  exit 1
fi

mkdir -p "$tools_dir" "$project_root/android/app/libs"

if [[ ! -x "$gomobile_bin" ]]; then
  GOBIN="$tools_dir" go install golang.org/x/mobile/cmd/gomobile@latest
fi

export GOBIN="$tools_dir"
export PATH="$tools_dir:$PATH"
export ANDROID_HOME="$android_sdk"

cd "$go_root"
"$gomobile_bin" init
"$gomobile_bin" bind \
  -target=android \
  -androidapi 24 \
  -o "$project_root/android/app/libs/wacore.aar" \
  ./wacore

echo "AAR generated at android/app/libs/wacore.aar"
