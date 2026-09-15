#!/usr/bin/env bash
set -euo pipefail

echo "Publication disabled: the former plaintext package path is retired." >&2
echo "Use object_archive.py prepare for local encrypted validation; remote transport is not enabled." >&2
exit 1
