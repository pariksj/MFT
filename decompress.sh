#!/bin/bash
# Decompress market.duckdb.zst → market.duckdb
# Usage: ./decompress.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC="$SCRIPT_DIR/market.duckdb.zst"
DST="$SCRIPT_DIR/market.duckdb"

if [ ! -f "$SRC" ]; then
  echo "Error: $SRC not found"
  exit 1
fi

if [ -f "$DST" ]; then
  echo "market.duckdb already exists. Delete it first to re-extract."
  exit 0
fi

# Try zstd, fall back to python
if command -v zstd &>/dev/null; then
  echo "Decompressing with zstd..."
  zstd -d "$SRC" -o "$DST"
elif command -v python3 &>/dev/null; then
  echo "zstd not found, trying python3..."
  pip3 install zstandard -q
  python3 -c "
import zstandard, pathlib
src = pathlib.Path('$SRC')
dst = pathlib.Path('$DST')
dctx = zstandard.ZstdDecompressor()
with open(src, 'rb') as fin, open(dst, 'wb') as fout:
    dctx.copy_stream(fin, fout)
print(f'Decompressed to {dst} ({dst.stat().st_size / 1e6:.0f} MB)')
"
else
  echo "Error: Need zstd or python3 to decompress"
  exit 1
fi

echo "Done: $(du -h "$DST" | cut -f1) → $DST"
echo "Query with: duckdb market.duckdb"
