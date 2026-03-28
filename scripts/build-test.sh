#!/bin/bash
set -e

echo "=== Checking build tools ==="
which meson && meson --version || echo "meson not found"
which ninja && ninja --version || echo "ninja not found"
which gcc && gcc --version | head -1 || echo "gcc not found"
which pkg-config && pkg-config --version || echo "pkg-config not found"

echo ""
echo "=== Checking dependencies ==="
pkg-config --exists glib-2.0 && echo "glib-2.0: $(pkg-config --modversion glib-2.0)" || echo "glib-2.0 not found"
pkg-config --exists cairo && echo "cairo: $(pkg-config --modversion cairo)" || echo "cairo not found"
pkg-config --exists libcurl && echo "libcurl: $(pkg-config --modversion libcurl)" || echo "libcurl not found"
pkg-config --exists libtiff-4 && echo "libtiff: $(pkg-config --modversion libtiff-4)" || echo "libtiff not found"
pkg-config --exists libopenjp2 && echo "openjp2: $(pkg-config --modversion libopenjp2)" || echo "openjp2 not found"

echo ""
echo "=== Attempting meson setup ==="
cd /vercel/share/v0-project
rm -rf builddir
meson setup builddir 2>&1 || echo "Meson setup failed"

if [ -d builddir ]; then
  echo ""
  echo "=== Attempting ninja build ==="
  ninja -C builddir 2>&1 | head -100 || echo "Build failed"
fi
