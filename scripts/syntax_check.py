#!/usr/bin/env python3
"""Check C syntax using GCC preprocessor"""

import subprocess
import os

src_dir = "/vercel/share/v0-project/src"

# Check if gcc is available
try:
    result = subprocess.run(["gcc", "--version"], capture_output=True, text=True)
    print(f"GCC available: {result.stdout.split(chr(10))[0]}")
except FileNotFoundError:
    print("GCC not available")
    exit(1)

# Try to syntax-check our new files
files_to_check = ["openslide-http.c", "openslide-http.h", "openslide-file.c"]

print("\n" + "="*60)
print("Syntax Check (using gcc -fsyntax-only)")
print("="*60)

for fname in files_to_check:
    fpath = os.path.join(src_dir, fname)
    if not os.path.exists(fpath):
        print(f"[SKIP] {fname}: file not found")
        continue
    
    # Use gcc with pkg-config flags for dependencies
    cmd = [
        "gcc", "-fsyntax-only", "-Wall", "-Wextra",
        "-I", src_dir,
        fpath
    ]
    
    # Get pkg-config flags
    try:
        pkg_flags = subprocess.run(
            ["pkg-config", "--cflags", "glib-2.0", "libcurl"],
            capture_output=True, text=True
        )
        if pkg_flags.returncode == 0:
            cmd[2:2] = pkg_flags.stdout.strip().split()
    except:
        pass
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"[OK] {fname}: syntax OK")
        if result.stderr:
            print(f"     Warnings:\n{result.stderr}")
    else:
        print(f"[ERROR] {fname}:")
        print(result.stderr)

print("\n" + "="*60)
print("Note: Full compilation requires meson + all dependencies")
print("Test on a proper Linux dev environment for complete validation")
print("="*60)
