#!/usr/bin/env python3
"""Check C syntax using GCC preprocessor"""

import subprocess
import os

src_dir = "/vercel/share/v0-project/src"

# First list the src directory to verify files exist
print("Files in src directory:")
try:
    for f in sorted(os.listdir(src_dir)):
        if f.startswith("openslide-http") or f.startswith("openslide-file"):
            print(f"  - {f}")
except Exception as e:
    print(f"Error listing directory: {e}")

# Check if gcc is available
try:
    result = subprocess.run(["gcc", "--version"], capture_output=True, text=True)
    print(f"\nGCC available: {result.stdout.split(chr(10))[0]}")
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
    
    # Debug: print full path
    print(f"\nChecking: {fpath}")
    print(f"Exists: {os.path.exists(fpath)}")
    
    if not os.path.exists(fpath):
        print(f"[SKIP] {fname}: file not found")
        continue
    
    # Get pkg-config flags
    pkg_cflags = []
    try:
        pkg_flags = subprocess.run(
            ["pkg-config", "--cflags", "glib-2.0", "libcurl"],
            capture_output=True, text=True
        )
        if pkg_flags.returncode == 0:
            pkg_cflags = pkg_flags.stdout.strip().split()
            print(f"pkg-config flags: {' '.join(pkg_cflags[:4])}...")
    except Exception as e:
        print(f"pkg-config error: {e}")
    
    # Use gcc with pkg-config flags for dependencies
    cmd = [
        "gcc", "-fsyntax-only", "-Wall", "-Wextra",
        "-I", src_dir,
    ] + pkg_cflags + [fpath]
    
    print(f"Command: {' '.join(cmd[:8])}...")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"[OK] {fname}: syntax OK")
        if result.stderr:
            # Only show first few lines of warnings
            warnings = result.stderr.strip().split('\n')[:5]
            print(f"     Warnings ({len(result.stderr.strip().split(chr(10)))} total):")
            for w in warnings:
                print(f"       {w}")
    else:
        print(f"[ERROR] {fname}:")
        # Show first 20 lines of errors
        errors = result.stderr.strip().split('\n')[:20]
        for e in errors:
            print(f"  {e}")

print("\n" + "="*60)
print("Note: Full compilation requires meson + all dependencies")
print("Test on a proper Linux dev environment for complete validation")
print("="*60)
