#!/usr/bin/env python3
"""Check build environment and attempt to compile OpenSlide"""

import subprocess
import shutil
import sys
import os

def run_cmd(cmd, check=False):
    """Run a command and return output"""
    print(f"\n>>> {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        return result.returncode == 0
    except Exception as e:
        print(f"Error: {e}")
        return False

def check_tool(name):
    """Check if a tool is available"""
    path = shutil.which(name)
    if path:
        print(f"[OK] {name}: {path}")
        return True
    else:
        print(f"[MISSING] {name}")
        return False

def main():
    print("=" * 60)
    print("OpenSlide Build Environment Check")
    print("=" * 60)
    
    # Check basic tools
    print("\n--- Build Tools ---")
    tools = ['meson', 'ninja', 'gcc', 'pkg-config']
    all_tools = all(check_tool(t) for t in tools)
    
    # Check libraries via pkg-config
    print("\n--- Libraries (pkg-config) ---")
    libs = ['glib-2.0', 'cairo', 'libcurl', 'libtiff-4', 'libopenjp2', 
            'libjpeg', 'libpng', 'sqlite3', 'libxml-2.0', 'zlib']
    
    for lib in libs:
        result = subprocess.run(['pkg-config', '--exists', lib], capture_output=True)
        if result.returncode == 0:
            ver = subprocess.run(['pkg-config', '--modversion', lib], 
                               capture_output=True, text=True)
            print(f"[OK] {lib}: {ver.stdout.strip()}")
        else:
            print(f"[MISSING] {lib}")
    
    # Check libdicom (may need special handling)
    print("\n--- Special Dependencies ---")
    result = subprocess.run(['pkg-config', '--exists', 'libdicom'], capture_output=True)
    if result.returncode == 0:
        print("[OK] libdicom")
    else:
        print("[MISSING] libdicom (may need to be built)")
    
    # Try meson setup
    print("\n--- Attempting Meson Setup ---")
    os.chdir('/vercel/share/v0-project')
    
    # Clean previous build
    if os.path.exists('builddir'):
        import shutil
        shutil.rmtree('builddir')
    
    success = run_cmd(['meson', 'setup', 'builddir', '--buildtype=debug'])
    
    if success:
        print("\n--- Attempting Ninja Build ---")
        run_cmd(['ninja', '-C', 'builddir', '-j2'])
    else:
        print("\nMeson setup failed - checking what's missing...")
        
    print("\n" + "=" * 60)
    print("Build check complete")
    print("=" * 60)

if __name__ == '__main__':
    main()
