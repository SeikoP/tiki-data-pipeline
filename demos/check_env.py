#!/usr/bin/env python3
import sys

print("Python version:", sys.version)
print("Executable:", sys.executable)

# Quick package check
packages = ["aiohttp", "requests", "bs4", "selenium"]
for pkg in packages:
    try:
        __import__(pkg)
        print(f"✓ {pkg}")
    except ImportError:
        print(f"✗ {pkg} - NOT INSTALLED")
