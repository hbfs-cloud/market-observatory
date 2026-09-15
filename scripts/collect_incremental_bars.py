#!/usr/bin/env python3
"""Compatibility entry point; requires the new explicit manual CLI arguments."""
from incremental_collector import main


if __name__ == "__main__":
    raise SystemExit(main())
