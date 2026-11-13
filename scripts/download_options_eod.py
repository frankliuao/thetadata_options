#!/usr/bin/env python3
"""Standalone runner script to download 2016–present Options EOD + Greeks into Parquet."""

import sys
from options_data.cli import main

if __name__ == "__main__":
    sys.exit(main())
